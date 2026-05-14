"""Core HTTP API for Thread Observability add-on.

Serves a lightweight status dashboard at ``/`` (Ingress entry-point) plus
JSON endpoints under ``/v1/...`` for programmatic access.
"""

import asyncio
import contextlib
import json
import logging
import re
import shutil
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse

from . import supervisor_client
from .mcp_tools import _build_partition_state, _build_phantom_list
from ..config import ThreadObsConfig, get_config
from ..health import build_health_snapshot
from ..pipeline import analyze_node as analyze_node_mod
from ..pipeline import device_discovery
from ..pipeline import nodes as nodes_mod
from ..pipeline import otbr_adapter
from ..pipeline import otbr_rest
from ..pipeline import reasoner as reasoner_mod
from ..pipeline import routing as routing_mod
from ..pipeline import runner as pipeline_runner
from ..pipeline import topology as topology_mod
from ..pipeline import topology_snapshot as topology_snapshot_mod
from ..utils.datetime import utc_now_iso
from ..services import chat_memory
from ..services import direct_chat
from ..storage import influx_store as ts_store
from ..storage.sqlite_store import get_store

log = logging.getLogger(__name__)

_CONFIG_SECRET_KEYS = frozenset({"token", "api_key", "ha_admin_token"})


def _redact_config_secrets(value: object) -> object:
    if isinstance(value, dict):
        redacted: dict[str, object] = {}
        for key, item in value.items():
            if key in _CONFIG_SECRET_KEYS and item:
                redacted[key] = "***"
            else:
                redacted[key] = _redact_config_secrets(item)
        return redacted
    if isinstance(value, list):
        return [_redact_config_secrets(item) for item in value]
    return value


def _build_storage_capacity(storage: dict[str, object]) -> dict[str, object]:
    size_bytes = int(storage.get("size_bytes") or 0)
    db_path = Path(str(storage.get("db_path") or "")).expanduser()
    free_bytes: int | None = None
    total_bytes: int | None = None
    warning_free_bytes = max(size_bytes * 10, 1024 * 1024 * 1024)
    critical_free_bytes = max(size_bytes * 3, 256 * 1024 * 1024)
    growth_rate_bytes_per_day: float | None = None
    risk = "unknown"
    note = "disk capacity could not be determined"
    try:
        recent_ticks = get_store().get_recent_pipeline_ticks(limit=48)
    except Exception:
        recent_ticks = []
    sized_ticks = [
        row for row in reversed(recent_ticks)
        if row.get("db_size_bytes") is not None and row.get("completed_at")
    ]
    if len(sized_ticks) >= 2:
        first = sized_ticks[0]
        last = sized_ticks[-1]
        try:
            started = datetime.fromisoformat(str(first["completed_at"]))
            ended = datetime.fromisoformat(str(last["completed_at"]))
            seconds = max((ended - started).total_seconds(), 1.0)
            growth_rate_bytes_per_day = ((float(last["db_size_bytes"]) - float(first["db_size_bytes"])) / seconds) * 86400.0
        except Exception:
            growth_rate_bytes_per_day = None
    if db_path:
        try:
            usage = shutil.disk_usage(db_path.parent)
            free_bytes = int(usage.free)
            total_bytes = int(usage.total)
        except OSError:
            free_bytes = None
            total_bytes = None
    if free_bytes is not None and total_bytes:
        db_fraction = (size_bytes / total_bytes) if total_bytes > 0 else 0.0
        if free_bytes < critical_free_bytes:
            risk = "high"
            note = "free space is low relative to the current SQLite size"
        elif free_bytes < warning_free_bytes:
            risk = "medium"
            note = "SQLite is healthy now, but capacity headroom is getting tighter"
        else:
            risk = "low"
            note = "SQLite has comfortable free-space headroom"
        return {
            "size_bytes": size_bytes,
            "free_bytes": free_bytes,
            "total_bytes": total_bytes,
            "db_fraction": round(db_fraction, 6),
            "warning_free_bytes": warning_free_bytes,
            "critical_free_bytes": critical_free_bytes,
            "growth_rate_bytes_per_day": growth_rate_bytes_per_day,
            "risk": risk,
            "note": note,
        }
    return {
        "size_bytes": size_bytes,
        "free_bytes": free_bytes,
        "total_bytes": total_bytes,
        "db_fraction": None,
        "warning_free_bytes": warning_free_bytes,
        "critical_free_bytes": critical_free_bytes,
        "growth_rate_bytes_per_day": growth_rate_bytes_per_day,
        "risk": risk,
        "note": note,
    }


def _get_runtime_chat_config() -> ThreadObsConfig:
    cfg = get_config()
    options_path = Path(str(getattr(cfg, "options_path", "") or "")).expanduser()
    if getattr(cfg, "options_loaded", False) or options_path.exists():
        try:
            return ThreadObsConfig.load(options_path)
        except Exception:  # noqa: BLE001
            log.exception("failed to reload chat config from %s; using cached config", options_path)
    return cfg


def _build_diagnostics_summary(
    *,
    supervisor: dict[str, object],
    storage: dict[str, object],
    timeseries: dict[str, object],
    ingestion: dict[str, object],
    pipeline: dict[str, object],
    health: dict[str, object],
    partitions: dict[str, object],
    phantoms: dict[str, object],
    stale_link_count: int,
    config: dict[str, object],
    graph_diagnostics: list[dict[str, object]],
) -> dict[str, object]:
    health_summary = health.get("summary") if isinstance(health, dict) else {}
    issue_summary = health.get("active_issues") if isinstance(health, dict) else {}
    stages_failed = list(pipeline.get("stages_failed") or []) if isinstance(pipeline, dict) else []
    assessment_cfg = config.get("assessment") if isinstance(config, dict) else {}
    storage_capacity = _build_storage_capacity(storage)
    ingestion_error = str(ingestion.get("error") or "").strip() if isinstance(ingestion, dict) else ""
    ingestion_slug = str(ingestion.get("slug") or "").strip() if isinstance(ingestion, dict) else ""
    sources = {
        "supervisor": {
            "status": "error" if supervisor.get("error") else "ok",
            "detail": str(supervisor.get("error") or "reachable via Supervisor API"),
        },
        "pipeline": {
            "status": "error" if stages_failed else ("running" if pipeline.get("running") else "ok"),
            "detail": (
                f"failed stages: {', '.join(stages_failed)}"
                if stages_failed else (
                    f"tick #{pipeline.get('tick_count') or 0} in progress"
                    if pipeline.get("running") else f"last tick #{pipeline.get('tick_count') or 0} completed"
                )
            ),
            "failed_stages": stages_failed,
            "last_finished_at": pipeline.get("finished_at"),
        },
        "otbr_ingestion": {
            "status": "error" if ingestion_error else ("warn" if not ingestion_slug else "ok"),
            "detail": ingestion_error or (
                "no OTBR add-on selected for log ingestion" if not ingestion_slug else "OTBR ingest state available"
            ),
            "last_run_at": ingestion.get("last_run_at") if isinstance(ingestion, dict) else None,
        },
        "timeseries": {
            "status": "ok" if timeseries.get("ok") else "warn",
            "detail": str(timeseries.get("error") or timeseries.get("backend") or "unknown backend"),
            "backend": timeseries.get("backend"),
        },
        "assessment": {
            "status": "ok" if assessment_cfg.get("enabled") else "warn",
            "detail": "Adaptive Monitoring enabled" if assessment_cfg.get("enabled") else "Adaptive Monitoring disabled",
        },
    }
    data_quality = {
        "status": health.get("status") if isinstance(health, dict) else "unknown",
        "data_age_seconds": health.get("data_age_seconds") if isinstance(health, dict) else None,
        "stale_nodes": int((health_summary or {}).get("stale_nodes") or 0),
        "offline_nodes": int((health_summary or {}).get("offline_nodes") or 0),
        "duplicate_physical_device_groups": int((health_summary or {}).get("duplicate_physical_device_groups") or 0),
        "distinct_thread_networks": int((health_summary or {}).get("distinct_thread_networks") or 0),
        "active_issue_count": int((issue_summary or {}).get("count") or 0),
        "partition_count": int(partitions.get("partition_count") or 0) if isinstance(partitions, dict) else 0,
        "phantom_count": int(phantoms.get("count") or 0) if isinstance(phantoms, dict) else 0,
        "stale_link_count": int(stale_link_count or 0),
    }
    attention_items: list[dict[str, str]] = []
    if stages_failed:
        attention_items.append({
            "severity": "bad",
            "title": "Pipeline stages are failing",
            "detail": f"Failed stages: {', '.join(stages_failed)}",
        })
    if data_quality["distinct_thread_networks"] > 1:
        attention_items.append({
            "severity": "warn",
            "title": "Multiple Thread networks detected",
            "detail": f"{data_quality['distinct_thread_networks']} distinct Thread networks are present in current node data.",
        })
    if data_quality["duplicate_physical_device_groups"] > 0:
        attention_items.append({
            "severity": "warn",
            "title": "Duplicate hardware identities need cleanup",
            "detail": f"{data_quality['duplicate_physical_device_groups']} duplicate device groups remain in the mesh inventory.",
        })
    if data_quality["offline_nodes"] > 0 or data_quality["stale_nodes"] > 0:
        attention_items.append({
            "severity": "warn",
            "title": "Node freshness is degraded",
            "detail": f"{data_quality['offline_nodes']} offline and {data_quality['stale_nodes']} stale nodes are currently reported.",
        })
    if storage_capacity["risk"] in {"medium", "high"}:
        attention_items.append({
            "severity": "warn" if storage_capacity["risk"] == "medium" else "bad",
            "title": "SQLite capacity headroom is tightening",
            "detail": str(storage_capacity["note"]),
        })
    for fact in graph_diagnostics[:2]:
        attention_items.append({
            "severity": str(fact.get("severity") or "warn"),
            "title": str(fact.get("title") or "Graph-derived concern"),
            "detail": str(fact.get("detail") or ""),
        })
    if not attention_items:
        attention_items.append({
            "severity": "good",
            "title": "No urgent observability blockers detected",
            "detail": "Current sources and retained mesh data look healthy enough for normal troubleshooting.",
        })
    return {
        "sources": sources,
        "data_quality": data_quality,
        "storage_capacity": storage_capacity,
        "attention_items": attention_items,
        "graph_diagnostics": graph_diagnostics,
    }


def _render_chat_message(
    message: str,
    page_context: dict[str, object] | None,
    session_context: dict[str, object] | None = None,
) -> str:
    text = message.strip()
    sections: list[str] = []
    if session_context:
        sections.append(
            "Session memory: "
            + json.dumps(session_context, separators=(",", ":"), ensure_ascii=True)
        )
    if page_context:
        sections.append(
            "Page context: "
            + json.dumps(page_context, separators=(",", ":"), ensure_ascii=True)
        )
    sections.append(f"User message: {text}")
    if sections:
        return "\n\n".join(sections)
    if not page_context:
        return text
    return text


def _augment_chat_page_context(page_context: dict[str, object] | None) -> dict[str, object] | None:
    if not page_context:
        return page_context
    enriched = dict(page_context)
    try:
        include_phantoms = bool(enriched.get("include_phantoms"))
        topo = topology_mod.build_topology(include_phantoms=include_phantoms)
        enriched["graph_diagnostics"] = topology_mod.derive_graph_diagnostics(topo)
        enriched.setdefault(
            "topology_summary",
            {
                "node_count": topo.get("node_count"),
                "link_count": topo.get("link_count"),
                "partition_count": len(topo.get("partitions") or []),
                "split": bool(topo.get("split")),
            },
        )
    except Exception:  # noqa: BLE001
        log.exception("chat context: failed to derive graph diagnostics")
    return enriched


def _record_chat_turn_telemetry(
    *,
    conversation_id: str | None,
    backend: str,
    agent_id: str | None,
    model_name: str | None,
    status: str,
    error_kind: str | None,
    duration_ms: int,
    tool_call_count: int,
    page_context: dict[str, object] | None,
) -> None:
    try:
        get_store().record_chat_turn_stat(
            conversation_id=conversation_id,
            recorded_at=_utc_now(),
            backend=backend,
            agent_id=agent_id,
            model_name=model_name,
            status=status,
            error_kind=error_kind,
            duration_ms=duration_ms,
            tool_call_count=tool_call_count,
            had_page_context=bool(page_context),
            selected_node_eui64=str((page_context or {}).get("selected_node_eui64") or "").strip() or None,
            active_tab=str((page_context or {}).get("active_tab") or "").strip() or None,
        )
    except Exception:  # noqa: BLE001
        log.exception("chat telemetry: failed to record turn stat")


def _looks_like_builtin_chat_fallback(text: str) -> bool:
    normalized = " ".join(text.strip().lower().split())
    if not normalized:
        return False
    fallback_prefixes = (
        "sorry, i couldn't understand that",
        "sorry, i could not understand that",
        "sorry, i didn't understand that",
        "sorry, i did not understand that",
        "i'm sorry, but i couldn't understand that",
        "i am sorry, but i couldn't understand that",
    )
    return any(normalized.startswith(prefix) for prefix in fallback_prefixes)


def _rewrite_builtin_chat_fallback(
    text: str,
    *,
    model: object,
    agent_id: object,
    requested_agent_id: str | None,
) -> str:
    plain_text = str(text or "").strip()
    if not _looks_like_builtin_chat_fallback(plain_text):
        return plain_text
    selected_agent = str(agent_id or requested_agent_id or "Home Assistant default").strip()
    if model:
        return plain_text
    return (
        "Home Assistant handled this with its default conversation agent, not an LLM-backed Assist "
        f"agent, so it returned the generic fallback: \"{plain_text}\". Configure or select an "
        f"LLM-capable conversation agent in Home Assistant Assist, then retry. Current agent: {selected_agent}."
    )


def _extract_chat_turn(
    payload: dict[str, object],
    *,
    requested_agent_id: str | None,
    duration_ms: int,
) -> dict[str, object]:
    response_block = payload.get("response")
    if isinstance(response_block, list) and response_block:
        response_block = response_block[0]
    response_dict = response_block if isinstance(response_block, dict) else {}
    speech = response_dict.get("speech") if isinstance(response_dict, dict) else {}
    speech = speech if isinstance(speech, dict) else {}
    plain = speech.get("plain") if isinstance(speech, dict) else {}
    plain = plain if isinstance(plain, dict) else {}
    data = response_dict.get("data") if isinstance(response_dict, dict) else {}
    data = data if isinstance(data, dict) else {}
    intent_extras = data.get("intent_extras")
    tool_calls = data.get("tool_calls") or payload.get("tool_calls") or []
    if not isinstance(tool_calls, list):
        tool_calls = [tool_calls] if tool_calls else []
    card = data.get("card") if isinstance(data.get("card"), dict) else None
    if card is None and isinstance(intent_extras, dict):
        maybe_card = intent_extras.get("card")
        if isinstance(maybe_card, dict):
            card = maybe_card
    model = data.get("model") or response_dict.get("model") or payload.get("model")
    agent_id = payload.get("agent_id") or requested_agent_id
    response_text = _rewrite_builtin_chat_fallback(
        str(plain.get("speech") or data.get("text") or ""),
        model=model,
        agent_id=agent_id,
        requested_agent_id=requested_agent_id,
    )
    return {
        "conversation_id": payload.get("conversation_id"),
        "agent_id": agent_id,
        "response": {
            "text": response_text,
            "card": card,
        },
        "tool_calls": tool_calls,
        "duration_ms": duration_ms,
        "model": model,
        "streaming": False,
    }


def _read_addon_version() -> str:
    """Read version from config.yaml so it never drifts from the manifest."""
    here = Path(__file__).resolve()
    candidates = [
        Path("/opt/thread-observability/config.yaml"),  # baked into image
        Path("/config.yaml"),                       # mounted into container
        Path("/app/config.yaml"),                   # alt container layout
        *(p / "config.yaml" for p in here.parents), # walk up (covers dev tree)
    ]
    for p in candidates:
        try:
            if p.exists():
                m = re.search(r"^version:\s*([^\s#]+)", p.read_text(), re.MULTILINE)
                if m:
                    return m.group(1).strip().strip('"').strip("'")
        except OSError:
            continue
    return "unknown"


ADDON_VERSION = _read_addon_version()
LOG_PATH = Path("/data/thread-observability/addon.log")
_CHAT_STARTER_PROMPTS_PATH = Path(__file__).parent / "chat_starter_prompts.json"
_CHAT_KNOWN_THREAD_TOOLS = frozenset({"get_health_snapshot", "get_mesh_state", "list_active_issues", "start_triage"})
_HA_MCP_CLIENT_URL = "http://9e5048e8-thread-observability:8100/mcp/sse"
_HA_INTEGRATIONS_URL = "/config/integrations/dashboard"


def _utc_now() -> str:
    return utc_now_iso()


def _tail_log(n: int = 80) -> list[str]:
    if not LOG_PATH.exists():
        return []
    try:
        return LOG_PATH.read_text(errors="replace").splitlines()[-n:]
    except OSError:
        return []


DASHBOARD_HTML = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")


def _load_chat_starter_prompts() -> list[str]:
    try:
        payload = json.loads(_CHAT_STARTER_PROMPTS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    prompts: list[str] = []
    for item in payload:
        if isinstance(item, str) and item.strip():
            prompts.append(item.strip())
    return prompts


def _agent_has_thread_tools(row: dict[str, object]) -> bool:
    tool_names = row.get("tool_names") if isinstance(row.get("tool_names"), list) else []
    if not tool_names:
        return bool(row.get("has_thread_tools"))
    normalized = {str(name).strip() for name in tool_names if str(name).strip()}
    return any(name in _CHAT_KNOWN_THREAD_TOOLS for name in normalized)


def _window_deltas(samples: list[dict[str, object]]) -> dict[str, object]:
    """Compute (last - first) per numeric counter across a sample window.

    Returns ``{counter_name: delta_int_or_None}``. Reset (negative diff)
    yields ``None`` so the UI can render it explicitly instead of as a drop.
    """
    if not samples or len(samples) < 2:
        return {}
    first = samples[0].get("counters") if isinstance(samples[0], dict) else None
    last = samples[-1].get("counters") if isinstance(samples[-1], dict) else None
    if not isinstance(first, dict) or not isinstance(last, dict):
        return {}
    out: dict[str, object] = {}
    for k in set(first) | set(last):
        a = first.get(k)
        b = last.get(k)
        if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
            continue
        diff = b - a
        out[k] = None if diff < 0 else (int(diff) if diff == int(diff) else round(diff, 3))
    return out


async def _periodic(name: str, interval: int, coro_factory) -> None:
    """Deprecated. Kept only because tests import it; the live scheduler
    now uses :mod:`thread_observability.pipeline.runner` instead. Runs the
    factory once immediately, then on ``interval`` cadence.
    """
    while True:
        try:
            await coro_factory()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("periodic task %s failed", name)
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI):
    """Start/stop the background pipeline alongside the FastAPI app.

    The pipeline is a single atomic tick (OTBR log ingest → OTBR REST →
    Matter discovery → reasoner) that runs immediately on startup and then
    every ``pipeline_interval_seconds`` after the previous tick finishes.
    There are no other background loops — everything the UI shows comes
    from this one source.
    """
    cfg = get_config()
    pipeline_interval = int(getattr(cfg.scheduler, "pipeline_interval_seconds", 30))

    # The SQLite store is a live cache of what the Thread fabric currently
    # reports. Anything that survives across a restart but does not come back
    # in the next poll cycle is, by definition, stale. Wiping on boot makes
    # the DB authoritative-by-construction.
    if getattr(cfg, "reset_db_on_start", True):
        try:
            deleted = get_store().reset_data()
            log.info("reset_db_on_start: wiped %d rows from cache tables", deleted)
        except Exception:  # noqa: BLE001
            log.exception("reset_db_on_start: failed to truncate cache tables")
    else:
        log.info("reset_db_on_start=false: preserving previous DB contents")

    # v0.9.44 — record our own cold start as an ``addon:self`` observer
    # event. The reasoner uses this to suppress / downgrade ``offline_node``
    # and similar issues that fire in the seconds right after boot, where
    # the cache has no last_seen for anyone yet. Best-effort: a failed
    # write must not block startup.
    try:
        from ..pipeline.observer_events import record_self_start  # local import
        record_self_start(get_store(), version=ADDON_VERSION)
    except Exception:  # noqa: BLE001
        log.exception("observer_events: failed to record self-start")

    tasks = [
        asyncio.create_task(
            pipeline_runner.run_forever(interval_seconds=pipeline_interval),
            name="pipeline-runner",
        ),
    ]
    log.info("pipeline scheduler started: interval=%ss (single atomic tick)", pipeline_interval)
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        log.info("pipeline scheduler stopped")


def create_core_app() -> FastAPI:
    """Create the core FastAPI application."""
    app = FastAPI(
        title="Thread Observability Core API",
        version=ADDON_VERSION,
        lifespan=_lifespan,
    )

    @app.get("/", response_class=HTMLResponse)
    def dashboard() -> HTMLResponse:
        return HTMLResponse(DASHBOARD_HTML)

    @app.get("/api")
    def api_root() -> dict[str, str]:
        return {"service": "core", "name": "thread-observability", "version": ADDON_VERSION}

    @app.get("/v1/chat/agents")
    async def chat_agents() -> dict[str, object]:
        cfg = _get_runtime_chat_config()
        starter_prompts = _load_chat_starter_prompts()
        if not cfg.chat.enabled:
            return {
                "enabled": False,
                "agents": [],
                "count": 0,
                "source": None,
                "default_backend": direct_chat.default_chat_backend(cfg.ai, None),
                "default_label": "Chat disabled",
                "default_agent_id": str(cfg.chat.default_agent_id or "").strip() or None,
                "send_page_context": bool(cfg.chat.send_page_context),
                "persist_transcripts": bool(cfg.chat.persist_transcripts),
                "chat_retention_days": int(cfg.retention.chat_days),
                "thread_tools_connected": False,
                "mcp_connect_url": _HA_MCP_CLIENT_URL,
                "ha_integrations_url": _HA_INTEGRATIONS_URL,
                "starter_prompts": starter_prompts,
            }
        direct_target = direct_chat.resolve_direct_chat_target(cfg.ai)
        agents: list[dict[str, object]] = []
        source_parts: list[str] = []
        try:
            payload = await supervisor_client.list_conversation_agents()
            agents.extend(payload.get("agents") or [])
            source = payload.get("source")
            if source:
                source_parts.append(str(source))
        except Exception as exc:  # noqa: BLE001
            if direct_target is None:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Failed to list conversation agents: {exc}",
                ) from exc
        if direct_target is not None:
            agents.insert(0, direct_chat.direct_agent_row(direct_target))
            source_parts.append("direct")
        thread_tools_connected = any(_agent_has_thread_tools(agent) for agent in agents)
        return {
            "enabled": True,
            "agents": agents,
            "count": len(agents),
            "source": "+".join(source_parts) if source_parts else None,
            "default_backend": direct_chat.default_chat_backend(cfg.ai, direct_target),
            "default_label": direct_chat.default_chat_label(cfg.ai, direct_target),
            "default_agent_id": str(cfg.chat.default_agent_id or "").strip() or None,
            "send_page_context": bool(cfg.chat.send_page_context),
            "persist_transcripts": bool(cfg.chat.persist_transcripts),
            "chat_retention_days": int(cfg.retention.chat_days),
            "thread_tools_connected": thread_tools_connected,
            "mcp_connect_url": _HA_MCP_CLIENT_URL,
            "ha_integrations_url": _HA_INTEGRATIONS_URL,
            "starter_prompts": starter_prompts,
        }

    @app.post("/v1/chat/turn")
    async def chat_turn(payload: dict[str, object]) -> dict[str, object]:
        cfg = _get_runtime_chat_config()
        if not cfg.chat.enabled:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chat is disabled in add-on options.",
            )
        message = str((payload or {}).get("message") or "").strip()
        if not message:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="message required",
            )
        if bool((payload or {}).get("streaming")):
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="streaming not implemented yet; retry with streaming=false",
            )

        conversation_id = (payload or {}).get("conversation_id")
        conversation_id = str(conversation_id).strip() if conversation_id else None
        agent_id = (payload or {}).get("agent_id")
        agent_id = str(agent_id).strip() if agent_id else None
        if not agent_id:
            agent_id = str(cfg.chat.default_agent_id or "").strip() or None
        page_context = (payload or {}).get("page_context")
        page_context = page_context if isinstance(page_context, dict) else None
        if cfg.chat.send_page_context:
            page_context = _augment_chat_page_context(page_context)
        else:
            page_context = None
        direct_target = direct_chat.resolve_direct_chat_target(cfg.ai)
        if direct_chat.direct_chat_preferred(cfg.ai, agent_id, direct_target) and not conversation_id:
            conversation_id = f"direct-{uuid.uuid4()}"
        session_context = chat_memory.build_prompt_context(conversation_id)
        rendered_message = _render_chat_message(message, page_context, session_context)

        if direct_chat.direct_chat_preferred(cfg.ai, agent_id, direct_target):
            try:
                target = direct_target or direct_chat.require_direct_chat_target(cfg.ai)
                result = await direct_chat.direct_chat_turn(
                    target=target,
                    message=message,
                    rendered_message=rendered_message,
                    conversation_id=conversation_id,
                )
                if result.get("conversation_id"):
                    chat_memory.record_turn(
                        conversation_id=str(result["conversation_id"]),
                        message=message,
                        page_context=page_context,
                        tool_calls=result.get("tool_calls") if isinstance(result.get("tool_calls"), list) else None,
                        response_text=((result.get("response") or {}).get("text") if isinstance(result.get("response"), dict) else None),
                        persist=bool(cfg.chat.persist_transcripts),
                        persist_days=int(cfg.retention.chat_days),
                    )
                _record_chat_turn_telemetry(
                    conversation_id=str(result.get("conversation_id") or conversation_id or "").strip() or None,
                    backend="direct",
                    agent_id=str(result.get("agent_id") or agent_id or "").strip() or None,
                    model_name=str(result.get("model") or target.model or "").strip() or None,
                    status="ok",
                    error_kind=None,
                    duration_ms=int(result.get("duration_ms") or 0),
                    tool_call_count=len(result.get("tool_calls") or []) if isinstance(result.get("tool_calls"), list) else 0,
                    page_context=page_context,
                )
                return result
            except direct_chat.DirectChatConfigError as exc:
                _record_chat_turn_telemetry(
                    conversation_id=conversation_id,
                    backend="direct",
                    agent_id=agent_id,
                    model_name=direct_target.model if direct_target is not None else None,
                    status="error",
                    error_kind="config",
                    duration_ms=0,
                    tool_call_count=0,
                    page_context=page_context,
                )
                raise HTTPException(
                    status_code=status.HTTP_412_PRECONDITION_FAILED,
                    detail=str(exc),
                ) from exc
            except httpx.HTTPStatusError as exc:
                _record_chat_turn_telemetry(
                    conversation_id=conversation_id,
                    backend="direct",
                    agent_id=agent_id,
                    model_name=direct_target.model if direct_target is not None else None,
                    status="error",
                    error_kind="upstream_http",
                    duration_ms=0,
                    tool_call_count=0,
                    page_context=page_context,
                )
                detail = exc.response.text if exc.response is not None else str(exc)
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Direct model chat failed: {detail}",
                ) from exc
            except Exception as exc:  # noqa: BLE001
                _record_chat_turn_telemetry(
                    conversation_id=conversation_id,
                    backend="direct",
                    agent_id=agent_id,
                    model_name=direct_target.model if direct_target is not None else None,
                    status="error",
                    error_kind="internal",
                    duration_ms=0,
                    tool_call_count=0,
                    page_context=page_context,
                )
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Direct model chat failed: {exc}",
                ) from exc

        started = time.perf_counter()
        try:
            upstream = await supervisor_client.conversation_process(
                text=rendered_message,
                conversation_id=conversation_id,
                agent_id=agent_id,
            )
        except supervisor_client.NoConversationAgentConfigured as exc:
            raise HTTPException(
                status_code=status.HTTP_412_PRECONDITION_FAILED,
                detail=(
                    "No Home Assistant conversation agent is configured. "
                    "Set one up in HA Assist / Conversations, then retry. "
                    f"Upstream detail: {exc}"
                ),
            ) from exc
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text if exc.response is not None else str(exc)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"HA conversation.process failed: {detail}",
            ) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"HA conversation proxy failed: {exc}",
            ) from exc

        duration_ms = max(0, int((time.perf_counter() - started) * 1000))
        result = _extract_chat_turn(
            upstream,
            requested_agent_id=agent_id,
            duration_ms=duration_ms,
        )
        if result.get("conversation_id"):
            chat_memory.record_turn(
                conversation_id=str(result["conversation_id"]),
                message=message,
                page_context=page_context,
                tool_calls=result.get("tool_calls") if isinstance(result.get("tool_calls"), list) else None,
                response_text=((result.get("response") or {}).get("text") if isinstance(result.get("response"), dict) else None),
                persist=bool(cfg.chat.persist_transcripts),
                persist_days=int(cfg.retention.chat_days),
            )
        _record_chat_turn_telemetry(
            conversation_id=str(result.get("conversation_id") or conversation_id or "").strip() or None,
            backend="ha",
            agent_id=str(result.get("agent_id") or agent_id or "").strip() or None,
            model_name=None,
            status="ok",
            error_kind=None,
            duration_ms=duration_ms,
            tool_call_count=len(result.get("tool_calls") or []) if isinstance(result.get("tool_calls"), list) else 0,
            page_context=page_context,
        )
        return result

    @app.get("/v1/chat/stats")
    def chat_stats(since: str | None = None) -> dict[str, object]:
        try:
            return get_store().get_chat_turn_stats(since=since)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "core", "checked_at": _utc_now()}

    @app.get("/v1/health/snapshot")
    def health_snapshot() -> dict[str, object]:
        try:
            return build_health_snapshot()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "computed_at": _utc_now()}

    @app.get("/v1/issues/active")
    def list_active_issues() -> dict[str, object]:
        # Issue detection is paused pending redesign — see tracking
        # issue #5 and placeholder issue #4. We deliberately return an
        # empty list with an explicit ``status: "placeholder"`` so
        # consumers (dashboard, MCP, AI reasoners) don't misread the
        # absence of issues as "all clear".
        from ..pipeline.reasoner import ISSUES_PAUSED, ISSUES_PAUSED_NOTE
        if ISSUES_PAUSED:
            return {
                "count": 0,
                "issues": [],
                "status": "placeholder",
                "note": ISSUES_PAUSED_NOTE,
                "computed_at": _utc_now(),
            }
        try:
            issues = get_store().list_active_issues()
            return {"count": len(issues), "issues": issues, "computed_at": _utc_now()}
        except Exception as exc:  # noqa: BLE001
            return {"count": 0, "issues": [], "error": str(exc), "computed_at": _utc_now()}

    @app.get("/v1/topology")
    def topology_snapshot(include_phantoms: bool = False) -> dict[str, object]:
        try:
            return topology_mod.build_topology(include_phantoms=include_phantoms)
        except Exception as exc:  # noqa: BLE001
            return {"nodes": [], "links": [], "error": str(exc), "computed_at": _utc_now()}

    @app.get("/v1/topology/history")
    def topology_history(limit: int = 20) -> dict[str, object]:
        try:
            snaps = get_store().list_topology_snapshots(limit=max(1, min(int(limit), 100)))
            return {"snapshots": snaps, "count": len(snaps)}
        except Exception as exc:  # noqa: BLE001
            return {"snapshots": [], "count": 0, "error": str(exc)}

    @app.get("/v1/topology/history/diff")
    def topology_history_diff(snapshot_id_a: int, snapshot_id_b: int) -> dict[str, object]:
        try:
            return topology_snapshot_mod.diff_topology(
                get_store(),
                snapshot_id_a=int(snapshot_id_a),
                snapshot_id_b=int(snapshot_id_b),
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.get("/v1/partitions")
    def partitions_snapshot(include_phantoms: bool = False) -> dict[str, object]:
        try:
            return _build_partition_state(include_phantoms=include_phantoms)
        except Exception as exc:  # noqa: BLE001
            return {"partition_count": 0, "partitions": [], "error": str(exc)}

    @app.get("/v1/routes/{eui64}")
    def routes_to_otbr(eui64: str) -> dict[str, object]:
        """Walk the multi-hop forwarding path from a node to the OTBR.

        Returns the full hop chain (with per-hop LQI, path_cost, link
        established), a completeness flag, and any issues (loop, partition
        mismatch, unknown next hop). Replaces the client-side path walk
        previously done in the dashboard JS so MCP/AI consumers get the
        same view.
        """
        try:
            return routing_mod.walk_route_to_otbr(eui64.lower())
        except Exception as exc:  # noqa: BLE001
            return {"source_eui64": eui64, "error": str(exc), "hops": []}

    @app.get("/v1/neighbors/{eui64}")
    def neighbors_for(eui64: str) -> dict[str, object]:
        """Enriched NeighborTable + RouteTable rows for one reporter.

        Names are resolved against the nodes table; next-hop RouterIds are
        resolved to their EUI64s within the partition. Use this instead of
        joining ``/v1/topology`` links client-side.
        """
        try:
            return routing_mod.list_neighbors_enriched(eui64.lower())
        except Exception as exc:  # noqa: BLE001
            return {"reporter_eui64": eui64, "error": str(exc), "neighbors": [], "routes": []}

    @app.get("/v1/links/stale")
    def links_stale() -> dict[str, object]:
        """List every link row whose neighbor EUI is not in the registry.

        These are the dead-link references — router caches pointing at
        EUIs HA has never heard of (recommissioned devices, abandoned
        pairings, ghost neighbors from a previous partition). Replaces
        the old ``/v1/phantoms`` view as the troubleshooting entry point:
        the EUI here is *not* a node, it's a reference to investigate.
        """
        try:
            rows = get_store().list_stale_links()
            return {"count": len(rows), "links": rows}
        except Exception as exc:  # noqa: BLE001
            return {"count": 0, "links": [], "error": str(exc)}

    @app.get("/v1/children/{eui64}")
    def children_for(eui64: str) -> dict[str, object]:
        """Child roster as seen from this parent router.

        Sleepy / MTD children only appear in their parent's NeighborTable,
        so this is the canonical view of "which end devices have attached
        to this router right now". Returns per-child link quality
        (RSSI/LQI/frame error rate), sleepiness (``rx_on_when_idle``),
        capacity headroom against the practical 10-child cap, and a
        ``registered`` flag indicating whether the child EUI is in the
        HA registry (false = stale child cache from a recommissioned or
        unpaired device).
        """
        try:
            return routing_mod.list_children_enriched(eui64.lower())
        except Exception as exc:  # noqa: BLE001
            return {"parent_eui64": eui64, "error": str(exc), "children": []}

    @app.get("/v1/nodes/{eui64}/analysis")
    def node_analysis_for(eui64: str) -> dict[str, object]:
        try:
            return analyze_node_mod.analyze_node(eui64.lower(), store=get_store())
        except Exception as exc:  # noqa: BLE001
            return {"node": None, "error": str(exc)}

    @app.get("/v1/network-data")
    def network_data_list() -> dict[str, object]:
        """All known partition Network Data rows, freshest first.

        Each row is the OTBR-sourced Thread Network Data for one partition
        — PAN ID, channel, on-mesh prefixes, external routes, BR Server
        entries, SRP services. Two rows = the network is partitioned.
        """
        try:
            rows = get_store().list_network_data()
            return {"count": len(rows), "partitions": rows}
        except Exception as exc:  # noqa: BLE001
            return {"count": 0, "partitions": [], "error": str(exc)}

    @app.get("/v1/network-data/{partition_id}")
    def network_data_one(partition_id: int) -> dict[str, object]:
        """Network Data for a specific partition."""
        try:
            row = get_store().get_network_data(int(partition_id))
            if row is None:
                return {"partition_id": partition_id, "error": "not found"}
            return row
        except Exception as exc:  # noqa: BLE001
            return {"partition_id": partition_id, "error": str(exc)}

    @app.get("/v1/phantoms")
    def phantoms_snapshot() -> dict[str, object]:
        try:
            return _build_phantom_list()
        except Exception as exc:  # noqa: BLE001
            return {"count": 0, "phantoms": [], "error": str(exc)}

    @app.get("/v1/counters/deltas")
    def counter_deltas() -> dict[str, object]:
        """Per-node counter deltas for 1h and 24h windows in a single shot.

        Dashboard uses this to render trend columns in the Nodes table
        without N round-trips. For each EUI we pull the 24h sample window
        once, then compute (last - first) per counter, and separately
        compute (last - first_within_1h) for the 1h subset. Counter
        resets (negative diff) report ``null``.
        """
        from datetime import timedelta as _td
        store = get_store()
        now = datetime.now(tz=UTC)
        since_24h = (now - _td(hours=24)).isoformat()
        since_1h = (now - _td(hours=1)).isoformat()
        until = now.isoformat()
        out: dict[str, dict[str, dict[str, object]]] = {}
        try:
            node_rows = store.list_nodes()
        except Exception:  # noqa: BLE001
            node_rows = []
        for nrow in node_rows:
            eui = nrow.get("eui64") if isinstance(nrow, dict) else None
            if not eui:
                continue
            try:
                samples = store.get_counter_samples(
                    eui64=eui, since=since_24h, until=until, limit=2000,
                )
            except Exception:  # noqa: BLE001
                continue
            if not samples:
                continue
            samples_1h = [r for r in samples if (r.get("observed_at") or "") >= since_1h]
            out[eui] = {
                "1h": _window_deltas(samples_1h),
                "24h": _window_deltas(samples),
            }
        return {
            "now": until,
            "windows": {"1h": since_1h, "24h": since_24h},
            "nodes": out,
        }

    @app.post("/v1/reasoner/run")
    def reasoner_run() -> dict[str, object]:
        try:
            return reasoner_mod.run_reasoner()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.post("/v1/discover/run")
    async def discover_run() -> dict[str, object]:
        """Trigger a Matter cluster-53 discovery + sync cycle."""
        try:
            return await device_discovery.discover_and_sync()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.get("/v1/dev/status")
    async def dev_status(include_phantoms: bool = False) -> dict[str, object]:
        try:
            sup: dict[str, object] = await supervisor_client.get_addon_info()
        except Exception as exc:  # noqa: BLE001
            sup = {"error": str(exc)}
        try:
            storage = get_store().stats()
        except Exception as exc:  # noqa: BLE001
            storage = {"error": str(exc)}
        try:
            ts_health = await ts_store.timeseries_health()
        except Exception as exc:  # noqa: BLE001
            ts_health = {"backend": "unknown", "error": str(exc)}
        try:
            cfg = _redact_config_secrets(get_config().model_dump())
        except Exception as exc:  # noqa: BLE001
            cfg = {"error": str(exc)}
        try:
            ingestion = otbr_adapter.get_state()
        except Exception as exc:  # noqa: BLE001
            ingestion = {"error": str(exc)}
        try:
            pipeline = pipeline_runner.get_runner_state()
        except Exception as exc:  # noqa: BLE001
            pipeline = {"error": str(exc)}
        # Pre-compute the set of failed stages so the dashboard (and any MCP
        # consumer) doesn't have to iterate stages client-side.
        if isinstance(pipeline, dict):
            stages = pipeline.get("stages") or {}
            if isinstance(stages, dict):
                pipeline["stages_failed"] = [
                    name for name, st in stages.items()
                    if isinstance(st, dict) and st.get("ok") is False
                ]
        try:
            all_nodes = nodes_mod.list_nodes_enriched(
                include_signal_strength=True,
                include_phantoms=include_phantoms,
            )
            # Server-side sort: phantoms last, then by display_name.
            # Consumers (UI, AI) should render in this order; do not re-sort.
            all_nodes.sort(key=lambda n: (
                1 if n.get("status") == "phantom" else 0,
                (n.get("display_name") or "").lower(),
            ))
        except Exception as exc:  # noqa: BLE001
            all_nodes = []
        health = health_snapshot()
        # Counts by status — saves every consumer recomputing them.
        node_counts: dict[str, int] = {"total": len(all_nodes)}
        for n in all_nodes:
            st = n.get("status") or "online"
            node_counts[st] = node_counts.get(st, 0) + 1
        node_counts.setdefault("online", 0)
        node_counts.setdefault("offline", 0)
        node_counts.setdefault("unregistered", 0)
        node_counts.setdefault("phantom", 0)
        try:
            partitions = _build_partition_state(include_phantoms=include_phantoms)
            # Human-readable summary so consumers don't string-format it.
            if isinstance(partitions, dict) and "partition_count" in partitions:
                pc = partitions.get("partition_count", 0)
                if pc <= 0:
                    partitions["summary"] = "no partitions discovered"
                elif pc == 1:
                    partitions["summary"] = "single partition"
                else:
                    partitions["summary"] = f"network is split across {pc} partitions"
        except Exception as exc:  # noqa: BLE001
            partitions = {"error": str(exc)}
        try:
            phantoms = _build_phantom_list()
        except Exception as exc:  # noqa: BLE001
            phantoms = {"error": str(exc), "phantoms": []}
        # Stale link count: troubleshooting bait surfaced from links table.
        # See ``/v1/links/stale`` for the full list.
        try:
            stale_link_count = len(get_store().list_stale_links())
        except Exception:  # noqa: BLE001
            stale_link_count = 0
        # Resolve OTBR up-front so consumers don't infer it from heuristics.
        try:
            otbr = routing_mod.find_otbr()
            otbr_eui64 = otbr.get("eui64") if otbr else None
        except Exception:  # noqa: BLE001
            otbr_eui64 = None
        topo = topology_snapshot(include_phantoms=include_phantoms)
        graph_diagnostics = topology_mod.derive_graph_diagnostics(topo)
        diagnostics_summary = _build_diagnostics_summary(
            supervisor=sup,
            storage=storage,
            timeseries=ts_health,
            ingestion=ingestion,
            pipeline=pipeline,
            health=health,
            partitions=partitions,
            phantoms=phantoms,
            stale_link_count=stale_link_count,
            config=cfg,
            graph_diagnostics=graph_diagnostics,
        )
        return {
            "addon_version": ADDON_VERSION,
            "checked_at": _utc_now(),
            "supervisor": sup,
            "health": health,
            "issues": list_active_issues(),
            "topology": topo,
            "partitions": partitions,
            "phantoms": phantoms,
            "recent_logs": _tail_log(80),
            "storage": storage,
            "timeseries": ts_health,
            "config": cfg,
            "ingestion": ingestion,
            "pipeline": pipeline,
            "otbr_eui64": otbr_eui64,
            "node_counts": node_counts,
            "stale_link_count": stale_link_count,
            "diagnostics_summary": diagnostics_summary,
            "graph_diagnostics": graph_diagnostics,
            "all_nodes": all_nodes,
        }

    @app.get("/v1/pipeline/state")
    def pipeline_state() -> dict[str, object]:
        """Last pipeline tick summary (stages, durations, errors)."""
        try:
            return pipeline_runner.get_runner_state()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.post("/v1/pipeline/run")
    async def pipeline_run() -> dict[str, object]:
        """Force-trigger an immediate pipeline tick (out-of-band). The
        regular cadence keeps running independently.
        """
        try:
            return await pipeline_runner.run_tick()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.get("/v1/dev/mcp-health")
    async def dev_mcp_health() -> dict[str, object]:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                r = await client.get("http://127.0.0.1:8100/health")
            return {"ok": r.status_code == 200, "status_code": r.status_code}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "detail": str(exc)}

    # -- OTBR ingestion (Phase 2.5) ---------------------------------------

    @app.get("/v1/ingest/state")
    def ingest_state() -> dict[str, object]:
        try:
            return otbr_adapter.get_state()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.get("/v1/ingest/candidates")
    async def ingest_candidates() -> dict[str, object]:
        try:
            cands = await otbr_adapter.list_candidates()
            return {"count": len(cands), "candidates": cands}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "candidates": []}

    @app.post("/v1/ingest/run")
    async def ingest_run() -> dict[str, object]:
        try:
            return await otbr_adapter.ingest_once()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.post("/v1/ingest/slug")
    async def ingest_set_slug(payload: dict[str, str]) -> dict[str, object]:
        slug = (payload or {}).get("slug", "").strip()
        if not slug:
            return {"error": "slug required"}
        try:
            return otbr_adapter.set_slug(slug)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.get("/v1/ingest/debug")
    async def ingest_debug() -> dict[str, object]:
        """Debug endpoint: fetch raw OTBR logs to inspect format."""
        try:
            ingest_st = otbr_adapter.get_state()
            slug = ingest_st.get("slug")
            if not slug:
                return {"error": "no OTBR slug configured"}
            # Fetch latest 50 lines from the OTBR addon
            logs = await supervisor_client.get_addon_logs(slug=slug, lines=50)
            return {
                "slug": slug,
                "log_line_count": len(logs),
                "sample_lines": logs[-10:] if logs else [],
                "raw_sample": "\n".join(logs[-20:]) if logs else "",
            }
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    # -- Node metadata (Phase 3) ------------------------------------------

    @app.get("/v1/nodes/all")
    def nodes_list() -> dict[str, object]:
        try:
            nodes = nodes_mod.list_nodes_enriched(include_signal_strength=True)
            return {"count": len(nodes), "nodes": nodes}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "nodes": []}

    @app.get("/v1/nodes/{eui64}")
    def nodes_get(eui64: str) -> dict[str, object]:
        try:
            return nodes_mod.get_node_summary(eui64, include_signal_strength=True)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.post("/v1/nodes/{eui64}/friendly-name")
    def nodes_set_name(eui64: str, payload: dict[str, str]) -> dict[str, object]:
        name = (payload or {}).get("name", "").strip()
        if not name:
            return {"error": "name required"}
        try:
            ok = get_store().set_node_friendly_name(eui64, name)
            if not ok:
                return {"error": f"node {eui64} not found"}
            return nodes_mod.get_node_summary(eui64, include_signal_strength=True)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.get("/v1/assessment/state")
    def assessment_state() -> dict[str, object]:
        try:
            from ..services.assessment.scheduler import (
                AssessmentScheduler,
                ScheduleConfig,
            )

            cfg = get_config().assessment
            sched = AssessmentScheduler(
                store=get_store(),
                config=ScheduleConfig(
                    enabled=cfg.enabled,
                    probation_interval_minutes=cfg.probation_interval_minutes,
                    probation_checks=cfg.probation_checks,
                    relaxing_initial_hours=cfg.relaxing_initial_hours,
                    relaxing_max_hours=cfg.relaxing_max_hours,
                    heightened_initial_minutes=cfg.heightened_initial_minutes,
                    heightened_max_hours=cfg.heightened_max_hours,
                    engaged_interval_minutes=cfg.engaged_interval_minutes,
                    engaged_decay_minutes=cfg.engaged_decay_minutes,
                    daily_budget_calls=cfg.daily_budget_calls,
                ),
            )
            snap = sched.snapshot()
            return {
                "enabled": snap.enabled,
                "state": snap.state,
                "current_interval_seconds": snap.current_interval_seconds,
                "next_check_at": snap.next_assessment_at,
                "last_check_at": snap.last_assessment_at,
                "last_verdict": snap.reason,
                "calls_today": snap.budget_calls_used,
                "daily_budget": snap.daily_budget_calls,
                "probation_checks_remaining": max(0, cfg.probation_checks - snap.consecutive_ok),
                "reason": snap.reason,
            }
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    def _assessment_scheduler():
        from ..services.assessment.scheduler import AssessmentScheduler, ScheduleConfig

        cfg = ScheduleConfig.from_dict(get_config().assessment.model_dump())
        return AssessmentScheduler(store=get_store(), config=cfg)

    def _assessment_engine():
        from ..services.assessment.engine import AssessmentEngine

        cfg = get_config().assessment
        return AssessmentEngine(
            store=get_store(),
            context_recent_findings_default=cfg.context_recent_findings_default,
            context_recent_findings_by_model=cfg.context_recent_findings_by_model,
        )

    def _assessment_result_payload(result) -> dict[str, object]:
        return {
            "envelope": result.envelope.to_dict(),
            "finding_id": result.finding_id,
            "finding_key": result.finding_key,
            "dedup_hit": result.dedup_hit,
            "parse_attempts": result.parse_attempts,
            "duration_seconds": result.duration_seconds,
            "cleared_count": result.cleared_count,
            "suppressed": result.suppressed,
        }

    @app.get("/v1/assessment/findings")
    def assessment_findings(state: str = "open", limit: int = 50) -> dict[str, object]:
        try:
            rows = get_store().list_assessment_findings(state=state, limit=limit)
            return {"findings": rows, "count": len(rows)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "findings": []}

    @app.get("/v1/assessment/history")
    def assessment_history(limit: int = 20, offset: int = 0) -> dict[str, object]:
        try:
            safe_limit = max(1, min(int(limit), 100))
            safe_offset = max(0, int(offset))
            rows = get_store().list_assessment_runs(
                limit=safe_limit + 1,
                offset=safe_offset,
            )
            return {
                "runs": rows[:safe_limit],
                "count": len(rows[:safe_limit]),
                "limit": safe_limit,
                "offset": safe_offset,
                "has_more": len(rows) > safe_limit,
            }
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "runs": []}

    @app.post("/v1/assessment/run-now")
    def assessment_run_now(payload: dict[str, object] | None = None) -> dict[str, object]:
        try:
            scheduler = _assessment_scheduler()
            decision = scheduler.decide(force=True)
            decision_payload = {
                "should_run": decision.should_run,
                "reason": decision.reason,
                "next_run_at": decision.next_run_at,
                "state": decision.state,
                "budget_exhausted": decision.budget_exhausted,
            }
            if not decision.should_run:
                return {"ok": False, "decision": decision_payload}

            result = asyncio.run(_assessment_engine().run_once(extra_context=payload))
            snapshot = scheduler.record_assessment(verdict=result.envelope.verdict)
            return {
                "ok": True,
                "decision": decision_payload,
                "result": _assessment_result_payload(result),
                "schedule": snapshot.as_dict(),
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    @app.post("/v1/assessment/findings/{finding_id}/dismiss")
    def assessment_dismiss(
        finding_id: int, payload: dict[str, object] | None = None
    ) -> dict[str, object]:
        try:
            suppress_seconds = int((payload or {}).get("suppress_seconds") or 86400)
            row = get_store().dismiss_assessment_finding(
                finding_id, suppress_seconds=suppress_seconds
            )
            if row is None:
                return {"error": f"finding {finding_id} not found"}
            return {"ok": True, "finding": row}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.post("/v1/assessment/findings/{finding_id}/feedback")
    def assessment_feedback(
        finding_id: int, payload: dict[str, object]
    ) -> dict[str, object]:
        try:
            from ..services.assessment import feedback as feedback_mod

            outcome = str((payload or {}).get("outcome") or "").strip()
            notes = (payload or {}).get("notes")
            notes_str = str(notes) if notes is not None else None
            result = feedback_mod.mark_outcome(
                finding_id=finding_id,
                outcome=outcome,
                notes=notes_str,
                store=get_store(),
            )
            return {"ok": True, "result": result}
        except LookupError as exc:
            return {"error": str(exc)}
        except ValueError as exc:
            return {"error": str(exc)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.get("/v1/assessment/quality")
    def assessment_quality(since_hours: int = 168) -> dict[str, object]:
        try:
            from datetime import timedelta

            from ..services.assessment import feedback as feedback_mod

            since = (datetime.now(UTC) - timedelta(hours=since_hours)).isoformat()
            return feedback_mod.quality_summary(since=since, store=get_store())
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    return app
