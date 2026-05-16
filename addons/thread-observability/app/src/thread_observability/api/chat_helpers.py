"""Chat-related HTTP helper functions."""

from __future__ import annotations

import json
import logging
import time
import uuid
from pathlib import Path

import httpx
from fastapi import HTTPException, status

from . import supervisor_client
from ..config import ThreadObsConfig, get_config
from ..pipeline import topology as topology_mod
from ..services import chat_memory
from ..services import direct_chat
from ..storage.sqlite_store import get_store

log = logging.getLogger(__name__)

_CHAT_STARTER_PROMPTS_PATH = Path(__file__).parent / "chat_starter_prompts.json"
_CHAT_KNOWN_THREAD_TOOLS = frozenset(
    {"get_health_snapshot", "get_mesh_state", "list_active_issues", "start_triage"}
)

HA_MCP_CLIENT_URL = "http://9e5048e8-thread-observability:8100/mcp/sse"
HA_INTEGRATIONS_URL = "/config/integrations/dashboard"


def get_runtime_chat_config() -> ThreadObsConfig:
    cfg = get_config()
    options_path = Path(str(getattr(cfg, "options_path", "") or "")).expanduser()
    if getattr(cfg, "options_loaded", False) or options_path.exists():
        try:
            return ThreadObsConfig.load(options_path)
        except Exception:  # noqa: BLE001
            log.exception(
                "failed to reload chat config from %s; using cached config", options_path
            )
    return cfg


def load_chat_starter_prompts() -> list[str]:
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


def agent_has_thread_tools(row: dict[str, object]) -> bool:
    tool_names = row.get("tool_names") if isinstance(row.get("tool_names"), list) else []
    if not tool_names:
        return bool(row.get("has_thread_tools"))
    normalized = {str(name).strip() for name in tool_names if str(name).strip()}
    return any(name in _CHAT_KNOWN_THREAD_TOOLS for name in normalized)


def render_chat_message(
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
    sections.append(f"User message: {text}")
    return "\\n\\n".join(sections) if sections else text


def augment_chat_page_context(
    page_context: dict[str, object] | None,
) -> dict[str, object] | None:
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


def record_chat_turn_telemetry(
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
    utc_now: callable,
    get_store_fn=get_store,
) -> None:
    try:
        get_store_fn().record_chat_turn_stat(
            conversation_id=conversation_id,
            recorded_at=utc_now(),
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


def extract_chat_turn(
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
        "response": {"text": response_text, "card": card},
        "tool_calls": tool_calls,
        "duration_ms": duration_ms,
        "model": model,
        "streaming": False,
    }


async def chat_turn_via_direct_model(
    *,
    cfg: ThreadObsConfig,
    message: str,
    rendered_message: str,
    conversation_id: str | None,
    agent_id: str | None,
    page_context: dict[str, object] | None,
    utc_now: callable,
    get_store_fn=get_store,
) -> dict[str, object]:
    direct_target = direct_chat.resolve_direct_chat_target(cfg.ai)
    if direct_chat.direct_chat_preferred(cfg.ai, agent_id, direct_target) and not conversation_id:
        conversation_id = f"direct-{uuid.uuid4()}"
    try:
        target = direct_target or direct_chat.require_direct_chat_target(cfg.ai)
        result = await direct_chat.direct_chat_turn(
            target=target,
            message=message,
            rendered_message=rendered_message,
            conversation_id=conversation_id,
        )
        if result.get("conversation_id"):
            transcript = result.pop("transcript", None)
            chat_memory.record_turn(
                conversation_id=str(result["conversation_id"]),
                message=message,
                page_context=page_context,
                tool_calls=result.get("tool_calls") if isinstance(result.get("tool_calls"), list) else None,
                response_text=((result.get("response") or {}).get("text") if isinstance(result.get("response"), dict) else None),
                backend="direct",
                agent_id=str(result.get("agent_id") or "").strip() or None,
                model_name=str(result.get("model") or target.model or "").strip() or None,
                transcript=transcript if isinstance(transcript, dict) else None,
                persist=bool(cfg.chat.persist_transcripts),
                persist_days=int(cfg.retention.chat_days),
            )
        record_chat_turn_telemetry(
            conversation_id=str(result.get("conversation_id") or conversation_id or "").strip() or None,
            backend="direct",
            agent_id=str(result.get("agent_id") or agent_id or "").strip() or None,
            model_name=str(result.get("model") or target.model or "").strip() or None,
            status="ok",
            error_kind=None,
            duration_ms=int(result.get("duration_ms") or 0),
            tool_call_count=len(result.get("tool_calls") or []) if isinstance(result.get("tool_calls"), list) else 0,
            page_context=page_context,
            utc_now=utc_now,
            get_store_fn=get_store_fn,
        )
        return result
    except direct_chat.DirectChatConfigError as exc:
        record_chat_turn_telemetry(
            conversation_id=conversation_id,
            backend="direct",
            agent_id=agent_id,
            model_name=direct_target.model if direct_target is not None else None,
            status="error",
            error_kind="config",
            duration_ms=0,
            tool_call_count=0,
            page_context=page_context,
            utc_now=utc_now,
            get_store_fn=get_store_fn,
        )
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED, detail=str(exc)) from exc
    except httpx.HTTPStatusError as exc:
        record_chat_turn_telemetry(
            conversation_id=conversation_id,
            backend="direct",
            agent_id=agent_id,
            model_name=direct_target.model if direct_target is not None else None,
            status="error",
            error_kind="upstream_http",
            duration_ms=0,
            tool_call_count=0,
            page_context=page_context,
            utc_now=utc_now,
            get_store_fn=get_store_fn,
        )
        detail = exc.response.text if exc.response is not None else str(exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Direct model chat failed: {detail}",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        record_chat_turn_telemetry(
            conversation_id=conversation_id,
            backend="direct",
            agent_id=agent_id,
            model_name=direct_target.model if direct_target is not None else None,
            status="error",
            error_kind="internal",
            duration_ms=0,
            tool_call_count=0,
            page_context=page_context,
            utc_now=utc_now,
            get_store_fn=get_store_fn,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Direct model chat failed: {exc}",
        ) from exc


async def chat_turn_via_supervisor_proxy(
    *,
    message: str,
    rendered_message: str,
    conversation_id: str | None,
    agent_id: str | None,
    page_context: dict[str, object] | None,
    utc_now: callable,
    persist_transcripts: bool,
    retention_days: int,
    get_store_fn=get_store,
) -> dict[str, object]:
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
    result = extract_chat_turn(
        upstream,
        requested_agent_id=agent_id,
        duration_ms=duration_ms,
    )
    if result.get("conversation_id"):
        transcript = {
            "kind": "ha_conversation_proxy",
            "rendered_message": rendered_message,
            "upstream_response": upstream,
        }
        chat_memory.record_turn(
            conversation_id=str(result["conversation_id"]),
            message=message,
            page_context=page_context,
            tool_calls=result.get("tool_calls") if isinstance(result.get("tool_calls"), list) else None,
            response_text=((result.get("response") or {}).get("text") if isinstance(result.get("response"), dict) else None),
            backend="ha",
            agent_id=str(result.get("agent_id") or agent_id or "").strip() or None,
            model_name=None,
            transcript=transcript,
            persist=persist_transcripts,
            persist_days=retention_days,
        )
    record_chat_turn_telemetry(
        conversation_id=str(result.get("conversation_id") or conversation_id or "").strip() or None,
        backend="ha",
        agent_id=str(result.get("agent_id") or agent_id or "").strip() or None,
        model_name=None,
        status="ok",
        error_kind=None,
        duration_ms=duration_ms,
        tool_call_count=len(result.get("tool_calls") or []) if isinstance(result.get("tool_calls"), list) else 0,
        page_context=page_context,
        utc_now=utc_now,
        get_store_fn=get_store_fn,
    )
    return result
