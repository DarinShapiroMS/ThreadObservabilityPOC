"""Core HTTP API for Thread Observability add-on.

Serves a lightweight status dashboard at ``/`` (Ingress entry-point) plus
JSON endpoints under ``/v1/...`` for programmatic access.
"""

import asyncio
import contextlib
import logging
import re
from datetime import UTC, datetime
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from . import supervisor_client
from .mcp_tools import _build_partition_state, _build_phantom_list
from ..config import get_config
from ..health import build_health_snapshot
from ..pipeline import device_discovery
from ..pipeline import nodes as nodes_mod
from ..pipeline import otbr_adapter
from ..pipeline import otbr_rest
from ..pipeline import reasoner as reasoner_mod
from ..pipeline import topology as topology_mod
from ..storage import influx_store as ts_store
from ..storage.sqlite_store import get_store

log = logging.getLogger(__name__)


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


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _tail_log(n: int = 80) -> list[str]:
    if not LOG_PATH.exists():
        return []
    try:
        return LOG_PATH.read_text(errors="replace").splitlines()[-n:]
    except OSError:
        return []


DASHBOARD_HTML = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")


async def _periodic(name: str, interval: int, coro_factory) -> None:
    """Run ``coro_factory()`` every ``interval`` seconds, logging exceptions.

    The first iteration runs after one ``interval`` so startup races settle.
    """
    while True:
        try:
            await asyncio.sleep(interval)
            await coro_factory()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("periodic task %s failed", name)


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI):
    """Start/stop background scheduler tasks alongside the FastAPI app.

    Everything the UI shows is produced by these loops — there is no
    user-initiated trigger for data collection.
    """
    cfg = get_config()
    ingest_interval = int(getattr(cfg.scheduler, "ingestion_interval_seconds", 10))
    discover_interval = int(getattr(cfg.scheduler, "discover_interval_seconds", 300))
    reasoner_interval = int(getattr(cfg.scheduler, "reasoner_interval_seconds", 120))
    otbr_rest_interval = int(getattr(cfg.scheduler, "otbr_rest_interval_seconds", 60))

    tasks = [
        asyncio.create_task(
            otbr_adapter.run_forever(interval_seconds=ingest_interval),
            name="otbr-ingest-loop",
        ),
        asyncio.create_task(
            otbr_rest.run_forever(interval_seconds=otbr_rest_interval),
            name="otbr-rest-loop",
        ),
        asyncio.create_task(
            _periodic("matter-discovery", discover_interval, device_discovery.discover_and_sync),
            name="matter-discovery-loop",
        ),
        asyncio.create_task(
            _periodic(
                "reasoner",
                reasoner_interval,
                lambda: asyncio.to_thread(reasoner_mod.run_reasoner),
            ),
            name="reasoner-loop",
        ),
    ]
    log.info(
        "scheduler started: ingest=%ss otbr_rest=%ss discover=%ss reasoner=%ss",
        ingest_interval, otbr_rest_interval, discover_interval, reasoner_interval,
    )
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        log.info("scheduler stopped")


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

    @app.get("/v1/partitions")
    def partitions_snapshot(include_phantoms: bool = False) -> dict[str, object]:
        try:
            return _build_partition_state(include_phantoms=include_phantoms)
        except Exception as exc:  # noqa: BLE001
            return {"partition_count": 0, "partitions": [], "error": str(exc)}

    @app.get("/v1/phantoms")
    def phantoms_snapshot() -> dict[str, object]:
        try:
            return _build_phantom_list()
        except Exception as exc:  # noqa: BLE001
            return {"count": 0, "phantoms": [], "error": str(exc)}

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
            cfg = get_config().model_dump()
            if cfg.get("influx", {}).get("token"):
                cfg["influx"]["token"] = "***"
        except Exception as exc:  # noqa: BLE001
            cfg = {"error": str(exc)}
        try:
            ingestion = otbr_adapter.get_state()
        except Exception as exc:  # noqa: BLE001
            ingestion = {"error": str(exc)}
        try:
            all_nodes = nodes_mod.list_nodes_enriched(
                include_signal_strength=True,
                include_phantoms=include_phantoms,
            )
        except Exception as exc:  # noqa: BLE001
            all_nodes = []
        try:
            partitions = _build_partition_state(include_phantoms=include_phantoms)
        except Exception as exc:  # noqa: BLE001
            partitions = {"error": str(exc)}
        try:
            phantoms = _build_phantom_list()
        except Exception as exc:  # noqa: BLE001
            phantoms = {"error": str(exc), "phantoms": []}
        return {
            "addon_version": ADDON_VERSION,
            "checked_at": _utc_now(),
            "supervisor": sup,
            "health": health_snapshot(),
            "issues": list_active_issues(),
            "topology": topology_snapshot(include_phantoms=include_phantoms),
            "partitions": partitions,
            "phantoms": phantoms,
            "recent_logs": _tail_log(80),
            "storage": storage,
            "timeseries": ts_health,
            "config": cfg,
            "ingestion": ingestion,
            "all_nodes": all_nodes,
        }

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

    return app
