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
from ..pipeline import routing as routing_mod
from ..pipeline import runner as pipeline_runner
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
            "pipeline": pipeline,
            "otbr_eui64": otbr_eui64,
            "node_counts": node_counts,
            "stale_link_count": stale_link_count,
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

    return app
