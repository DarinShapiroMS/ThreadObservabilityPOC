from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter

from ..mcp_tools import _build_partition_state, _build_phantom_list
from ...health import build_health_snapshot
from ...pipeline import analyze_node as analyze_node_mod
from ...pipeline import device_discovery
from ...pipeline import reasoner as reasoner_mod
from ...pipeline import routing as routing_mod
from ...pipeline import topology as topology_mod
from ...pipeline import topology_snapshot as topology_snapshot_mod
from .. import signal_series as signal_series_mod
from .. import link_signal_history as link_signal_history_mod


def create_mesh_router(*, get_store_fn, utc_now: callable) -> APIRouter:
    router = APIRouter()

    @router.get("/v1/health/snapshot")
    def health_snapshot() -> dict[str, object]:
        try:
            return build_health_snapshot()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "computed_at": utc_now()}

    @router.get("/v1/issues/active")
    def list_active_issues() -> dict[str, object]:
        from ...pipeline.reasoner import ISSUES_PAUSED, ISSUES_PAUSED_NOTE

        if ISSUES_PAUSED:
            return {
                "count": 0,
                "issues": [],
                "status": "placeholder",
                "note": ISSUES_PAUSED_NOTE,
                "computed_at": utc_now(),
            }
        try:
            issues = get_store_fn().list_active_issues()
            return {"count": len(issues), "issues": issues, "computed_at": utc_now()}
        except Exception as exc:  # noqa: BLE001
            return {
                "count": 0,
                "issues": [],
                "error": str(exc),
                "computed_at": utc_now(),
            }

    @router.get("/v1/topology")
    def topology_snapshot(include_phantoms: bool = False) -> dict[str, object]:
        try:
            return topology_mod.build_topology(include_phantoms=include_phantoms)
        except Exception as exc:  # noqa: BLE001
            return {"nodes": [], "links": [], "error": str(exc), "computed_at": utc_now()}

    @router.get("/v1/topology/history")
    def topology_history(limit: int = 20) -> dict[str, object]:
        try:
            snaps = get_store_fn().list_topology_snapshots(limit=max(1, min(int(limit), 100)))
            return {"snapshots": snaps, "count": len(snaps)}
        except Exception as exc:  # noqa: BLE001
            return {"snapshots": [], "count": 0, "error": str(exc)}

    @router.get("/v1/topology/history/diff")
    def topology_history_diff(snapshot_id_a: int, snapshot_id_b: int) -> dict[str, object]:
        try:
            return topology_snapshot_mod.diff_topology(
                get_store_fn(),
                snapshot_id_a=int(snapshot_id_a),
                snapshot_id_b=int(snapshot_id_b),
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @router.get("/v1/partitions")
    def partitions_snapshot(include_phantoms: bool = False) -> dict[str, object]:
        try:
            return _build_partition_state(include_phantoms=include_phantoms)
        except Exception as exc:  # noqa: BLE001
            return {"partition_count": 0, "partitions": [], "error": str(exc)}

    @router.get("/v1/routes/{eui64}")
    def routes_to_otbr(eui64: str) -> dict[str, object]:
        try:
            return routing_mod.walk_route_to_otbr(eui64.lower())
        except Exception as exc:  # noqa: BLE001
            return {"source_eui64": eui64, "error": str(exc), "hops": []}

    @router.get("/v1/neighbors/{eui64}")
    def neighbors_for(eui64: str) -> dict[str, object]:
        try:
            return routing_mod.list_neighbors_enriched(eui64.lower())
        except Exception as exc:  # noqa: BLE001
            return {
                "reporter_eui64": eui64,
                "error": str(exc),
                "neighbors": [],
                "routes": [],
            }

    @router.get("/v1/links/stale")
    def links_stale() -> dict[str, object]:
        try:
            rows = get_store_fn().list_stale_links()
            return {"count": len(rows), "links": rows}
        except Exception as exc:  # noqa: BLE001
            return {"count": 0, "links": [], "error": str(exc)}

    @router.get("/v1/children/{eui64}")
    def children_for(eui64: str) -> dict[str, object]:
        try:
            return routing_mod.list_children_enriched(eui64.lower())
        except Exception as exc:  # noqa: BLE001
            return {"parent_eui64": eui64, "error": str(exc), "children": []}

    @router.get("/v1/nodes/{eui64}/analysis")
    def node_analysis_for(eui64: str) -> dict[str, object]:
        try:
            return analyze_node_mod.analyze_node(eui64.lower(), store=get_store_fn())
        except Exception as exc:  # noqa: BLE001
            return {"node": None, "error": str(exc)}

    @router.get("/v1/network-data")
    def network_data_list() -> dict[str, object]:
        try:
            rows = get_store_fn().list_network_data()
            return {"count": len(rows), "partitions": rows}
        except Exception as exc:  # noqa: BLE001
            return {"count": 0, "partitions": [], "error": str(exc)}

    @router.get("/v1/network-data/{partition_id}")
    def network_data_one(partition_id: int) -> dict[str, object]:
        try:
            row = get_store_fn().get_network_data(int(partition_id))
            if row is None:
                return {"partition_id": partition_id, "error": "not found"}
            return row
        except Exception as exc:  # noqa: BLE001
            return {"partition_id": partition_id, "error": str(exc)}

    @router.get("/v1/phantoms")
    def phantoms_snapshot() -> dict[str, object]:
        try:
            return _build_phantom_list()
        except Exception as exc:  # noqa: BLE001
            return {"count": 0, "phantoms": [], "error": str(exc)}

    def _window_deltas(samples: list[dict[str, object]]) -> dict[str, object]:
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

    @router.get("/v1/counters/deltas")
    def counter_deltas() -> dict[str, object]:
        from datetime import timedelta as _td

        store = get_store_fn()
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
                    eui64=eui,
                    since=since_24h,
                    until=until,
                    limit=2000,
                )
            except Exception:  # noqa: BLE001
                continue
            if not samples:
                continue
            samples_1h = [r for r in samples if (r.get("observed_at") or "") >= since_1h]
            out[eui] = {"1h": _window_deltas(samples_1h), "24h": _window_deltas(samples)}
        return {"now": until, "windows": {"1h": since_1h, "24h": since_24h}, "nodes": out}

    @router.get("/v1/signals/{eui64}/series")
    def signal_series(
        eui64: str,
        since: str | None = None,
        until: str | None = None,
        resolution: str = "raw",
    ) -> dict[str, object]:
        try:
            return signal_series_mod.get_signal_series(
                eui64=eui64.lower(),
                since=since,
                until=until,
                resolution=resolution,
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "series": [], "metrics": {}}

    @router.get("/v1/signals/{eui64}/links/history")
    def node_link_signal_history(
        eui64: str,
        since: str | None = None,
        until: str | None = None,
        peer_eui64: str | None = None,
        source: str | None = None,
        limit: int = 5000,
    ) -> dict[str, object]:
        try:
            return link_signal_history_mod.get_node_link_signal_history(
                eui64=eui64.lower(),
                since=since,
                until=until,
                peer_eui64=peer_eui64.lower() if isinstance(peer_eui64, str) else None,
                source=source,
                limit=limit,
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "links": []}

    @router.post("/v1/reasoner/run")
    def reasoner_run() -> dict[str, object]:
        try:
            return reasoner_mod.run_reasoner()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @router.post("/v1/discover/run")
    async def discover_run() -> dict[str, object]:
        try:
            return await device_discovery.discover_and_sync()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    return router

