from __future__ import annotations

import httpx
from fastapi import APIRouter

from .. import supervisor_client
from ..diagnostics import build_diagnostics_summary, redact_config_secrets
from ...pipeline import nodes as nodes_mod
from ...pipeline import otbr_adapter
from ...pipeline import routing as routing_mod
from ...pipeline import runner as pipeline_runner
from ...pipeline import topology as topology_mod
from ...storage import influx_store as ts_store
from ..mcp_tools import _build_partition_state, _build_phantom_list


def create_dev_pipeline_router(
    *,
    addon_version: str,
    get_config_fn,
    get_store_fn,
    utc_now: callable,
    tail_log: callable,
) -> APIRouter:
    router = APIRouter()

    def _health_snapshot() -> dict[str, object]:
        from ...health import build_health_snapshot

        try:
            return build_health_snapshot()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "computed_at": utc_now()}

    def _list_active_issues() -> dict[str, object]:
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
            return {"count": 0, "issues": [], "error": str(exc), "computed_at": utc_now()}

    def _topology_snapshot(*, include_phantoms: bool) -> dict[str, object]:
        try:
            return topology_mod.build_topology(include_phantoms=include_phantoms)
        except Exception as exc:  # noqa: BLE001
            return {"nodes": [], "links": [], "error": str(exc), "computed_at": utc_now()}

    @router.get("/v1/dev/status")
    async def dev_status(include_phantoms: bool = False) -> dict[str, object]:
        try:
            sup: dict[str, object] = await supervisor_client.get_addon_info()
        except Exception as exc:  # noqa: BLE001
            sup = {"error": str(exc)}
        try:
            storage = get_store_fn().stats()
        except Exception as exc:  # noqa: BLE001
            storage = {"error": str(exc)}
        try:
            ts_health = await ts_store.timeseries_health()
        except Exception as exc:  # noqa: BLE001
            ts_health = {"backend": "unknown", "error": str(exc)}
        try:
            cfg = redact_config_secrets(get_config_fn().model_dump())
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
            all_nodes.sort(
                key=lambda n: (
                    1 if n.get("status") == "phantom" else 0,
                    (n.get("display_name") or "").lower(),
                )
            )
        except Exception:  # noqa: BLE001
            all_nodes = []
        health = _health_snapshot()
        node_counts: dict[str, int] = {"total": len(all_nodes)}
        for row in all_nodes:
            st = row.get("status") or "online"
            node_counts[st] = node_counts.get(st, 0) + 1
        node_counts.setdefault("online", 0)
        node_counts.setdefault("sleeping", 0)
        node_counts.setdefault("offline", 0)
        node_counts.setdefault("unregistered", 0)
        node_counts.setdefault("phantom", 0)
        try:
            partitions = _build_partition_state(include_phantoms=include_phantoms)
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
        try:
            stale_link_count = len(get_store_fn().list_stale_links())
        except Exception:  # noqa: BLE001
            stale_link_count = 0
        try:
            otbr = routing_mod.find_otbr()
            otbr_eui64 = otbr.get("eui64") if otbr else None
        except Exception:  # noqa: BLE001
            otbr_eui64 = None
        topo = _topology_snapshot(include_phantoms=include_phantoms)
        graph_diagnostics = topology_mod.derive_graph_diagnostics(topo)
        diagnostics_summary = build_diagnostics_summary(
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
            "addon_version": addon_version,
            "checked_at": utc_now(),
            "supervisor": sup,
            "health": health,
            "issues": _list_active_issues(),
            "topology": topo,
            "partitions": partitions,
            "phantoms": phantoms,
            "recent_logs": tail_log(80),
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

    @router.get("/v1/pipeline/state")
    def pipeline_state() -> dict[str, object]:
        try:
            return pipeline_runner.get_runner_state()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @router.post("/v1/pipeline/run")
    async def pipeline_run() -> dict[str, object]:
        try:
            return await pipeline_runner.run_tick()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @router.get("/v1/dev/mcp-health")
    async def dev_mcp_health() -> dict[str, object]:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                r = await client.get("http://127.0.0.1:8100/health")
            return {"ok": r.status_code == 200, "status_code": r.status_code}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "detail": str(exc)}

    return router

