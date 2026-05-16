from __future__ import annotations

from fastapi import APIRouter

from ...pipeline import nodes as nodes_mod


def create_nodes_router(*, get_store_fn) -> APIRouter:
    router = APIRouter()

    @router.get("/v1/nodes/all")
    def nodes_list() -> dict[str, object]:
        try:
            nodes = nodes_mod.list_nodes_enriched(include_signal_strength=True)
            return {"count": len(nodes), "nodes": nodes}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "nodes": []}

    @router.get("/v1/nodes/{eui64}")
    def nodes_get(eui64: str) -> dict[str, object]:
        try:
            return nodes_mod.get_node_summary(eui64, include_signal_strength=True)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @router.post("/v1/nodes/{eui64}/friendly-name")
    def nodes_set_name(eui64: str, payload: dict[str, str]) -> dict[str, object]:
        name = (payload or {}).get("name", "").strip()
        if not name:
            return {"error": "name required"}
        try:
            ok = get_store_fn().set_node_friendly_name(eui64, name)
            if not ok:
                return {"error": f"node {eui64} not found"}
            return nodes_mod.get_node_summary(eui64, include_signal_strength=True)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    return router

