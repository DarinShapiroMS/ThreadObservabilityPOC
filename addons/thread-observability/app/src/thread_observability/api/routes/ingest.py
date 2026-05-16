from __future__ import annotations

from fastapi import APIRouter

from .. import supervisor_client
from ...pipeline import otbr_adapter


def create_ingest_router() -> APIRouter:
    router = APIRouter()

    @router.get("/v1/ingest/state")
    def ingest_state() -> dict[str, object]:
        try:
            return otbr_adapter.get_state()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @router.get("/v1/ingest/candidates")
    async def ingest_candidates() -> dict[str, object]:
        try:
            cands = await otbr_adapter.list_candidates()
            return {"count": len(cands), "candidates": cands}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "candidates": []}

    @router.post("/v1/ingest/run")
    async def ingest_run() -> dict[str, object]:
        try:
            return await otbr_adapter.ingest_once()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @router.post("/v1/ingest/slug")
    async def ingest_set_slug(payload: dict[str, str]) -> dict[str, object]:
        slug = (payload or {}).get("slug", "").strip()
        if not slug:
            return {"error": "slug required"}
        try:
            return otbr_adapter.set_slug(slug)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @router.get("/v1/ingest/debug")
    async def ingest_debug() -> dict[str, object]:
        try:
            ingest_st = otbr_adapter.get_state()
            slug = ingest_st.get("slug")
            if not slug:
                return {"error": "no OTBR slug configured"}
            logs = await supervisor_client.get_addon_logs(slug=slug, lines=50)
            return {
                "slug": slug,
                "log_line_count": len(logs),
                "sample_lines": logs[-10:] if logs else [],
                "raw_sample": "\n".join(logs[-20:]) if logs else "",
            }
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    return router

