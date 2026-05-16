from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse


def create_root_router(
    *,
    dashboard_html: str,
    addon_version: str,
    utc_now: callable,
) -> APIRouter:
    router = APIRouter()

    @router.get("/", response_class=HTMLResponse)
    def dashboard() -> HTMLResponse:
        return HTMLResponse(dashboard_html)

    @router.get("/api")
    def api_root() -> dict[str, str]:
        return {"service": "core", "name": "thread-observability", "version": addon_version}

    @router.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "core", "checked_at": utc_now()}

    return router

