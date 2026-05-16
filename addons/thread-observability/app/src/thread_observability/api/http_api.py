"""Core HTTP API for Thread Mesh Detective add-on.

This module is intentionally kept small: it owns the FastAPI app factory and
wires together route modules and background orchestration.
"""

from __future__ import annotations

import functools

from fastapi import FastAPI

from .app_meta import ADDON_VERSION, DASHBOARD_HTML, tail_log, utc_now
from .lifespan import lifespan as lifespan_cm
from .routes.assessment import create_assessment_router
from .routes.chat import create_chat_router
from .routes.dev_pipeline import create_dev_pipeline_router
from .routes.ingest import create_ingest_router
from .routes.mesh import create_mesh_router
from .routes.nodes import create_nodes_router
from .routes.root import create_root_router
from ..config import get_config
from ..storage.sqlite_store import get_store


def _get_config_dynamic():  # noqa: ANN001
    return get_config()


def _get_store_dynamic():  # noqa: ANN001
    return get_store()


def create_core_app() -> FastAPI:
    """Create the core FastAPI application."""
    app = FastAPI(
        title="Thread Mesh Detective Core API",
        version=ADDON_VERSION,
        lifespan=functools.partial(
            lifespan_cm,
            get_config_fn=_get_config_dynamic,
            get_store_fn=_get_store_dynamic,
            addon_version=ADDON_VERSION,
        ),
    )

    app.include_router(
        create_root_router(
            dashboard_html=DASHBOARD_HTML,
            addon_version=ADDON_VERSION,
            utc_now=utc_now,
        )
    )
    app.include_router(
        create_chat_router(
            get_config_fn=_get_config_dynamic,
            get_store_fn=_get_store_dynamic,
            utc_now=utc_now,
        )
    )
    app.include_router(create_mesh_router(get_store_fn=_get_store_dynamic, utc_now=utc_now))
    app.include_router(
        create_dev_pipeline_router(
            addon_version=ADDON_VERSION,
            get_config_fn=_get_config_dynamic,
            get_store_fn=_get_store_dynamic,
            utc_now=utc_now,
            tail_log=tail_log,
        )
    )
    app.include_router(create_ingest_router())
    app.include_router(create_nodes_router(get_store_fn=_get_store_dynamic))
    app.include_router(create_assessment_router(get_config_fn=_get_config_dynamic, get_store_fn=_get_store_dynamic))

    return app
