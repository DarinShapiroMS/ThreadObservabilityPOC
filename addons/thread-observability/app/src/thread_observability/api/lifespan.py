"""FastAPI lifespan and background orchestration helpers."""

from __future__ import annotations

import asyncio
import contextlib
import logging

from fastapi import FastAPI

from .app_meta import ADDON_VERSION
from ..config import get_config
from ..pipeline import runner as pipeline_runner
from ..storage.sqlite_store import get_store

log = logging.getLogger(__name__)


async def periodic(name: str, interval: int, coro_factory) -> None:
    """Deprecated. Kept only because downstream callers may import it."""
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
async def lifespan(
    app: FastAPI,
    *,
    get_config_fn=get_config,
    get_store_fn=get_store,
    addon_version: str = ADDON_VERSION,
):
    """Start/stop the background pipeline alongside the FastAPI app."""
    cfg = get_config_fn()
    pipeline_interval = int(getattr(cfg.scheduler, "pipeline_interval_seconds", 30))

    if getattr(cfg, "reset_db_on_start", True):
        try:
            deleted = get_store_fn().reset_data()
            log.info("reset_db_on_start: wiped %d rows from cache tables", deleted)
        except Exception:  # noqa: BLE001
            log.exception("reset_db_on_start: failed to truncate cache tables")
    else:
        log.info("reset_db_on_start=false: preserving previous DB contents")

    try:
        from ..pipeline.observer_events import record_self_start  # local import

        record_self_start(get_store_fn(), version=addon_version)
    except Exception:  # noqa: BLE001
        log.exception("observer_events: failed to record self-start")

    tasks = [
        asyncio.create_task(
            pipeline_runner.run_forever(interval_seconds=pipeline_interval),
            name="pipeline-runner",
        ),
    ]
    log.info(
        "pipeline scheduler started: interval=%ss (single atomic tick)", pipeline_interval
    )
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        log.info("pipeline scheduler stopped")
