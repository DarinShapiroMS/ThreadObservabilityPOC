"""Unified pipeline runner.

A single atomic tick that walks every data-collection stage in dependency
order and finally invokes the reasoner against the freshly-written data.

Stages, in order:

1. ``otbr_log_ingest``    — pull new lines from the OTBR add-on log, parse to
   events, persist. Cheap, runs every tick.
2. ``otbr_rest``          — fetch ``/node`` from the OTBR REST API, upsert the
   border router as a node (gives us the OTBR even before Matter responds).
3. ``matter_discovery``   — full WebSocket walk of the Matter server: nodes,
   neighbor/route tables, diagnostics, link replacement, status recompute,
   purge of expired non-HA-registered nodes.
4. ``reasoner``           — issue detection over the now-fresh DB.

Each stage runs inside its own ``try/except`` so a transient failure in one
does not block the rest of the tick. The tick is scheduled at boot
(immediate first run) and then every ``interval_seconds`` after the previous
tick completes — so we never overlap ticks, and the interval is "rest time
between runs," not "wall-clock cadence."

There are no other background loops. Everything the dashboard shows comes
from this single source.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from . import device_discovery
from . import observer_events
from . import otbr_adapter
from . import otbr_rest
from . import reasoner as reasoner_mod
from . import topology_snapshot

log = logging.getLogger(__name__)

# Module-level state so the API can report what the runner last did.
_last_tick: dict[str, Any] = {
    "running": False,
    "current_stage": None,
    "started_at": None,
    "finished_at": None,
    "duration_seconds": None,
    "stages": {},
    "error": None,
    "tick_count": 0,
    "next_tick_after": None,
    "interval_seconds": None,
}


def get_runner_state() -> dict[str, Any]:
    """Snapshot of the last tick — exposed via /v1/dev/* for diagnostics."""
    return dict(_last_tick)


async def _run_stage(name: str, coro_factory) -> dict[str, Any]:
    """Invoke a stage, catching exceptions and timing the run."""
    _last_tick["current_stage"] = name
    t0 = time.monotonic()
    out: dict[str, Any] = {"ok": True, "duration_seconds": 0.0, "summary": None, "error": None}
    try:
        result = await coro_factory()
        out["summary"] = result if isinstance(result, dict) else {"result": result}
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.exception("pipeline stage %s failed", name)
        out["ok"] = False
        out["error"] = f"{type(exc).__name__}: {exc}"
    out["duration_seconds"] = round(time.monotonic() - t0, 3)
    return out


async def run_tick() -> dict[str, Any]:
    """Execute one atomic pipeline tick. Returns the summary it just wrote
    to module state.

    Safe to call manually (e.g. from an API endpoint that wants to force a
    refresh). The background loop also calls this — there is only one
    implementation of "do a full data refresh."
    """
    t0 = time.monotonic()
    _last_tick["running"] = True
    _last_tick["started_at"] = time.time()
    _last_tick["finished_at"] = None
    _last_tick["error"] = None
    _last_tick["current_stage"] = None
    stages: dict[str, dict[str, Any]] = {}
    _last_tick["stages"] = stages

    # 1) OTBR log ingest. Cheap; ensures any new partition/role change events
    # in the OTBR add-on log are persisted before discovery reads them.
    stages["otbr_log_ingest"] = await _run_stage(
        "otbr_log_ingest", otbr_adapter.ingest_once
    )

    # 1b) Observer events. Polls Supervisor for restart / outage windows of
    # the upstream add-ons we depend on (OTBR, Matter Server). Runs before
    # discovery + reasoner so any observer event we record this tick is
    # visible to the reasoner's suppression-window check at the end of
    # the same tick.
    from ..storage.sqlite_store import get_store as _get_store

    stages["observer_events"] = await _run_stage(
        "observer_events",
        lambda: observer_events.poll_supervisor_addons(_get_store()),
    )

    # 2) OTBR REST. Upserts the border router as a node, gives us router_id
    # / partition / active routers without waiting for Matter.
    stages["otbr_rest"] = await _run_stage(
        "otbr_rest", otbr_rest.ingest_once
    )

    # 3) Matter discovery. The heavy stage — walks every node over the WS
    # API, persists diagnostics + neighbor/route tables, recomputes node
    # status, purges expired non-HA-registered rows.
    stages["matter_discovery"] = await _run_stage(
        "matter_discovery", device_discovery.discover_and_sync
    )

    # 3b) Topology snapshot (Tier 4). Captures the live topology graph
    # into the ``topology_snapshots`` table whenever it differs from the
    # last write (or at heartbeat). Sync; runs in a thread.
    stages["topology_snapshot"] = await _run_stage(
        "topology_snapshot",
        lambda: asyncio.to_thread(topology_snapshot.capture_snapshot),
    )

    # 4) Reasoner. Runs against the freshly-written DB so issue detection
    # never lags behind discovery. Sync function → run in a thread so the
    # event loop stays responsive for the API.
    stages["reasoner"] = await _run_stage(
        "reasoner", lambda: asyncio.to_thread(reasoner_mod.run_reasoner)
    )

    duration = round(time.monotonic() - t0, 3)
    _last_tick["finished_at"] = time.time()
    _last_tick["duration_seconds"] = duration
    _last_tick["stages"] = stages
    _last_tick["tick_count"] = int(_last_tick.get("tick_count") or 0) + 1
    _last_tick["current_stage"] = None
    _last_tick["running"] = False
    failed = [n for n, s in stages.items() if not s["ok"]]
    if failed:
        _last_tick["error"] = f"stages failed: {','.join(failed)}"
    log.info(
        "pipeline tick #%d done in %.2fs (%s)",
        _last_tick["tick_count"],
        duration,
        ", ".join(f"{n}={s['duration_seconds']}s" for n, s in stages.items()),
    )
    # Persist the tick for the temporal-honesty envelope (Phase 1).
    # Best-effort: failures here must never propagate or break a tick.
    try:
        from ..storage.sqlite_store import get_store as _get_store2

        _get_store2().record_pipeline_tick(get_runner_state())
    except Exception:  # noqa: BLE001
        log.exception("pipeline tick persistence failed (non-fatal)")
    return get_runner_state()


async def run_forever(interval_seconds: int = 30) -> None:
    """Run the pipeline immediately, then every ``interval_seconds`` after
    the previous tick completes. Cancellation propagates cleanly.

    The interval is rest-time between ticks, not wall-clock cadence — if a
    tick takes 8s, the next tick fires 30s later, total cycle 38s. This
    avoids overlapping ticks if the Matter server is slow.
    """
    log.info("pipeline runner starting (interval=%ss, immediate first tick)", interval_seconds)
    _last_tick["interval_seconds"] = interval_seconds
    while True:
        try:
            await run_tick()
        except asyncio.CancelledError:
            log.info("pipeline runner cancelled")
            raise
        except Exception:  # noqa: BLE001
            # run_tick already catches per-stage failures; this is the
            # belt-and-suspenders catch for runner-level bugs.
            log.exception("pipeline runner: unhandled error in tick")
        _last_tick["next_tick_after"] = time.time() + interval_seconds
        try:
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            raise
