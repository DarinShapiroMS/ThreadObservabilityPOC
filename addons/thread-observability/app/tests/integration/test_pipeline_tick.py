"""Integration tests for the pipeline runner.

These tests exercise the orchestration layer with each I/O-bound stage
replaced by a deterministic stub. Per-stage exceptions must be
isolated: a failure in one stage must not prevent the others from
running. The runner state must accurately reflect what happened.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from thread_observability.pipeline import (
    device_discovery,
    otbr_adapter,
    otbr_rest,
    reasoner as reasoner_mod,
    runner as pipeline_runner,
)


STAGE_NAMES = ("otbr_log_ingest", "otbr_rest", "matter_discovery", "reasoner")


@pytest.fixture()
def stubbed_stages(monkeypatch):
    """Replace every I/O-touching stage with an in-memory stub.

    Returns a dict the test can mutate to control each stage's outcome
    before calling :func:`pipeline_runner.run_tick`. Each entry is either:

    * ``("ok", summary_dict)`` — the stage returns the summary.
    * ``("raise", exception_instance)`` — the stage raises.

    Default: every stage returns ``{"stub": True}``.
    """
    controls: dict[str, tuple[str, Any]] = {
        name: ("ok", {"stub": True, "stage": name}) for name in STAGE_NAMES
    }

    def _make_stub(name: str):
        async def _stub() -> dict[str, Any]:
            mode, payload = controls[name]
            if mode == "raise":
                raise payload
            return payload
        return _stub

    monkeypatch.setattr(otbr_adapter, "ingest_once", _make_stub("otbr_log_ingest"))
    monkeypatch.setattr(otbr_rest, "ingest_once", _make_stub("otbr_rest"))
    monkeypatch.setattr(device_discovery, "discover_and_sync", _make_stub("matter_discovery"))

    # The reasoner is sync; the runner wraps it with ``asyncio.to_thread``.
    def _sync_stub() -> dict[str, Any]:
        mode, payload = controls["reasoner"]
        if mode == "raise":
            raise payload
        return payload
    monkeypatch.setattr(reasoner_mod, "run_reasoner", _sync_stub)

    # Reset the runner's module-level state so tick counts start fresh.
    pipeline_runner._last_tick.update(  # noqa: SLF001 - test reset
        running=False,
        started_at=None,
        finished_at=None,
        duration_seconds=None,
        stages={},
        error=None,
        tick_count=0,
        next_tick_after=None,
        interval_seconds=None,
    )
    return controls


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_pipeline_happy_path(stubbed_stages) -> None:
    """Every stage succeeds → state has all four stages with ok=True."""
    state = _run(pipeline_runner.run_tick())
    assert state["tick_count"] == 1
    assert state["error"] is None
    assert state["running"] is False
    stages = state["stages"]
    for name in STAGE_NAMES:
        assert stages[name]["ok"] is True, f"{name} should have succeeded"
        assert stages[name]["error"] is None
        assert isinstance(stages[name]["duration_seconds"], float)


def test_pipeline_stage_isolation_matter_fails(stubbed_stages) -> None:
    """Matter discovery raises — downstream reasoner still runs.

    This is the core resilience property: a flaky Matter server must not
    silently stop the reasoner (or the OTBR ingest from completing).
    """
    stubbed_stages["matter_discovery"] = ("raise", RuntimeError("ws timeout"))
    state = _run(pipeline_runner.run_tick())
    stages = state["stages"]
    assert stages["otbr_log_ingest"]["ok"] is True
    assert stages["otbr_rest"]["ok"] is True
    assert stages["matter_discovery"]["ok"] is False
    assert "ws timeout" in stages["matter_discovery"]["error"]
    # Reasoner still ran despite the failure above it.
    assert stages["reasoner"]["ok"] is True
    # Overall error string flags the failed stage.
    assert state["error"] is not None
    assert "matter_discovery" in state["error"]


def test_pipeline_stage_isolation_first_fails(stubbed_stages) -> None:
    """OTBR log ingest raises — the rest of the pipeline still runs."""
    stubbed_stages["otbr_log_ingest"] = ("raise", ValueError("malformed log"))
    state = _run(pipeline_runner.run_tick())
    stages = state["stages"]
    assert stages["otbr_log_ingest"]["ok"] is False
    for name in ("otbr_rest", "matter_discovery", "reasoner"):
        assert stages[name]["ok"] is True, f"{name} should not be blocked"


def test_pipeline_multiple_failures_reported(stubbed_stages) -> None:
    """Multiple failing stages must all appear in the error string."""
    stubbed_stages["otbr_rest"] = ("raise", RuntimeError("rest down"))
    stubbed_stages["reasoner"] = ("raise", RuntimeError("db locked"))
    state = _run(pipeline_runner.run_tick())
    assert state["error"] is not None
    assert "otbr_rest" in state["error"]
    assert "reasoner" in state["error"]


def test_pipeline_tick_count_increments(stubbed_stages) -> None:
    """Each ``run_tick`` call bumps ``tick_count``."""
    _run(pipeline_runner.run_tick())
    _run(pipeline_runner.run_tick())
    state = _run(pipeline_runner.run_tick())
    assert state["tick_count"] == 3


def test_pipeline_state_running_flag_resets(stubbed_stages) -> None:
    """Even on stage failures, ``running`` must drop back to False."""
    stubbed_stages["matter_discovery"] = ("raise", RuntimeError("boom"))
    _run(pipeline_runner.run_tick())
    state = pipeline_runner.get_runner_state()
    assert state["running"] is False
    assert state["current_stage"] is None


def test_pipeline_get_runner_state_returns_copy(stubbed_stages) -> None:
    """Mutating the returned dict must not corrupt internal state."""
    _run(pipeline_runner.run_tick())
    snap = pipeline_runner.get_runner_state()
    snap["tick_count"] = 999
    snap["stages"] = {}
    # Re-read — internal state untouched.
    again = pipeline_runner.get_runner_state()
    assert again["tick_count"] == 1
    assert set(again["stages"].keys()) == set(STAGE_NAMES)
