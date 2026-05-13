"""Tests for the adaptive scheduler state machine (#18)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from thread_observability.services.assessment.scheduler import (
    AssessmentScheduler,
    ScheduleConfig,
)
from thread_observability.storage.sqlite_store import SQLiteStore


def _now() -> datetime:
    return datetime(2026, 5, 12, 10, 0, 0, tzinfo=UTC)


def _enabled_cfg(**overrides) -> ScheduleConfig:
    cfg = ScheduleConfig(enabled=True)
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def test_initial_snapshot_creates_row(store: SQLiteStore) -> None:
    sched = AssessmentScheduler(config=_enabled_cfg(), store=store)
    snap = sched.snapshot(now=_now())
    assert snap.state == "probation"
    assert snap.enabled is True
    assert snap.next_assessment_at is not None


def test_disabled_decide_returns_disabled(store: SQLiteStore) -> None:
    sched = AssessmentScheduler(config=ScheduleConfig(enabled=False), store=store)
    decision = sched.decide(now=_now())
    assert decision.should_run is False
    assert decision.reason == "disabled"


def test_decide_runs_when_due(store: SQLiteStore) -> None:
    sched = AssessmentScheduler(config=_enabled_cfg(), store=store)
    sched.snapshot(now=_now())  # initialize
    # advance past the probation interval
    later = _now() + timedelta(minutes=20)
    decision = sched.decide(now=later)
    assert decision.should_run is True
    assert decision.reason == "cadence_due"


def test_decide_blocks_when_not_due(store: SQLiteStore) -> None:
    sched = AssessmentScheduler(config=_enabled_cfg(), store=store)
    sched.snapshot(now=_now())
    decision = sched.decide(now=_now() + timedelta(minutes=1))
    assert decision.should_run is False
    assert decision.reason == "cadence_not_due"


def test_force_bypasses_cadence(store: SQLiteStore) -> None:
    sched = AssessmentScheduler(config=_enabled_cfg(), store=store)
    sched.snapshot(now=_now())
    decision = sched.decide(now=_now() + timedelta(minutes=1), force=True)
    assert decision.should_run is True
    assert decision.reason == "forced"


def test_budget_exhaustion_blocks_force(store: SQLiteStore) -> None:
    sched = AssessmentScheduler(
        config=_enabled_cfg(daily_budget_calls=2),
        store=store,
    )
    sched.snapshot(now=_now())
    # Burn 2 calls
    sched.record_assessment(verdict="ok", now=_now())
    sched.record_assessment(verdict="ok", now=_now())
    decision = sched.decide(now=_now() + timedelta(hours=2), force=True)
    assert decision.should_run is False
    assert decision.budget_exhausted is True
    assert decision.reason == "daily_budget_exhausted"


def test_probation_to_relaxing_transition(store: SQLiteStore) -> None:
    sched = AssessmentScheduler(
        config=_enabled_cfg(probation_checks=2),
        store=store,
    )
    sched.snapshot(now=_now())
    sched.record_assessment(verdict="ok", now=_now())
    snap = sched.record_assessment(verdict="ok", now=_now())
    assert snap.state == "relaxing"
    assert snap.reason == "probation_clean"


def test_relaxing_exponential_to_steady(store: SQLiteStore) -> None:
    cfg = _enabled_cfg(
        probation_checks=1,
        relaxing_initial_hours=1,
        relaxing_max_hours=4,
    )
    sched = AssessmentScheduler(config=cfg, store=store)
    sched.snapshot(now=_now())
    sched.record_assessment(verdict="ok", now=_now())  # probation -> relaxing (1h)
    snap = sched.record_assessment(verdict="ok", now=_now())  # 2h
    assert snap.state == "relaxing"
    snap = sched.record_assessment(verdict="ok", now=_now())  # 4h => steady
    assert snap.state == "steady"


def test_concern_drops_to_heightened(store: SQLiteStore) -> None:
    sched = AssessmentScheduler(config=_enabled_cfg(), store=store)
    sched.snapshot(now=_now())
    sched.record_assessment(verdict="ok", now=_now())
    sched.record_assessment(verdict="ok", now=_now())
    sched.record_assessment(verdict="ok", now=_now())
    snap = sched.record_assessment(verdict="investigate", now=_now())
    assert snap.state == "heightened"
    assert snap.consecutive_concern == 1


def test_user_engaged_state(store: SQLiteStore) -> None:
    sched = AssessmentScheduler(config=_enabled_cfg(), store=store)
    sched.snapshot(now=_now())
    snap = sched.note_user_engaged(now=_now())
    assert snap.state == "engaged"
    assert snap.current_interval_seconds == 5 * 60


def test_set_enabled_flips_state(store: SQLiteStore) -> None:
    sched = AssessmentScheduler(config=ScheduleConfig(enabled=False), store=store)
    snap = sched.set_enabled(True, now=_now())
    assert snap.state == "probation"
    assert snap.enabled is True
    snap = sched.set_enabled(False, now=_now())
    assert snap.state == "disabled"
    assert snap.enabled is False


def test_budget_rolls_over_at_utc_midnight(store: SQLiteStore) -> None:
    sched = AssessmentScheduler(config=_enabled_cfg(daily_budget_calls=2), store=store)
    sched.snapshot(now=_now())
    sched.record_assessment(verdict="ok", now=_now())
    sched.record_assessment(verdict="ok", now=_now())
    # next day
    tomorrow = _now() + timedelta(days=1)
    snap = sched.snapshot(now=tomorrow)
    assert snap.budget_calls_used == 0
