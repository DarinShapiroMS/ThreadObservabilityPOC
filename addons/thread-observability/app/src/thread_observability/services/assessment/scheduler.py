"""Adaptive scheduler for Background Diagnostics (#18).

State machine
-------------

* ``probation``  — short-interval checks for a few iterations right after
  enable / install / restart, to learn the network's baseline.
* ``relaxing``   — clean network: interval grows exponentially toward
  ``relaxing_max_hours`` while verdicts stay ``ok``.
* ``steady``     — at the max relaxed interval; one cheap check per
  ``relaxing_max_hours``.
* ``heightened`` — a recent verdict was ``investigate`` or ``watch``;
  poll more often, decaying toward ``heightened_max_hours``.
* ``engaged``    — a user is actively chatting / triaging; poll fast for
  a window then decay back.
* ``disabled``   — user turned the feature off (or never opted in).

All state lives in the ``assessment_schedule`` SQLite row so a stable
network does not reset to probation on every addon update.

Budget
------

``daily_budget_calls`` caps how many assessments may run in the same
UTC day. ``budget_calls_used`` resets at UTC midnight (rollover handled
lazily on the next decision). Force-now requests respect the budget;
on budget-exhausted, the scheduler returns a ``budget`` decision so
the UI can render a graceful "budget reached; resumes at <time>" hint.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from ...storage.sqlite_store import SQLiteStore, get_store
from ...utils.datetime import parse_iso_datetime, to_iso_utc, utc_now


SchedulerState = Literal[
    "probation",
    "relaxing",
    "steady",
    "heightened",
    "engaged",
    "disabled",
]

Verdict = Literal["ok", "watch", "investigate"]


@dataclass(slots=True)
class ScheduleConfig:
    """Knobs from ``assessment:`` in the addon options.

    Defaults match documentation/07-agentic-ai-sprint.md §11.1.
    """

    enabled: bool = False
    probation_interval_minutes: int = 15
    probation_checks: int = 3
    relaxing_initial_hours: int = 1
    relaxing_max_hours: int = 24
    heightened_initial_minutes: int = 30
    heightened_max_hours: int = 6
    engaged_interval_minutes: int = 5
    engaged_decay_minutes: int = 60
    daily_budget_calls: int = 12

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "ScheduleConfig":
        if not data:
            return cls()
        known = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**known)


@dataclass(slots=True)
class ScheduleSnapshot:
    """Current scheduler state surfaced to the UI / MCP."""

    state: SchedulerState
    state_since: str
    last_assessment_at: str | None
    next_assessment_at: str | None
    current_interval_seconds: int
    consecutive_ok: int
    consecutive_concern: int
    budget_calls_used: int
    budget_window_start_at: str
    daily_budget_calls: int
    reason: str | None
    enabled: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "state_since": self.state_since,
            "last_assessment_at": self.last_assessment_at,
            "next_assessment_at": self.next_assessment_at,
            "current_interval_seconds": self.current_interval_seconds,
            "consecutive_ok": self.consecutive_ok,
            "consecutive_concern": self.consecutive_concern,
            "budget": {
                "used": self.budget_calls_used,
                "limit": self.daily_budget_calls,
                "window_start_at": self.budget_window_start_at,
                "remaining": max(0, self.daily_budget_calls - self.budget_calls_used),
            },
            "reason": self.reason,
            "enabled": self.enabled,
        }


@dataclass(slots=True)
class SchedulerDecision:
    """Result of asking the scheduler whether to run now."""

    should_run: bool
    reason: str
    next_run_at: str | None = None
    state: SchedulerState = "probation"
    budget_exhausted: bool = False


def _utc_now() -> datetime:
    return utc_now()


def _iso(dt: datetime) -> str:
    return to_iso_utc(dt)


def _parse(ts: str | None) -> datetime | None:
    return parse_iso_datetime(ts)


class AssessmentScheduler:
    """Pure logic + persistence wrapper. No I/O beyond the store."""

    def __init__(
        self,
        *,
        config: ScheduleConfig | None = None,
        store: SQLiteStore | None = None,
    ) -> None:
        self.config = config or ScheduleConfig()
        self._store = store or get_store()

    # ----- public API -------------------------------------------------

    def snapshot(self, *, now: datetime | None = None) -> ScheduleSnapshot:
        """Return the current scheduler state (initializing if first call)."""
        row = self._store.get_assessment_schedule()
        if row is None:
            row = self._initialize(now=now)
        self._roll_budget_if_needed(row, now=now or _utc_now())
        return self._row_to_snapshot(row)

    def decide(
        self,
        *,
        now: datetime | None = None,
        force: bool = False,
    ) -> SchedulerDecision:
        """Decide whether to run an assessment right now.

        ``force=True`` honors the daily budget but ignores cadence. If the
        feature is disabled, returns ``should_run=False`` regardless.
        """
        now = now or _utc_now()
        row = self._store.get_assessment_schedule() or self._initialize(now=now)
        self._roll_budget_if_needed(row, now=now)

        if not self.config.enabled or row.get("state") == "disabled":
            return SchedulerDecision(
                should_run=False,
                reason="disabled",
                state=row.get("state", "disabled"),
                next_run_at=row.get("next_assessment_at"),
            )

        budget_used = int(row.get("budget_calls_used") or 0)
        budget_limit = self.config.daily_budget_calls
        if budget_used >= budget_limit:
            return SchedulerDecision(
                should_run=False,
                reason="daily_budget_exhausted",
                state=row.get("state"),
                next_run_at=row.get("next_assessment_at"),
                budget_exhausted=True,
            )

        if force:
            return SchedulerDecision(
                should_run=True,
                reason="forced",
                state=row.get("state"),
                next_run_at=row.get("next_assessment_at"),
            )

        next_at = _parse(row.get("next_assessment_at"))
        if next_at is None or now >= next_at:
            return SchedulerDecision(
                should_run=True,
                reason="cadence_due",
                state=row.get("state"),
                next_run_at=row.get("next_assessment_at"),
            )
        return SchedulerDecision(
            should_run=False,
            reason="cadence_not_due",
            state=row.get("state"),
            next_run_at=row.get("next_assessment_at"),
        )

    def record_assessment(
        self,
        *,
        verdict: Verdict,
        now: datetime | None = None,
    ) -> ScheduleSnapshot:
        """Update the state machine after an assessment ran."""
        now = now or _utc_now()
        row = self._store.get_assessment_schedule() or self._initialize(now=now)
        self._roll_budget_if_needed(row, now=now)

        state: SchedulerState = row.get("state", "probation")
        consecutive_ok = int(row.get("consecutive_ok") or 0)
        consecutive_concern = int(row.get("consecutive_concern") or 0)
        current_interval = int(row.get("current_interval_seconds") or 900)
        reason: str | None = None

        if verdict == "ok":
            consecutive_concern = 0
            consecutive_ok += 1
            state, current_interval, reason = self._on_ok(
                state, current_interval, consecutive_ok
            )
        else:
            # 'watch' or 'investigate' both indicate concern; investigate
            # ramps harder.
            consecutive_ok = 0
            consecutive_concern += 1
            state, current_interval, reason = self._on_concern(
                state, current_interval, verdict
            )

        next_assessment_at = now + timedelta(seconds=current_interval)
        budget_used = int(row.get("budget_calls_used") or 0) + 1

        state_since = row.get("state_since") or _iso(now)
        if state != row.get("state"):
            state_since = _iso(now)

        updated = self._store.upsert_assessment_schedule(
            {
                "state": state,
                "state_since": state_since,
                "last_assessment_at": _iso(now),
                "next_assessment_at": _iso(next_assessment_at),
                "consecutive_ok": consecutive_ok,
                "consecutive_concern": consecutive_concern,
                "current_interval_seconds": current_interval,
                "budget_calls_used": budget_used,
                "budget_window_start_at": row.get("budget_window_start_at"),
                "reason": reason,
            }
        )
        return self._row_to_snapshot(updated)

    def note_user_engaged(self, *, now: datetime | None = None) -> ScheduleSnapshot:
        """User opened the chat drawer / started triage — bump to engaged."""
        now = now or _utc_now()
        row = self._store.get_assessment_schedule() or self._initialize(now=now)
        interval = self.config.engaged_interval_minutes * 60
        updated = self._store.upsert_assessment_schedule(
            {
                "state": "engaged",
                "state_since": _iso(now),
                "current_interval_seconds": interval,
                "next_assessment_at": _iso(now + timedelta(seconds=interval)),
                "reason": "user_engaged",
                "budget_calls_used": row.get("budget_calls_used", 0),
                "budget_window_start_at": row.get("budget_window_start_at"),
            }
        )
        return self._row_to_snapshot(updated)

    def set_enabled(self, enabled: bool, *, now: datetime | None = None) -> ScheduleSnapshot:
        """Runtime enable/disable (e.g., from the switch entity)."""
        self.config = replace(self.config, enabled=enabled)
        now = now or _utc_now()
        row = self._store.get_assessment_schedule() or self._initialize(now=now)
        if enabled:
            interval = self.config.probation_interval_minutes * 60
            updated = self._store.upsert_assessment_schedule(
                {
                    "state": "probation",
                    "state_since": _iso(now),
                    "current_interval_seconds": interval,
                    "next_assessment_at": _iso(now + timedelta(seconds=interval)),
                    "consecutive_ok": 0,
                    "consecutive_concern": 0,
                    "reason": "enabled_by_user",
                }
            )
        else:
            updated = self._store.upsert_assessment_schedule(
                {
                    "state": "disabled",
                    "state_since": _iso(now),
                    "next_assessment_at": None,
                    "reason": "disabled_by_user",
                }
            )
        return self._row_to_snapshot(updated)

    # ----- internals --------------------------------------------------

    def _initialize(self, *, now: datetime | None = None) -> dict[str, Any]:
        now = now or _utc_now()
        interval = self.config.probation_interval_minutes * 60
        state: SchedulerState = "probation" if self.config.enabled else "disabled"
        return self._store.upsert_assessment_schedule(
            {
                "state": state,
                "state_since": _iso(now),
                "current_interval_seconds": interval,
                "next_assessment_at": _iso(now + timedelta(seconds=interval))
                if self.config.enabled
                else None,
                "consecutive_ok": 0,
                "consecutive_concern": 0,
                "budget_calls_used": 0,
                "budget_window_start_at": _iso(now),
                "reason": "initial",
            }
        )

    def _roll_budget_if_needed(self, row: dict[str, Any], *, now: datetime) -> None:
        """Reset budget at UTC-midnight rollover."""
        window_start = _parse(row.get("budget_window_start_at")) or now
        if window_start.date() != now.date():
            self._store.upsert_assessment_schedule(
                {
                    "budget_calls_used": 0,
                    "budget_window_start_at": _iso(
                        now.replace(hour=0, minute=0, second=0, microsecond=0)
                    ),
                }
            )
            row["budget_calls_used"] = 0
            row["budget_window_start_at"] = _iso(
                now.replace(hour=0, minute=0, second=0, microsecond=0)
            )

    def _on_ok(
        self,
        state: SchedulerState,
        current_interval: int,
        consecutive_ok: int,
    ) -> tuple[SchedulerState, int, str | None]:
        if state == "probation":
            if consecutive_ok >= self.config.probation_checks:
                interval = self.config.relaxing_initial_hours * 3600
                return "relaxing", interval, "probation_clean"
            interval = self.config.probation_interval_minutes * 60
            return "probation", interval, None

        if state in ("relaxing", "steady", "heightened", "engaged"):
            relaxing_max = self.config.relaxing_max_hours * 3600
            # exponential decay back to steady cadence
            interval = min(current_interval * 2, relaxing_max)
            if interval >= relaxing_max:
                return "steady", relaxing_max, "settled"
            return "relaxing", interval, "decaying"

        # disabled / unknown
        return state, current_interval, None

    def _on_concern(
        self,
        state: SchedulerState,
        current_interval: int,
        verdict: Verdict,
    ) -> tuple[SchedulerState, int, str | None]:
        if state == "engaged":
            interval = self.config.engaged_interval_minutes * 60
            return "engaged", interval, f"user_engaged_{verdict}"

        # Drop to heightened cadence; investigate ramps to the floor faster.
        interval = self.config.heightened_initial_minutes * 60
        if verdict == "investigate":
            interval = max(60, interval // 2)
        return "heightened", interval, f"verdict_{verdict}"

    def _row_to_snapshot(self, row: dict[str, Any]) -> ScheduleSnapshot:
        return ScheduleSnapshot(
            state=row.get("state", "probation"),
            state_since=row.get("state_since") or _iso(_utc_now()),
            last_assessment_at=row.get("last_assessment_at"),
            next_assessment_at=row.get("next_assessment_at"),
            current_interval_seconds=int(row.get("current_interval_seconds") or 900),
            consecutive_ok=int(row.get("consecutive_ok") or 0),
            consecutive_concern=int(row.get("consecutive_concern") or 0),
            budget_calls_used=int(row.get("budget_calls_used") or 0),
            budget_window_start_at=row.get("budget_window_start_at")
            or _iso(_utc_now()),
            daily_budget_calls=self.config.daily_budget_calls,
            reason=row.get("reason"),
            enabled=self.config.enabled and row.get("state") != "disabled",
        )
