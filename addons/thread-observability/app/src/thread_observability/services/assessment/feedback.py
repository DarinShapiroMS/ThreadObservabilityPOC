"""Feedback / outcome capture for assessment findings (#22)."""

from __future__ import annotations

from typing import Any

from ...storage.sqlite_store import SQLiteStore, get_store

VALID_OUTCOMES = {
    "resolved",
    "wrong",
    "ignored_dismissed",
    "ignored_expired",
}


def mark_outcome(
    *,
    finding_id: str,
    outcome: str,
    notes: str | None = None,
    store: SQLiteStore | None = None,
) -> dict[str, Any]:
    """Record an outcome for a finding.

    Side-effects beyond the feedback row:

    * ``resolved`` → mark the finding cleared with cleared_by=user_resolve.
    * ``wrong``    → mark cleared with cleared_by=user_wrong.
    * ``ignored_dismissed`` → caller already invoked dismiss; we just record.
    * ``ignored_expired``   → engine-side; finding already auto-cleared.
    """
    if outcome not in VALID_OUTCOMES:
        raise ValueError(f"outcome must be one of {sorted(VALID_OUTCOMES)}")
    s = store or get_store()
    finding = s.get_assessment_finding(finding_id)
    if not finding:
        raise LookupError(f"unknown finding_id: {finding_id}")

    finding_type = finding.get("finding_type")

    if outcome == "resolved":
        s.clear_assessment_findings_by_key(
            finding.get("finding_key"), cleared_by="user_resolve"
        )
    elif outcome == "wrong":
        s.clear_assessment_findings_by_key(
            finding.get("finding_key"), cleared_by="user_wrong"
        )

    rec = s.record_assessment_feedback(
        finding_id=finding_id,
        outcome=outcome,
        finding_type=finding_type,
        notes=notes,
    )
    return rec


def quality_summary(
    *,
    since: str | None = None,
    store: SQLiteStore | None = None,
) -> dict[str, Any]:
    """Return precision / noisy-type metrics over the window."""
    s = store or get_store()
    return s.assessment_feedback_summary(since=since)
