"""Tests for assessment feedback + quality summary (#22)."""

from __future__ import annotations

import pytest

from thread_observability.services.assessment import feedback as fb
from thread_observability.storage.sqlite_store import SQLiteStore


def _seed_finding(store: SQLiteStore, *, finding_type: str = "parent_flapping") -> dict:
    return store.upsert_assessment_finding(
        finding_id=f"evid-{finding_type}",
        finding_key=f"key-{finding_type}",
        verdict="investigate",
        severity="investigate",
        confidence=0.7,
        headline="something",
        evidence=[{"tool": "a", "key_finding": "b"}],
        finding_type=finding_type,
    )


def test_mark_resolved_clears_finding(store: SQLiteStore) -> None:
    f = _seed_finding(store)
    rec = fb.mark_outcome(
        finding_id=f["finding_id"], outcome="resolved", store=store
    )
    assert rec["outcome"] == "resolved"
    open_rows = store.list_assessment_findings(state="open")
    assert open_rows == []
    cleared = store.list_assessment_findings(state="cleared")
    assert any(r["cleared_by"] == "user_resolve" for r in cleared)


def test_mark_wrong_records_outcome_and_clears(store: SQLiteStore) -> None:
    f = _seed_finding(store, finding_type="weak_link")
    fb.mark_outcome(finding_id=f["finding_id"], outcome="wrong", store=store)
    cleared = store.list_assessment_findings(state="cleared")
    assert any(r["cleared_by"] == "user_wrong" for r in cleared)


def test_mark_invalid_outcome_raises(store: SQLiteStore) -> None:
    f = _seed_finding(store)
    with pytest.raises(ValueError):
        fb.mark_outcome(finding_id=f["finding_id"], outcome="cheese", store=store)


def test_mark_unknown_finding_raises(store: SQLiteStore) -> None:
    with pytest.raises(LookupError):
        fb.mark_outcome(finding_id="evid-missing", outcome="resolved", store=store)


def test_quality_summary_flags_noisy_type(store: SQLiteStore) -> None:
    for i in range(4):
        f = store.upsert_assessment_finding(
            finding_id=f"evid-n{i}",
            finding_key=f"k{i}",
            verdict="investigate",
            severity="investigate",
            confidence=0.5,
            headline="x",
            evidence=[{"tool": "a", "key_finding": "b"}],
            finding_type="parent_flapping",
        )
        # 3 of 4 are wrong -> noisy
        outcome = "wrong" if i < 3 else "resolved"
        fb.mark_outcome(
            finding_id=f["finding_id"], outcome=outcome, store=store
        )
    summary = fb.quality_summary(store=store)
    assert summary["total_findings"] == 4
    noisy_types = {n["finding_type"] for n in summary["noisy_signal_types"]}
    assert "parent_flapping" in noisy_types
