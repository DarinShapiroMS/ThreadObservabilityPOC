"""Tests for the Tier 4 ``analyze_node`` bundled consultant tool."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from thread_observability.pipeline import analyze_node as an_mod
from thread_observability.pipeline import playbooks as pb_mod
from thread_observability.storage.sqlite_store import SQLiteStore


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def test_analyze_unknown_eui_returns_structure_with_node_none(
    store: SQLiteStore,
) -> None:
    pb_mod.reset_cache_for_tests()
    res = an_mod.analyze_node("aa" * 8, store=store)
    assert res["node"] is None
    assert res["open_issues"] == []
    assert res["recent_issues"] == []
    assert res["neighbors"] == []
    assert res["timeline"]["count"] == 0
    assert res["playbooks"] == []
    assert "baselines" in res


def test_analyze_with_open_issue_attaches_matching_playbooks(
    store: SQLiteStore,
) -> None:
    pb_mod.reset_cache_for_tests()
    eui = "deadbeefcafe0001"
    store.upsert_node_metadata(eui64=eui, friendly_name="Bulb", role="end_device")
    store.open_issue(
        kind="parent_churn",
        severity="warn",
        eui64=eui,
        evidence={"changes": 5},
    )
    res = an_mod.analyze_node(eui, store=store)
    assert res["node"] is not None
    assert len(res["open_issues"]) == 1
    assert res["matched_issue_kinds"] == ["parent_churn"]
    pb_ids = {p["id"] for p in res["playbooks"]}
    # parent_churn playbook itself must be included; observer_suppressed
    # also covers parent_churn and should be in the result.
    assert "parent_churn" in pb_ids
    assert "observer_suppressed" in pb_ids


def test_analyze_baselines_compare_recent_vs_prior(store: SQLiteStore) -> None:
    pb_mod.reset_cache_for_tests()
    eui = "deadbeefcafe0001"
    store.upsert_node_metadata(eui64=eui, friendly_name="Router", role="router")
    now = datetime(2026, 5, 12, 12, 0, 0, tzinfo=UTC)

    # 3 parent_change events in the recent 7-day window.
    for i in range(3):
        store.insert_event(
            eui64=eui,
            type="parent_change",
            ts=_iso(now - timedelta(days=1, hours=i)),
        )
    # 1 parent_change in the prior 7-day window.
    store.insert_event(
        eui64=eui,
        type="parent_change",
        ts=_iso(now - timedelta(days=10)),
    )

    res = an_mod.analyze_node(eui, store=store, baseline_days=7, now=now)
    b = res["baselines"]
    assert b["parent_change_count_recent"] == 3
    assert b["parent_change_count_prior"] == 1
    assert b["parent_change_delta"] == 2


def test_analyze_timeline_includes_events_for_node(store: SQLiteStore) -> None:
    pb_mod.reset_cache_for_tests()
    eui = "deadbeefcafe0001"
    store.upsert_node_metadata(eui64=eui, friendly_name="X", role="end_device")
    now = datetime(2026, 5, 12, 12, 0, 0, tzinfo=UTC)
    store.insert_event(eui64=eui, type="attach", ts=_iso(now - timedelta(hours=1)))
    store.insert_event(
        eui64=eui, type="parent_change", ts=_iso(now - timedelta(minutes=30))
    )

    res = an_mod.analyze_node(eui, store=store, timeline_hours=24, now=now)
    kinds = [r["kind"] for r in res["timeline"]["rows"]]
    assert "attach" in kinds
    assert "parent_change" in kinds


def test_analyze_binds_global_partition_split_issue_via_evidence(
    store: SQLiteStore,
) -> None:
    """A global ``partition_split`` issue whose evidence lists the EUI
    as a singleton-partition member must show up in ``open_issues``."""
    pb_mod.reset_cache_for_tests()
    lonely = "aa" * 8
    majority = "bb" * 8
    store.upsert_node_metadata(eui64=lonely, friendly_name="Lonely", role="router")
    store.upsert_node_metadata(eui64=majority, friendly_name="Big", role="router")
    store.open_issue(
        kind="partition_split",
        severity="warning",
        eui64=None,
        evidence={
            "partition_count": 2,
            "partitions": [
                {"partition_id": 1, "member_count": 1, "members": [lonely]},
                {"partition_id": 2, "member_count": 1, "members": [majority]},
            ],
        },
    )
    res = an_mod.analyze_node(lonely, store=store)
    assert len(res["open_issues"]) == 1
    assert res["open_issues"][0]["kind"] == "partition_split"
    assert res["open_issues"][0].get("implicated_via") == "evidence"
    assert "partition_split" in res["matched_issue_kinds"]
    pb_ids = {p["id"] for p in res["playbooks"]}
    assert "partition_split" in pb_ids


def test_analyze_does_not_implicate_unrelated_node_in_global_issue(
    store: SQLiteStore,
) -> None:
    """A node NOT named in a global issue's evidence must not pick it up."""
    pb_mod.reset_cache_for_tests()
    inside = "aa" * 8
    outside = "cc" * 8
    store.upsert_node_metadata(eui64=outside, friendly_name="Outside", role="router")
    store.open_issue(
        kind="partition_split",
        severity="warning",
        eui64=None,
        evidence={
            "partition_count": 2,
            "partitions": [
                {"partition_id": 1, "member_count": 1, "members": [inside]},
                {"partition_id": 2, "member_count": 1, "members": ["bb" * 8]},
            ],
        },
    )
    res = an_mod.analyze_node(outside, store=store)
    assert res["open_issues"] == []
    assert res["matched_issue_kinds"] == []
