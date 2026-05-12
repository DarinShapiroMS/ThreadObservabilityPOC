"""Tests for the deterministic anomaly reasoner."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from thread_observability.pipeline.reasoner import (
    ATTACH_FAIL_THRESHOLD,
    MESH_DISAGREEMENT_PCT_THRESHOLD,
    OFFLINE_THRESHOLD_MIN,
    PARENT_CHURN_THRESHOLD,
    RE_ATTACH_STORM_MIN_EVENTS,
    RE_ATTACH_STORM_MIN_REPORTERS,
    run_reasoner,
)
from thread_observability.storage.sqlite_store import SQLiteStore


def _now() -> datetime:
    return datetime.now(tz=UTC)


def test_reasoner_no_events(store: SQLiteStore) -> None:
    out = run_reasoner(store=store)
    assert out["opened"] == []
    assert out["closed"] == []
    assert store.list_active_issues() == []


def test_parent_churn_opens_issue(store: SQLiteStore) -> None:
    eui = "11" * 8
    for i in range(PARENT_CHURN_THRESHOLD):
        store.insert_event(
            eui64=eui,
            type="parent_change",
            ts=(_now() - timedelta(minutes=i)).isoformat(),
            parent_eui64="aa" * 8,
        )
    out = run_reasoner(store=store)
    assert len(out["opened"]) == 1
    issues = store.list_active_issues()
    assert len(issues) == 1
    assert issues[0]["kind"] == "parent_churn"
    assert issues[0]["eui64"] == eui
    assert issues[0]["evidence"]["count"] == PARENT_CHURN_THRESHOLD


def test_parent_churn_dedup(store: SQLiteStore) -> None:
    eui = "11" * 8
    for i in range(PARENT_CHURN_THRESHOLD + 1):
        store.insert_event(eui64=eui, type="parent_change",
                           ts=(_now() - timedelta(minutes=i)).isoformat(),
                           parent_eui64="aa" * 8)
    run_reasoner(store=store)
    second = run_reasoner(store=store)
    assert second["opened"] == []
    assert len(second["still_open"]) == 1
    assert len(store.list_active_issues()) == 1


def test_attach_failures_open_and_close(store: SQLiteStore) -> None:
    eui = "22" * 8
    for i in range(ATTACH_FAIL_THRESHOLD):
        store.insert_event(eui64=eui, type="attach_failed",
                           ts=(_now() - timedelta(minutes=i)).isoformat())
    first = run_reasoner(store=store)
    assert len(first["opened"]) == 1
    issue_id = first["opened"][0]

    # Advance time so the failure events fall outside the window;
    # rerun and confirm the attach_failures issue auto-closes. (The node
    # will also flip to offline in that future, which is correct behaviour.)
    far_future = _now() + timedelta(hours=2)
    closed = run_reasoner(store=store, now=far_future)
    assert issue_id in closed["closed"]
    still_open = store.list_active_issues()
    assert all(i["kind"] != "attach_failures" for i in still_open)


def test_offline_node_opens_crit_issue(store: SQLiteStore) -> None:
    eui = "33" * 8
    old = (_now() - timedelta(minutes=OFFLINE_THRESHOLD_MIN + 5)).isoformat()
    # Registry-first (v9): event ingestion no longer auto-creates node
    # rows. Seed the node first so insert_event can UPDATE its last_seen.
    store.upsert_node_metadata(eui64=eui)
    store.insert_event(eui64=eui, type="attach", ts=old)
    out = run_reasoner(store=store)
    assert len(out["opened"]) == 1
    issues = store.list_active_issues()
    assert issues[0]["kind"] == "offline_node"
    assert issues[0]["severity"] == "crit"


# ---------------------------------------------------------------------------
# v0.9.43 — Tier 2 rules
# ---------------------------------------------------------------------------


def test_re_attach_storm_requires_multiple_reporters(store: SQLiteStore) -> None:
    """A single reporter shouldn't trip the storm rule no matter the count.

    The whole point of the cross-reporter requirement is to filter out a
    single flaky link and reserve the alarm for a partition-wide
    identity problem (the Foyer-Light case).
    """
    neighbor = "aa" * 8
    store.upsert_node_metadata(eui64=neighbor)
    reporter_a = "bb" * 8
    store.upsert_node_metadata(eui64=reporter_a)
    for _ in range(5):
        store.insert_event(
            eui64=neighbor,
            type="re_attached_node",
            payload={
                "neighbor_eui64": neighbor,
                "reporter_eui64": reporter_a,
                "counter": "link_frame_counter",
                "old_value": 100, "new_value": 1,
            },
        )
    out = run_reasoner(store=store)
    assert not any(
        i["kind"] == "re_attach_storm" for i in store.list_active_issues()
    ), "single-reporter storm should NOT open an issue"
    assert out["opened"] == []


def test_re_attach_storm_opens_with_distinct_reporters(store: SQLiteStore) -> None:
    neighbor = "aa" * 8
    store.upsert_node_metadata(eui64=neighbor)
    reporters = [f"{i:02x}" * 8 for i in range(0xb0, 0xb0 + RE_ATTACH_STORM_MIN_REPORTERS)]
    for r in reporters:
        store.upsert_node_metadata(eui64=r)
    # One event per reporter — meets MIN_EVENTS (2) and MIN_REPORTERS (2).
    assert RE_ATTACH_STORM_MIN_EVENTS <= len(reporters)
    for r in reporters:
        store.insert_event(
            eui64=neighbor,
            type="re_attached_node",
            payload={
                "neighbor_eui64": neighbor,
                "reporter_eui64": r,
                "counter": "link_frame_counter",
                "old_value": 100, "new_value": 1,
            },
        )
    out = run_reasoner(store=store)
    storms = [i for i in store.list_active_issues() if i["kind"] == "re_attach_storm"]
    assert len(storms) == 1
    assert storms[0]["eui64"] == neighbor
    assert len(out["opened"]) == 1
    assert sorted(storms[0]["evidence"]["distinct_reporters"]) == sorted(reporters)


def test_mesh_disagreement_opens_when_delta_over_threshold(store: SQLiteStore) -> None:
    eui = "cc" * 8
    # Seed a node with a self-reported MAC TX counter and a fresh diag ts.
    store.upsert_node_metadata(eui64=eui)
    # Set tx_total_count + diag_updated_at directly via the diagnostics
    # setter used by the discovery pipeline.
    store.set_node_diagnostics(eui, tx_total_count=1000)
    # OTBR-witnessed value ≫ threshold below.
    store.insert_otbr_diagnostic(
        target_eui64=eui,
        target_rloc16=0x4400,
        mac_tx_total=500,  # 50% delta vs. 1000
    )
    assert MESH_DISAGREEMENT_PCT_THRESHOLD <= 50.0
    run_reasoner(store=store)
    disagreements = [
        i for i in store.list_active_issues() if i["kind"] == "mesh_disagreement"
    ]
    assert len(disagreements) == 1
    ev = disagreements[0]["evidence"]
    assert ev["self_tx_total"] == 1000
    assert ev["otbr_tx_total"] == 500
    assert ev["delta_pct"] >= MESH_DISAGREEMENT_PCT_THRESHOLD


def test_mesh_disagreement_skips_when_under_threshold(store: SQLiteStore) -> None:
    eui = "dd" * 8
    store.upsert_node_metadata(eui64=eui)
    store.set_node_diagnostics(eui, tx_total_count=1000)
    # 5% delta — well under default 25% threshold.
    store.insert_otbr_diagnostic(target_eui64=eui, mac_tx_total=950)
    run_reasoner(store=store)
    assert not any(
        i["kind"] == "mesh_disagreement" for i in store.list_active_issues()
    )


# ---------------------------------------------------------------------------
# v0.9.44 — Tier 3 observer-suppression annotation
# ---------------------------------------------------------------------------


def test_offline_issue_downgrades_when_observer_was_down(store: SQLiteStore) -> None:
    """A ``crit`` ``offline_node`` issue downgrades to ``warn`` and
    carries ``suppressed_by`` evidence when an observer-side disruption
    overlaps the (last_seen → now) trigger window.
    """
    eui = "ee" * 8
    old = (_now() - timedelta(minutes=OFFLINE_THRESHOLD_MIN + 5)).isoformat()
    store.upsert_node_metadata(eui64=eui)
    store.insert_event(eui64=eui, type="attach", ts=old)

    # Observer outage that spans the gap between last_seen and now.
    obs_started = (_now() - timedelta(minutes=OFFLINE_THRESHOLD_MIN)).isoformat()
    obs_ended = (_now() - timedelta(minutes=OFFLINE_THRESHOLD_MIN - 2)).isoformat()
    store.insert_observer_event(
        source="addon:core_matter_server",
        kind="outage",
        started_at=obs_started,
        ended_at=obs_ended,
    )

    run_reasoner(store=store)
    issues = [i for i in store.list_active_issues() if i["kind"] == "offline_node"]
    assert len(issues) == 1
    issue = issues[0]
    assert issue["severity"] == "warn", "crit should have been downgraded"
    assert "suppressed_by" in issue["evidence"]
    suppressors = issue["evidence"]["suppressed_by"]
    assert len(suppressors) == 1
    assert suppressors[0]["source"] == "addon:core_matter_server"
    assert suppressors[0]["kind"] == "outage"


def test_offline_issue_stays_crit_when_no_observer_disruption(store: SQLiteStore) -> None:
    """Same scenario but without an overlapping observer event — the
    issue remains at full ``crit`` severity with no suppression.
    """
    eui = "ff" * 8
    old = (_now() - timedelta(minutes=OFFLINE_THRESHOLD_MIN + 5)).isoformat()
    store.upsert_node_metadata(eui64=eui)
    store.insert_event(eui64=eui, type="attach", ts=old)

    run_reasoner(store=store)
    issues = [i for i in store.list_active_issues() if i["kind"] == "offline_node"]
    assert len(issues) == 1
    assert issues[0]["severity"] == "crit"
    assert "suppressed_by" not in issues[0]["evidence"]


def test_observer_event_strictly_before_trigger_does_not_suppress(
    store: SQLiteStore,
) -> None:
    """An old observer event from yesterday must not suppress today's
    issues. Suppression only applies within the trigger window plus grace.
    """
    eui = "ab" * 8
    old = (_now() - timedelta(minutes=OFFLINE_THRESHOLD_MIN + 5)).isoformat()
    store.upsert_node_metadata(eui64=eui)
    store.insert_event(eui64=eui, type="attach", ts=old)

    # An observer outage from 6 hours ago — well before last_seen.
    long_ago_start = (_now() - timedelta(hours=6)).isoformat()
    long_ago_end = (_now() - timedelta(hours=6, minutes=-1)).isoformat()
    store.insert_observer_event(
        source="addon:self", kind="restart",
        started_at=long_ago_start, ended_at=long_ago_end,
    )

    run_reasoner(store=store)
    issues = [i for i in store.list_active_issues() if i["kind"] == "offline_node"]
    assert len(issues) == 1
    assert issues[0]["severity"] == "crit"
    assert "suppressed_by" not in issues[0]["evidence"]


def test_reasoner_closes_stale_partition_split_when_topology_resolved(
    store: SQLiteStore,
) -> None:
    """v0.9.45: the reasoner owns ``partition_split`` close-on-resolve.

    When live topology shows <= 1 partition, any still-open
    partition_split issue is stale and must be closed by the reasoner
    even if the discovery stage never ran the close path.
    """
    eui = "aa" * 8
    store.upsert_node_metadata(eui64=eui, friendly_name="X", role="router")
    issue_id = store.open_issue(
        kind="partition_split",
        severity="warning",
        eui64=None,
        evidence={
            "partition_count": 2,
            "partitions": [
                {"partition_id": 1, "members": [eui]},
                {"partition_id": 2, "members": ["bb" * 8]},
            ],
        },
    )
    assert any(i["id"] == issue_id for i in store.list_active_issues())

    result = run_reasoner(store=store)
    assert issue_id in result["closed"]
    assert not any(i["id"] == issue_id for i in store.list_active_issues())


