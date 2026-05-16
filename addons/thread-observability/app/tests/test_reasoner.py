"""Tests for the redesigned deterministic issues reasoner."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from thread_observability.pipeline.reasoner import (
    DEAD_LINK_MIN_TICKS,
    PARTITION_CHANGE_WINDOW_MIN,
    run_reasoner,
)
from thread_observability.storage.sqlite_store import SQLiteStore


def _now() -> datetime:
    return datetime.now(tz=UTC)


def test_reasoner_no_observations(store: SQLiteStore) -> None:
    out = run_reasoner(store=store)
    assert out["opened"] == []
    assert out["closed"] == []
    assert store.list_active_issues() == []


def test_dead_link_reference_requires_persistence(store: SQLiteStore) -> None:
    reporter = "aa" * 8
    unknown = "bb" * 8
    store.upsert_node_metadata(eui64=reporter)

    # Unknown neighbor reference appears in the current snapshot.
    store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [{"neighbor_eui64": unknown, "link_established": True}],
        partition_id=1,
        observed_at=_now().isoformat(),
    )

    for _ in range(DEAD_LINK_MIN_TICKS - 1):
        run_reasoner(store=store)
        assert not any(i["kind"] == "dead_link_reference" for i in store.list_active_issues())

    run_reasoner(store=store)
    issues = [i for i in store.list_active_issues() if i["kind"] == "dead_link_reference"]
    assert len(issues) == 1
    assert issues[0]["eui64"] == reporter
    refs = issues[0]["evidence"]["references"]
    assert any(r.get("neighbor_eui64") == unknown for r in refs)

    # Clearing predicate: remove the unknown reference, then re-run.
    store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [],
        partition_id=1,
        observed_at=_now().isoformat(),
    )
    out = run_reasoner(store=store)
    assert out["closed"]
    assert not any(i["kind"] == "dead_link_reference" for i in store.list_active_issues())


def test_route_to_otbr_unreachable_opens_on_unknown_next_hop(store: SQLiteStore) -> None:
    otbr = "11" * 8
    router = "22" * 8
    store.upsert_node_metadata(eui64=otbr, role="border_router")
    store.upsert_node_metadata(eui64=router)
    store.set_node_diagnostics(otbr, partition_id=1, routing_role="leader")
    store.set_node_diagnostics(router, partition_id=1, routing_role="router")
    store.set_node_router_id(otbr, 1)
    store.set_node_router_id(router, 2)

    # RouteTable entry points to a router_id that doesn't exist in the
    # partition's router index → unknown_next_hop.
    store.replace_links_for_reporter(
        router,
        "route_table",
        [
            {
                "neighbor_eui64": otbr,
                "path_cost": 2,
                "next_hop_router_id": 10,
                "link_established": True,
            }
        ],
        partition_id=1,
        observed_at=_now().isoformat(),
    )

    run_reasoner(store=store)
    issues = [i for i in store.list_active_issues() if i["kind"] == "route_to_otbr_unreachable"]
    assert len(issues) == 1
    assert issues[0]["eui64"] == router
    assert issues[0]["severity"] == "crit"

    # Clear by switching to a direct route (path_cost=1 + link_established).
    store.replace_links_for_reporter(
        router,
        "route_table",
        [{"neighbor_eui64": otbr, "path_cost": 1, "link_established": True}],
        partition_id=1,
        observed_at=_now().isoformat(),
    )
    out = run_reasoner(store=store)
    assert out["closed"]
    assert not any(i["kind"] == "route_to_otbr_unreachable" for i in store.list_active_issues())


def test_real_partition_split_requires_recent_partition_change(store: SQLiteStore) -> None:
    # Two live partitions exist.
    leader_a = "aa" * 8
    leader_b = "bb" * 8
    mover = "cc" * 8
    store.upsert_node_metadata(eui64=leader_a)
    store.upsert_node_metadata(eui64=leader_b)
    store.upsert_node_metadata(eui64=mover)
    store.set_node_diagnostics(leader_a, partition_id=1, routing_role="leader")
    store.set_node_diagnostics(leader_b, partition_id=2, routing_role="leader")
    store.set_node_diagnostics(mover, partition_id=2, routing_role="router")

    # Evidence: a device transitioned between partitions within the window.
    ts = (_now() - timedelta(minutes=1)).isoformat()
    store.insert_event(
        eui64=mover,
        type="partition_change",
        ts=ts,
        payload={"from": 1, "to": 2},
    )

    run_reasoner(store=store)
    issues = [i for i in store.list_active_issues() if i["kind"] == "real_partition_split"]
    assert len(issues) == 1
    assert issues[0]["eui64"] is None
    assert any(ch.get("eui64") == mover for ch in issues[0]["evidence"]["recent_partition_changes"])

    # Advance beyond the window; no recent partition_change → auto-close.
    future = _now() + timedelta(minutes=PARTITION_CHANGE_WINDOW_MIN + 5)
    out = run_reasoner(store=store, now=future)
    assert out["closed"]
    assert not any(i["kind"] == "real_partition_split" for i in store.list_active_issues())

