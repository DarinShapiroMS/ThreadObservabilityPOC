"""Tests for v0.9.45 Fix C: re_attached_node reporter-reattach guard.

When a router's own ``parent_change_count`` increases (it re-attached
to a new parent), MLE establishes new sessions with every neighbor and
each neighbor's link/MLE frame counter — *as seen by this reporter* —
resets to a fresh value. Without a guard this looks like every
neighbor re-attached, producing a storm of false ``re_attached_node``
events attributed to the wrong devices.
"""

from __future__ import annotations

import asyncio

from thread_observability.pipeline import device_discovery as dd
from thread_observability.storage.sqlite_store import SQLiteStore


def _seed_rich(reporter: str, partition_id: int, parent_change_count: int,
               neighbors: list[tuple[str, int, int]]) -> None:
    """Replace ``_LAST_MATTER_RICH_INFO`` with one router + neighbors.

    ``neighbors`` is a list of ``(neighbor_eui, link_fc, mle_fc)`` tuples.
    """
    dd._LAST_MATTER_RICH_INFO = {
        1: {
            "eui64": reporter,
            "diagnostics": {
                "partition_id": partition_id,
                "parent_change_count": parent_change_count,
            },
            "neighbor_table": [
                {
                    "neighbor_eui64": nei,
                    "link_frame_counter": link_fc,
                    "mle_frame_counter": mle_fc,
                }
                for nei, link_fc, mle_fc in neighbors
            ],
            "route_table": [],
        },
    }


def _prior_snapshot(store: SQLiteStore) -> list[dict]:
    return store.list_nodes()


def test_reporter_reattach_suppresses_neighbor_re_attached_events(
    store: SQLiteStore,
) -> None:
    """Reporter's parent_change_count increments + neighbor counters
    reset → MUST NOT emit re_attached_node for those neighbors."""
    reporter = "aa" * 8
    nei_a = "bb" * 8
    nei_b = "cc" * 8
    store.upsert_node_metadata(eui64=reporter, role="router")
    store.upsert_node_metadata(eui64=nei_a, role="router")
    store.upsert_node_metadata(eui64=nei_b, role="router")

    # Baseline cycle: reporter at pcc=5, neighbors have established counters.
    _seed_rich(reporter, partition_id=1, parent_change_count=5,
               neighbors=[(nei_a, 1000, 2000), (nei_b, 1500, 2500)])
    asyncio.run(dd._persist_matter_diagnostics(store, _prior_snapshot(store)))
    # Update node diagnostics rows so prior_by_eui has parent_change_count.
    store.set_node_diagnostics(eui64=reporter, partition_id=1,
                               parent_change_count=5)

    # Now the reporter re-attaches: pcc=6, all neighbor counters reset.
    _seed_rich(reporter, partition_id=1, parent_change_count=6,
               neighbors=[(nei_a, 50, 60), (nei_b, 40, 70)])
    summary = asyncio.run(
        dd._persist_matter_diagnostics(store, _prior_snapshot(store))
    )
    assert summary["re_attached_events"] == 0, (
        "re_attached_node must be suppressed when the *reporter* "
        "re-attached; otherwise every neighbor falsely looks fresh."
    )


def test_neighbor_counter_drop_without_reporter_reattach_still_fires(
    store: SQLiteStore,
) -> None:
    """Reporter's pcc unchanged but a neighbor's counter drops →
    genuine neighbor re-attach, MUST still fire re_attached_node."""
    reporter = "aa" * 8
    nei = "bb" * 8
    store.upsert_node_metadata(eui64=reporter, role="router")
    store.upsert_node_metadata(eui64=nei, role="router")

    _seed_rich(reporter, partition_id=1, parent_change_count=5,
               neighbors=[(nei, 1000, 2000)])
    asyncio.run(dd._persist_matter_diagnostics(store, _prior_snapshot(store)))
    store.set_node_diagnostics(eui64=reporter, partition_id=1,
                               parent_change_count=5)

    # Reporter stable; neighbor counter drops (genuine re-attach).
    _seed_rich(reporter, partition_id=1, parent_change_count=5,
               neighbors=[(nei, 50, 60)])
    summary = asyncio.run(
        dd._persist_matter_diagnostics(store, _prior_snapshot(store))
    )
    assert summary["re_attached_events"] == 1
