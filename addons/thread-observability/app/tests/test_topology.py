"""Tests for the topology graph builder."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from thread_observability.pipeline.topology import build_topology
from thread_observability.storage.sqlite_store import SQLiteStore


def test_topology_empty(store: SQLiteStore) -> None:
    snap = build_topology(store=store)
    assert snap["node_count"] == 0
    assert snap["link_count"] == 0
    assert snap["nodes"] == []
    assert snap["links"] == []


def test_topology_links_from_attach(store: SQLiteStore) -> None:
    store.upsert_node_metadata(eui64="aa" * 8, friendly_name="Leader", role="leader")
    store.upsert_node_metadata(eui64="bb" * 8, friendly_name="Child",  role="router")
    store.insert_event(eui64="bb" * 8, type="attach",
                       parent_eui64="aa" * 8, rssi=-55, lqi=240)

    snap = build_topology(store=store)
    assert snap["node_count"] == 2
    # Links now come from the dedicated links table, not events. Events still
    # populate the per-node parent_eui64 fallback.
    assert snap["link_count"] == 0
    child = next(n for n in snap["nodes"] if n["eui64"] == "bb" * 8)
    assert child["parent_eui64"] == "aa" * 8
    assert child["last_rssi"] == -55
    assert child["last_lqi"] == 240


def test_topology_uses_latest_parent_change(store: SQLiteStore) -> None:
    store.upsert_node_metadata(eui64="cc" * 8)
    store.insert_event(eui64="cc" * 8, type="attach",
                       ts=(datetime.now(tz=UTC) - timedelta(minutes=10)).isoformat(),
                       parent_eui64="aa" * 8)
    store.insert_event(eui64="cc" * 8, type="parent_change",
                       ts=(datetime.now(tz=UTC) - timedelta(minutes=2)).isoformat(),
                       parent_eui64="bb" * 8)

    snap = build_topology(store=store)
    child = next(n for n in snap["nodes"] if n["eui64"] == "cc" * 8)
    assert child["parent_eui64"] == "bb" * 8


def test_topology_stale_window(store: SQLiteStore) -> None:
    # Node with an old attach event outside the freshness window
    store.upsert_node_metadata(eui64="dd" * 8)
    store.insert_event(eui64="dd" * 8, type="attach",
                       ts=(datetime.now(tz=UTC) - timedelta(hours=3)).isoformat(),
                       parent_eui64="aa" * 8)

    snap = build_topology(store=store, freshness_minutes=60)
    child = next(n for n in snap["nodes"] if n["eui64"] == "dd" * 8)
    assert child["parent_eui64"] is None, "old parent edge should not be inferred"
    assert snap["link_count"] == 0


# ---------------------------------------------------------------------------
# Matter cluster-53 backed topology (links table)
# ---------------------------------------------------------------------------

A = "aa" * 8
B = "bb" * 8
C = "cc" * 8
D = "dd" * 8


def test_topology_links_from_links_table(store: SQLiteStore) -> None:
    """Links come from the new links table populated by Matter diagnostics."""
    store.upsert_node_metadata(eui64=A, friendly_name="Leader")
    store.upsert_node_metadata(eui64=B, friendly_name="Router-2")
    store.replace_links_for_reporter(A, "neighbor_table", [
        {"neighbor_eui64": B, "rssi_avg": -50, "rssi_last": -52,
         "lqi_in": 240, "lqi_out": None, "is_child": 0,
         "age_seconds": 5, "frame_error_rate": 0, "message_error_rate": 0,
         "path_cost": None},
    ])
    store.replace_links_for_reporter(B, "neighbor_table", [
        {"neighbor_eui64": A, "rssi_avg": -52, "rssi_last": -55,
         "lqi_in": 230, "lqi_out": None, "is_child": 0,
         "age_seconds": 5, "frame_error_rate": 0, "message_error_rate": 0,
         "path_cost": None},
    ])

    snap = build_topology(store=store)
    assert snap["link_count"] == 2
    pairs = {(l["from"], l["to"]) for l in snap["links"]}
    assert (A, B) in pairs and (B, A) in pairs
    for link in snap["links"]:
        assert "weak_link" not in link["tags"]
        assert "asymmetric" not in link["tags"]


def test_topology_tags_weak_and_asymmetric(store: SQLiteStore) -> None:
    store.upsert_node_metadata(eui64=A)
    store.upsert_node_metadata(eui64=B)
    # A reports B at -90 (weak); B reports A at -50 (huge asymmetry).
    store.replace_links_for_reporter(A, "neighbor_table", [
        {"neighbor_eui64": B, "rssi_avg": -90, "rssi_last": -92,
         "lqi_in": 50, "lqi_out": None, "is_child": 0,
         "age_seconds": 5, "frame_error_rate": 20, "message_error_rate": 5,
         "path_cost": None},
    ])
    store.replace_links_for_reporter(B, "neighbor_table", [
        {"neighbor_eui64": A, "rssi_avg": -50, "rssi_last": -50,
         "lqi_in": 240, "lqi_out": None, "is_child": 0,
         "age_seconds": 5, "frame_error_rate": 0, "message_error_rate": 0,
         "path_cost": None},
    ])

    snap = build_topology(store=store)
    weak = [l for l in snap["links"] if l["from"] == A and l["to"] == B][0]
    assert "weak_link" in weak["tags"]
    assert "asymmetric" in weak["tags"]
    assert "high_error" in weak["tags"]


def test_topology_partition_split_detection(store: SQLiteStore) -> None:
    store.upsert_node_metadata(eui64=A)
    store.upsert_node_metadata(eui64=B)
    store.upsert_node_metadata(eui64=C)
    store.set_node_diagnostics(A, partition_id=1111, routing_role="leader")
    store.set_node_diagnostics(B, partition_id=1111, routing_role="router")
    store.set_node_diagnostics(C, partition_id=2222, routing_role="leader")

    snap = build_topology(store=store)
    assert snap["split"] is True
    pids = {p["partition_id"] for p in snap["partitions"]}
    assert pids == {1111, 2222}
    leaders = {p["partition_id"]: p["leader_eui64"] for p in snap["partitions"]}
    assert leaders[1111] == A
    assert leaders[2222] == C


def test_topology_partition_healthy_single(store: SQLiteStore) -> None:
    store.upsert_node_metadata(eui64=A)
    store.upsert_node_metadata(eui64=B)
    store.set_node_diagnostics(A, partition_id=42, routing_role="leader")
    store.set_node_diagnostics(B, partition_id=42, routing_role="router")

    snap = build_topology(store=store)
    assert snap["split"] is False
    assert len(snap["partitions"]) == 1
    assert snap["partitions"][0]["member_count"] == 2


def test_topology_parent_inferred_from_is_child(store: SQLiteStore) -> None:
    """If router A reports D as is_child=1, D's parent_eui64 is A."""
    store.upsert_node_metadata(eui64=A)
    store.upsert_node_metadata(eui64=D)
    store.replace_links_for_reporter(A, "neighbor_table", [
        {"neighbor_eui64": D, "rssi_avg": -60, "rssi_last": -60,
         "lqi_in": 200, "lqi_out": None, "is_child": 1,
         "age_seconds": 1, "frame_error_rate": 0, "message_error_rate": 0,
         "path_cost": None},
    ])
    snap = build_topology(store=store)
    child = next(n for n in snap["nodes"] if n["eui64"] == D)
    assert child["parent_eui64"] == A


def test_topology_hides_phantoms_by_default(store: SQLiteStore) -> None:
    """Phantoms should be filtered out of nodes and links by default."""
    # Registry-first (v9): seed the rows explicitly; bump is UPDATE-only.
    store.upsert_node_metadata(eui64=A)
    store.upsert_node_metadata(eui64=B)
    store.bump_last_referenced([A, B])
    store.replace_links_for_reporter(A, "neighbor_table", [
        {"neighbor_eui64": B, "rssi_avg": -55, "is_child": 1},
    ])
    # Backdate B to make it a phantom.
    stale = (datetime.now(tz=UTC) - timedelta(hours=48)).isoformat()
    with store._tx() as conn:  # noqa: SLF001
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?", (stale, B))
    store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)

    snap = build_topology(store=store)
    euis = {n["eui64"] for n in snap["nodes"]}
    assert A in euis
    assert B not in euis
    # No link should touch B.
    for ln in snap["links"]:
        assert B not in (ln.get("reporter_eui64"), ln.get("neighbor_eui64"))

    # With include_phantoms=True, B reappears.
    snap2 = build_topology(store=store, include_phantoms=True)
    euis2 = {n["eui64"] for n in snap2["nodes"]}
    assert B in euis2
    b_row = next(n for n in snap2["nodes"] if n["eui64"] == B)
    assert b_row["status"] == "phantom"
