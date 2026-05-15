"""Tests for the health snapshot builder."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from thread_observability.health import build_health_snapshot
from thread_observability.storage.sqlite_store import SQLiteStore


def test_health_empty_store(store: SQLiteStore) -> None:
    snap = build_health_snapshot(store=store)
    assert snap["status"] == "ok"
    assert snap["summary"]["total_nodes"] == 0
    assert snap["active_issues"]["count"] == 0
    assert snap["data_age_seconds"] is None


def test_health_classifies_nodes(store: SQLiteStore) -> None:
    now = datetime.now(tz=UTC)
    # Registry-first (v9): event ingestion no longer auto-creates node
    # rows. Seed each EUI as a registry-known node so health classification
    # can see them.
    store.upsert_node_metadata(eui64="aa" * 8)
    store.upsert_node_metadata(eui64="bb" * 8)
    store.upsert_node_metadata(eui64="cc" * 8)
    store.insert_event(eui64="aa" * 8, type="attach", ts=now.isoformat())
    store.insert_event(eui64="bb" * 8, type="attach",
                       ts=(now - timedelta(minutes=10)).isoformat())
    store.insert_event(eui64="cc" * 8, type="attach",
                       ts=(now - timedelta(hours=2)).isoformat())
    snap = build_health_snapshot(store=store)
    s = snap["summary"]
    assert s["healthy_nodes"] == 1
    assert s["online_nodes"] == 1
    assert s["sleeping_nodes"] == 0
    assert s["stale_nodes"] == 1
    assert s["offline_nodes"] == 1
    assert s["total_nodes"] == 3


def test_health_counts_sleeping_nodes_separately(store: SQLiteStore) -> None:
    sleepy = "aa" * 8
    parent = "bb" * 8
    store.upsert_node_metadata(eui64=sleepy, device_id="shade-1")
    store.upsert_node_metadata(eui64=parent)
    store.set_node_diagnostics(parent, routing_role="router", partition_id=1234)
    store.set_node_diagnostics(sleepy, routing_role="sleepy_end_device", partition_id=1234)
    now = datetime.now(tz=UTC)
    store.insert_event(eui64=parent, type="attach", ts=now.isoformat())
    store.insert_event(eui64=sleepy, type="attach", ts=now.isoformat())
    store.apply_availability([(sleepy, False, "ha_entity")])
    store.replace_links_for_reporter(
        parent,
        "neighbor_table",
        [{
            "neighbor_eui64": sleepy,
            "rssi_avg": -65,
            "lqi_in": 3,
            "is_child": True,
        }],
    )
    store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)

    snap = build_health_snapshot(store=store)
    s = snap["summary"]
    assert s["online_nodes"] == 1
    assert s["sleeping_nodes"] == 1
    assert s["healthy_nodes"] == 1
    assert s["offline_nodes"] == 0
    assert s["stale_nodes"] == 0
    assert s["total_nodes"] == 2


def test_health_reflects_critical_issues(store: SQLiteStore) -> None:
    store.open_issue(kind="offline_node", severity="crit", eui64="aa" * 8)
    snap = build_health_snapshot(store=store)
    assert snap["status"] == "critical"
    assert snap["active_issues"]["by_severity"]["crit"] == 1


def test_health_counts_duplicate_physical_devices(store: SQLiteStore) -> None:
    """v0.9.46: hardware-identity duplicates surface in summary."""
    store.upsert_node_metadata(
        eui64="aa" * 8, vendor_id=1, product_id=2, serial_number="DUP",
    )
    store.upsert_node_metadata(
        eui64="bb" * 8, vendor_id=1, product_id=2, serial_number="DUP",
    )
    store.upsert_node_metadata(
        eui64="cc" * 8, vendor_id=1, product_id=2, serial_number="UNIQUE",
    )
    snap = build_health_snapshot(store=store)
    s = snap["summary"]
    assert s["duplicate_physical_device_groups"] == 1
    assert s["duplicate_physical_device_rows"] == 2


def test_health_counts_distinct_thread_networks(store: SQLiteStore) -> None:
    """v0.9.46: distinct extended_pan_ids surface in summary."""
    store.upsert_node_metadata(eui64="11" * 8)
    store.set_node_diagnostics("11" * 8, extended_pan_id="aaaaaaaaaaaaaaaa")
    store.upsert_node_metadata(eui64="22" * 8)
    store.set_node_diagnostics("22" * 8, extended_pan_id="aaaaaaaaaaaaaaaa")
    store.upsert_node_metadata(eui64="33" * 8)
    store.set_node_diagnostics("33" * 8, extended_pan_id="bbbbbbbbbbbbbbbb")
    snap = build_health_snapshot(store=store)
    assert snap["summary"]["distinct_thread_networks"] == 2

