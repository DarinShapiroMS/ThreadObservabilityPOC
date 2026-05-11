"""Unit tests for node metadata enrichment."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from thread_observability.pipeline import nodes


def test_get_node_display_name_prefers_friendly_name() -> None:
    node = {"eui64": "1234567890abcdef", "friendly_name": "Living Room"}
    assert nodes.get_node_display_name(node) == "Living Room"


def test_get_node_display_name_falls_back_to_eui64_suffix() -> None:
    node = {"eui64": "1234567890abcdef"}
    assert nodes.get_node_display_name(node) == "CDEF"


def test_infer_node_status_healthy_within_threshold() -> None:
    now = datetime.now(tz=UTC).isoformat()
    node = {"last_seen": now}
    assert nodes.infer_node_status(node, stale_minutes=60) == "healthy"


def test_infer_node_status_stale_past_threshold() -> None:
    past = (datetime.now(tz=UTC) - timedelta(minutes=45)).isoformat()
    node = {"last_seen": past}
    assert nodes.infer_node_status(node, stale_minutes=30) == "stale"


def test_infer_node_status_offline_very_old() -> None:
    past = (datetime.now(tz=UTC) - timedelta(hours=2)).isoformat()
    node = {"last_seen": past}
    assert nodes.infer_node_status(node, stale_minutes=30) == "offline"


def test_infer_node_status_offline_no_last_seen() -> None:
    node = {}
    assert nodes.infer_node_status(node) == "offline"


def test_get_node_summary(store) -> None:
    store.insert_event(eui64="aabbccddeeff0011", type="attach", rssi=-70, lqi=180)
    store.upsert_node_metadata(eui64="aabbccddeeff0011", friendly_name="Sensor A")

    summary = nodes.get_node_summary("aabbccddeeff0011", store=store, include_signal_strength=True)
    assert summary["eui64"] == "aabbccddeeff0011"
    assert summary["friendly_name"] == "Sensor A"
    assert summary["display_name"] == "Sensor A"
    assert summary["status"] == "healthy"
    assert summary["signal_strength"]["rssi"] == -70
    assert summary["signal_strength"]["lqi"] == 180


def test_list_nodes_enriched(store) -> None:
    store.insert_event(eui64="1111111111111111", type="attach")
    store.insert_event(eui64="2222222222222222", type="attach")
    store.upsert_node_metadata(eui64="1111111111111111", friendly_name="Node1")
    store.upsert_node_metadata(eui64="2222222222222222", friendly_name="Node2")

    enriched = nodes.list_nodes_enriched(store=store)
    assert len(enriched) == 2
    assert enriched[0]["display_name"] in ("Node1", "Node2")
    assert all("status" in n for n in enriched)


def test_get_latest_signal_strength(store) -> None:
    store.insert_event(eui64="aabbccddeeff0011", type="parent_response", rssi=-65, lqi=200)
    store.insert_event(eui64="aabbccddeeff0011", type="parent_response", rssi=-70, lqi=180)

    strength = nodes.get_latest_signal_strength("aabbccddeeff0011", store=store)
    # Latest is the first one retrieved (events ordered DESC by default)
    assert strength["rssi"] == -70
    assert strength["lqi"] == 180
    assert strength["rssi_avg"] in (-67, -68)  # average of -65 and -70
    assert strength["lqi_avg"] == 190  # average of 200 and 180
