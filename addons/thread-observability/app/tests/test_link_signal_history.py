"""Historical link-signal storage and API tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from thread_observability.api import link_signal_history
from thread_observability.api import mcp_tools
from thread_observability.api.http_api import create_core_app


def test_replace_links_for_reporter_records_added_changed_removed_and_heartbeat_samples(store):
    base = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    reporter = "aa" * 8
    peer = "bb" * 8
    store.upsert_node_metadata(eui64=reporter)
    store.upsert_node_metadata(eui64=peer)

    store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [{"neighbor_eui64": peer, "rssi_avg": -70, "rssi_last": -72, "lqi_in": 180, "is_child": False}],
        observed_at=base.isoformat(),
    )
    store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [{"neighbor_eui64": peer, "rssi_avg": -70, "rssi_last": -72, "lqi_in": 180, "is_child": False}],
        observed_at=(base + timedelta(minutes=5)).isoformat(),
    )
    store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [{"neighbor_eui64": peer, "rssi_avg": -65, "rssi_last": -67, "lqi_in": 190, "is_child": False}],
        observed_at=(base + timedelta(minutes=10)).isoformat(),
    )
    store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [{"neighbor_eui64": peer, "rssi_avg": -65, "rssi_last": -67, "lqi_in": 190, "is_child": False}],
        observed_at=(base + timedelta(minutes=75)).isoformat(),
    )
    store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [],
        observed_at=(base + timedelta(minutes=80)).isoformat(),
    )

    rows = store.list_link_signal_samples(reporter_eui64=reporter, source="neighbor_table")
    assert [row["change_reason"] for row in rows] == ["added", "changed", "heartbeat", "removed"]
    assert [row["present"] for row in rows] == [1, 1, 1, 0]
    assert rows[1]["rssi_avg"] == -65


def test_get_node_link_signal_history_groups_samples(store):
    base = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    reporter = "aa" * 8
    peer = "bb" * 8
    store.upsert_node_metadata(eui64=reporter)
    store.upsert_node_metadata(eui64=peer)
    store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [{"neighbor_eui64": peer, "rssi_avg": -72, "rssi_last": -73, "lqi_in": 175, "is_child": False}],
        observed_at=base.isoformat(),
    )
    store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [{"neighbor_eui64": peer, "rssi_avg": -66, "rssi_last": -67, "lqi_in": 190, "is_child": False}],
        observed_at=(base + timedelta(minutes=10)).isoformat(),
    )

    out = link_signal_history.get_node_link_signal_history(
        eui64=reporter,
        since=base.isoformat(),
        until=(base + timedelta(minutes=20)).isoformat(),
    )
    assert out["link_count"] == 1
    assert out["sample_count"] == 2
    link = out["links"][0]
    assert link["peer_eui64"] == peer
    assert link["metrics"]["rssi_avg"]["delta"] == 6.0


def test_link_signal_history_registered_in_mcp_catalog():
    names = {tool["name"] for tool in mcp_tools.TOOL_DEFS}
    assert "get_node_link_signal_history" in names
    assert "get_node_link_signal_history" in mcp_tools._READ_TOOLS


def test_link_signal_history_http_endpoint_returns_links(store):
    base = datetime.now(tz=UTC) - timedelta(minutes=30)
    reporter = "aa" * 8
    peer = "bb" * 8
    store.upsert_node_metadata(eui64=reporter)
    store.upsert_node_metadata(eui64=peer)
    store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [{"neighbor_eui64": peer, "rssi_avg": -72, "rssi_last": -73, "lqi_in": 175, "is_child": False}],
        observed_at=base.isoformat(),
    )

    client = TestClient(create_core_app())
    response = client.get(f"/v1/signals/{reporter}/links/history")

    assert response.status_code == 200
    body = response.json()
    assert body["link_count"] == 1
    assert body["links"][0]["peer_eui64"] == peer