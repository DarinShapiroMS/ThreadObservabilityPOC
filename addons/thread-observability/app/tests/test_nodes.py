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
    # 0.9.31: status column defaults to 'online' on upsert; previously the
    # `infer_node_status` heuristic returned 'healthy'.
    assert summary["status"] == "online"
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


def test_list_nodes_enriched_collapses_duplicate_hardware_rows(store) -> None:
    stale_eui = "11" * 8
    live_eui = "22" * 8

    store.upsert_node_metadata(
        eui64=stale_eui,
        friendly_name="Hall Sensor",
        device_id="old-device",
        vendor_id=123,
        product_id=456,
        serial_number="SN-1",
    )
    store.upsert_node_metadata(
        eui64=live_eui,
        friendly_name="Hall Sensor",
        device_id="live-device",
        vendor_id=123,
        product_id=456,
        serial_number="SN-1",
    )
    store.set_node_diagnostics(stale_eui, partition_id=1111, routing_role="router")
    store.set_node_diagnostics(live_eui, partition_id=2222, routing_role="router")
    store.apply_availability([
        (stale_eui, False, "ha_entity"),
        (live_eui, True, "ha_entity"),
    ])
    store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)

    enriched = nodes.list_nodes_enriched(store=store)

    assert len(enriched) == 1
    assert enriched[0]["eui64"] == live_eui
    assert enriched[0]["partition_id"] == 2222
    assert enriched[0]["suppressed_duplicate_euis"] == [stale_eui]


def test_get_latest_signal_strength(store) -> None:
    store.insert_event(eui64="aabbccddeeff0011", type="parent_response", rssi=-65, lqi=200)
    store.insert_event(eui64="aabbccddeeff0011", type="parent_response", rssi=-70, lqi=180)

    strength = nodes.get_latest_signal_strength("aabbccddeeff0011", store=store)
    # Latest is the first one retrieved (events ordered DESC by default)
    assert strength["rssi"] == -70
    assert strength["lqi"] == 180
    assert strength["rssi_avg"] in (-67, -68)  # average of -65 and -70
    assert strength["lqi_avg"] == 190  # average of 200 and 180


def test_get_latest_signal_strength_falls_back_to_reported_router_links(store) -> None:
    router = "aa" * 8
    peer_a = "bb" * 8
    peer_b = "cc" * 8
    store.upsert_node_metadata(eui64=router, friendly_name="Router A")
    store.upsert_node_metadata(eui64=peer_a, friendly_name="Peer B")
    store.upsert_node_metadata(eui64=peer_b, friendly_name="Peer C")
    store.replace_links_for_reporter(router, "neighbor_table", [
        {"neighbor_eui64": peer_a, "rssi_avg": -71, "lqi_out": 3, "is_child": False},
        {"neighbor_eui64": peer_b, "rssi_avg": -64, "lqi_out": 2, "is_child": False},
    ])

    strength = nodes.get_latest_signal_strength(router, store=store)

    assert strength["source"] == "reported_links"
    assert strength["rssi"] == -64
    assert strength["lqi"] == 3
    assert strength["best_reporter"] is not None
    assert strength["best_reporter"]["eui64"] == peer_b
    assert strength["best_reporter"]["name"] == "Peer C"
    assert [row["eui64"] for row in strength["neighbors"]] == [peer_b, peer_a]


def _setup_three_router_partition(store) -> tuple[str, str, str]:
    """Set up an OTBR + two routers in one partition with route-table links.

    Topology: OTBR (router_id=1) ←direct→ Router B (router_id=5) ←→ Router C
    (router_id=12). Router C's only path to the OTBR is *through* Router B.
    Returns ``(otbr_eui, b_eui, c_eui)``.
    """
    otbr = "aaaaaaaaaaaaaaaa"
    rb = "bbbbbbbbbbbbbbbb"
    rc = "cccccccccccccccc"
    partition = 0xABCDEF01

    store.upsert_node_metadata(eui64=otbr, friendly_name="HA Yellow OTBR", role="border_router")
    store.upsert_node_metadata(eui64=rb, friendly_name="Router B")
    store.upsert_node_metadata(eui64=rc, friendly_name="Router C")

    store.set_node_diagnostics(otbr, partition_id=partition, routing_role="leader")
    store.set_node_diagnostics(rb, partition_id=partition, routing_role="router")
    store.set_node_diagnostics(rc, partition_id=partition, routing_role="router")

    store.set_node_router_id(otbr, 1)
    store.set_node_router_id(rb, 5)
    store.set_node_router_id(rc, 12)

    # Router B has the OTBR as a direct neighbor (next_hop_router_id == OTBR's id).
    store.replace_links_for_reporter(rb, "route_table", [
        {"neighbor_eui64": otbr, "path_cost": 1, "next_hop_router_id": 1, "router_id": 1},
        {"neighbor_eui64": rb,   "path_cost": 0, "next_hop_router_id": 5, "router_id": 5},
        {"neighbor_eui64": rc,   "path_cost": 1, "next_hop_router_id": 12, "router_id": 12},
    ])
    # Router C must forward through Router B to reach the OTBR.
    store.replace_links_for_reporter(rc, "route_table", [
        {"neighbor_eui64": otbr, "path_cost": 2, "next_hop_router_id": 5, "router_id": 1},
        {"neighbor_eui64": rb,   "path_cost": 1, "next_hop_router_id": 5, "router_id": 5},
        {"neighbor_eui64": rc,   "path_cost": 0, "next_hop_router_id": 12, "router_id": 12},
    ])
    return otbr, rb, rc


def test_next_hop_to_otbr_direct_neighbor(store) -> None:
    otbr, rb, _ = _setup_three_router_partition(store)
    enriched = {n["eui64"]: n for n in nodes.list_nodes_enriched(store=store)}
    hop = enriched[rb]["next_hop_to_otbr"]
    assert hop is not None
    assert hop["is_direct"] is True
    assert hop["eui64"] == otbr
    assert hop["name"] == "HA Yellow OTBR"
    assert hop["path_cost"] == 1


def test_next_hop_to_otbr_multi_hop(store) -> None:
    _, rb, rc = _setup_three_router_partition(store)
    enriched = {n["eui64"]: n for n in nodes.list_nodes_enriched(store=store)}
    hop = enriched[rc]["next_hop_to_otbr"]
    assert hop is not None
    assert hop["is_direct"] is False
    # Router C forwards through Router B (router_id=5).
    assert hop["eui64"] == rb
    assert hop["name"] == "Router B"
    assert hop["router_id"] == 5
    assert hop["path_cost"] == 2


def test_next_hop_to_otbr_absent_when_no_border_router(store) -> None:
    # Two routers, no border_router role anywhere → next-hop view is N/A.
    store.upsert_node_metadata(eui64="1111111111111111", friendly_name="R1")
    store.upsert_node_metadata(eui64="2222222222222222", friendly_name="R2")
    store.set_node_diagnostics("1111111111111111", routing_role="router", partition_id=1)
    store.set_node_diagnostics("2222222222222222", routing_role="router", partition_id=1)

    enriched = nodes.list_nodes_enriched(store=store)
    for n in enriched:
        assert n["next_hop_to_otbr"] is None


def test_otbr_node_itself_has_no_next_hop(store) -> None:
    otbr, _, _ = _setup_three_router_partition(store)
    enriched = {n["eui64"]: n for n in nodes.list_nodes_enriched(store=store)}
    # The OTBR doesn't forward to itself.
    assert enriched[otbr]["next_hop_to_otbr"] is None

