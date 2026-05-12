"""Tests for the server-side route walker and neighbors enricher."""

from __future__ import annotations

from thread_observability.pipeline import routing
from thread_observability.pipeline.nodes import list_nodes_enriched  # noqa: F401

# Reuse the topology setup from test_nodes.
from .test_nodes import _setup_three_router_partition  # type: ignore[import-not-found]


def test_walk_route_to_otbr_direct(store) -> None:
    otbr, rb, _ = _setup_three_router_partition(store)
    result = routing.walk_route_to_otbr(rb, store=store)
    assert result["complete"] is True
    assert result["otbr_eui64"] == otbr
    assert result["issues"] == []
    assert result["hop_count"] == 2
    assert [h["eui64"] for h in result["hops"]] == [rb, otbr]
    assert result["hops"][1]["is_otbr"] is True
    assert result["hops"][1]["path_cost"] == 1


def test_walk_route_to_otbr_multihop(store) -> None:
    otbr, rb, rc = _setup_three_router_partition(store)
    result = routing.walk_route_to_otbr(rc, store=store)
    assert result["complete"] is True
    assert result["hop_count"] == 3
    assert [h["eui64"] for h in result["hops"]] == [rc, rb, otbr]
    # Last hop is the OTBR; the middle hop is Router B with path_cost=2
    # (the route_table row from rc → otbr reports total cost 2).
    assert result["hops"][1]["eui64"] == rb
    assert result["hops"][2]["is_otbr"] is True


def test_walk_route_no_otbr(store) -> None:
    # Empty store has no OTBR.
    result = routing.walk_route_to_otbr("aa" * 8, store=store)
    assert result["complete"] is False
    assert result["otbr_eui64"] is None
    assert any(i["code"] == "no_otbr" for i in result["issues"])


def test_walk_route_self_is_otbr(store) -> None:
    otbr, _, _ = _setup_three_router_partition(store)
    result = routing.walk_route_to_otbr(otbr, store=store)
    assert result["complete"] is True
    assert result["hop_count"] == 1
    assert result["hops"][0]["is_otbr"] is True
    assert any(i["code"] == "self_is_otbr" for i in result["issues"])


def test_find_otbr(store) -> None:
    otbr, _, _ = _setup_three_router_partition(store)
    found = routing.find_otbr(store=store)
    assert found is not None
    assert found["eui64"] == otbr


def test_list_neighbors_enriched(store) -> None:
    otbr, rb, rc = _setup_three_router_partition(store)
    # Add a neighbor_table entry so the neighbors list is non-empty.
    store.replace_links_for_reporter(rb, "neighbor_table", [
        {"neighbor_eui64": otbr, "rssi_avg": -55, "lqi_in": 240, "is_child": False,
         "rx_on_when_idle": True, "full_thread_device": True, "full_network_data": True},
        {"neighbor_eui64": rc, "rssi_avg": -70, "lqi_in": 180, "is_child": False},
    ])
    out = routing.list_neighbors_enriched(rb, store=store)
    assert out["reporter_eui64"] == rb
    assert out["reporter_name"] == "Router B"
    assert out["neighbor_count"] == 2
    assert out["route_count"] == 3
    # OTBR neighbor should be enriched with its friendly name.
    otbr_row = next(n for n in out["neighbors"] if n["neighbor_eui64"] == otbr)
    assert otbr_row["name"] == "HA Yellow OTBR"
    assert otbr_row["rx_on_when_idle"] == 1
    assert otbr_row["full_thread_device"] == 1
    # Route to rc: next_hop_router_id=12 (rc itself) → next_hop_eui64=rc.
    rc_route = next(r for r in out["routes"] if r["neighbor_eui64"] == rc)
    assert rc_route["next_hop_router_id"] == 12
    assert rc_route["next_hop_eui64"] == rc
    assert rc_route["next_hop_name"] == "Router C"


def test_topology_edge_class(store) -> None:
    """`/v1/topology` links must come back with `edge_class` populated."""
    from thread_observability.pipeline import topology

    otbr, rb, rc = _setup_three_router_partition(store)
    # Add a neighbor_table row both directions so the dedup kicks in.
    store.replace_links_for_reporter(rb, "neighbor_table", [
        {"neighbor_eui64": rc, "rssi_avg": -70, "is_child": False},
    ])
    store.replace_links_for_reporter(rc, "neighbor_table", [
        {"neighbor_eui64": rb, "rssi_avg": -72, "is_child": False},
    ])
    snap = topology.build_topology(store=store)
    classes = {ln["edge_class"] for ln in snap["links"]}
    assert "peer" in classes  # rb<->rc collapsed to one peer edge
    assert "route" in classes  # route_table entries
    # Only one peer edge for the rb/rc pair.
    peer_edges = [ln for ln in snap["links"] if ln["edge_class"] == "peer"]
    assert len(peer_edges) == 1


# ---------------------------------------------------------------------------
# 0.9.38: phantom-loop fix. OpenThread fills NextHopRouterId on RouteTable
# rows even when the reporter has a direct MLE link to the destination, so
# two routers that both directly reach the OTBR can appear to "route through
# each other" — a visual A↔B↔A loop that doesn't exist on the wire.
# PathCost=1 + LinkEstablished=1 is the authoritative direct-link signal.
# ---------------------------------------------------------------------------

def _setup_phantom_loop_pair(store) -> tuple[str, str, str]:
    """Two routers both with direct OTBR links but cross-pointing NextHops.

    Mirrors the live Office Light (rid=7) + Downstairs Hallway (rid=58)
    case. Both routers report ``path_cost=1, link_established=1`` to the
    OTBR — they're using direct links — yet each names the other as
    ``next_hop_router_id``. A naive walker follows the next-hop chain
    and produces Office → Hallway → Office → ... (loop_detected). The
    short-circuit must collapse both rows to direct hops.
    """
    otbr = "aaaaaaaaaaaaaaaa"
    office = "7777777777777777"   # router_id 7
    hall = "5858585858585858"     # router_id 58
    partition = 0xDEADBEEF
    store.upsert_node_metadata(eui64=otbr, friendly_name="OTBR", role="border_router")
    store.upsert_node_metadata(eui64=office, friendly_name="Office Light")
    store.upsert_node_metadata(eui64=hall, friendly_name="Downstairs Hallway Lights")
    store.set_node_diagnostics(otbr, partition_id=partition, routing_role="leader")
    store.set_node_diagnostics(office, partition_id=partition, routing_role="router")
    store.set_node_diagnostics(hall, partition_id=partition, routing_role="router")
    store.set_node_router_id(otbr, 19)
    store.set_node_router_id(office, 7)
    store.set_node_router_id(hall, 58)
    # Office: direct link to OTBR (path_cost=1, link_established=1) but
    # next_hop_router_id names Hallway. This is the OpenThread quirk.
    store.replace_links_for_reporter(office, "route_table", [
        {"neighbor_eui64": otbr, "path_cost": 1, "next_hop_router_id": 58,
         "link_established": 1, "lqi_in": 3, "lqi_out": 3, "allocated": 1},
        {"neighbor_eui64": hall, "path_cost": 1, "next_hop_router_id": 58,
         "link_established": 1, "lqi_in": 3, "lqi_out": 3, "allocated": 1},
    ])
    # Hallway: same story, cross-pointing back.
    store.replace_links_for_reporter(hall, "route_table", [
        {"neighbor_eui64": otbr, "path_cost": 1, "next_hop_router_id": 7,
         "link_established": 1, "lqi_in": 3, "lqi_out": 3, "allocated": 1},
        {"neighbor_eui64": office, "path_cost": 1, "next_hop_router_id": 7,
         "link_established": 1, "lqi_in": 3, "lqi_out": 3, "allocated": 1},
    ])
    return otbr, office, hall


def test_walk_route_direct_link_short_circuit(store) -> None:
    """path_cost=1 + link_established=1 must override next_hop_router_id."""
    otbr, office, hall = _setup_phantom_loop_pair(store)

    office_walk = routing.walk_route_to_otbr(office, store=store)
    assert office_walk["complete"] is True
    assert office_walk["hop_count"] == 2
    assert [h["eui64"] for h in office_walk["hops"]] == [office, otbr]
    # No loop_detected — the bug we're fixing.
    assert all(i["code"] != "loop_detected" for i in office_walk["issues"])

    hall_walk = routing.walk_route_to_otbr(hall, store=store)
    assert hall_walk["complete"] is True
    assert hall_walk["hop_count"] == 2
    assert [h["eui64"] for h in hall_walk["hops"]] == [hall, otbr]
    assert all(i["code"] != "loop_detected" for i in hall_walk["issues"])


def test_list_neighbors_effective_next_hop_direct(store) -> None:
    """Route rows must expose `effective_next_hop_eui64` resolving to dest."""
    otbr, office, _ = _setup_phantom_loop_pair(store)
    out = routing.list_neighbors_enriched(office, store=store)
    otbr_route = next(r for r in out["routes"] if r["neighbor_eui64"] == otbr)
    # Raw fields preserved for diagnostics.
    assert otbr_route["next_hop_router_id"] == 58
    # Derived field resolves to the destination because direct link is in use.
    assert otbr_route["effective_next_hop_eui64"] == otbr
    assert otbr_route["effective_next_hop_name"] == "OTBR"
    assert otbr_route["is_direct_link"] is True


def test_walk_route_genuine_multihop_unchanged(store) -> None:
    """Path cost > 1 still follows next_hop_router_id (no regression)."""
    # The three-router fixture has rc → otbr with path_cost=2 and
    # link_established unset (None). The short-circuit must NOT fire.
    _, rb, rc = _setup_three_router_partition(store)
    result = routing.walk_route_to_otbr(rc, store=store)
    assert result["complete"] is True
    assert result["hop_count"] == 3
    assert result["hops"][1]["eui64"] == rb


# ---------------------------------------------------------------------------
# 0.9.38: OTBR REST neighbor + router decoders. Lets the border router be
# a first-class reporter in the links table instead of a destination only.
# ---------------------------------------------------------------------------

def test_decode_otbr_neighbors_maps_fields() -> None:
    from thread_observability.pipeline.otbr_rest import _decode_otbr_neighbors

    raw = [{
        "ExtAddress": "0x267BA12E239DB320",
        "Age": 12,
        "Rloc16": "0x1c00",
        "LinkQualityIn": 3,
        "LinkQualityOut": 3,
        "AverageRssi": -55,
        "LastRssi": -53,
        "FrameErrorRate": 2,
        "MessageErrorRate": 0,
        "IsChild": False,
        "RxOnWhenIdle": True,
        "FullThreadDevice": True,
        "FullNetworkData": True,
        "LinkFrameCounter": 9876,
        "MleFrameCounter": 4321,
    }]
    out = _decode_otbr_neighbors(raw)
    assert len(out) == 1
    row = out[0]
    assert row["neighbor_eui64"] == "267ba12e239db320"
    assert row["rssi_avg"] == -55
    assert row["rssi_last"] == -53
    assert row["lqi_in"] == 3
    assert row["lqi_out"] == 3
    assert row["is_child"] == 0
    assert row["rx_on_when_idle"] == 1
    assert row["full_thread_device"] == 1
    assert row["link_frame_counter"] == 9876
    assert row["mle_frame_counter"] == 4321


def test_decode_otbr_routers_maps_fields() -> None:
    from thread_observability.pipeline.otbr_rest import _decode_otbr_routers

    raw = [{
        "ExtAddress": "0x267BA12E239DB320",
        "Rloc16": "0x1c00",
        "RouterId": 7,
        "NextHop": 58,
        "PathCost": 1,
        "LinkQualityIn": 3,
        "LinkQualityOut": 3,
        "Age": 4,
        "Allocated": True,
        "LinkEstablished": True,
    }]
    out = _decode_otbr_routers(raw)
    assert len(out) == 1
    row = out[0]
    assert row["neighbor_eui64"] == "267ba12e239db320"
    assert row["router_id"] == 7
    assert row["next_hop_router_id"] == 58
    assert row["path_cost"] == 1
    assert row["lqi_in"] == 3
    assert row["lqi_out"] == 3
    assert row["allocated"] == 1
    assert row["link_established"] == 1


def test_decode_otbr_skips_missing_eui() -> None:
    from thread_observability.pipeline.otbr_rest import (
        _decode_otbr_neighbors,
        _decode_otbr_routers,
    )

    assert _decode_otbr_neighbors([{"Age": 5}]) == []
    assert _decode_otbr_routers([{"PathCost": 2}]) == []
    assert _decode_otbr_neighbors(None) == []
    assert _decode_otbr_routers(None) == []
