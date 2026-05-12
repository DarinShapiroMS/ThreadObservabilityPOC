"""Contract tests for ``/v1/neighbors/{eui64}``.

The endpoint returns enriched NeighborTable + RouteTable rows for one
reporter, with names resolved and next-hop RouterIds turned into
EUI64s. Consumers must not have to re-join these against the topology
or nodes tables.
"""

from __future__ import annotations

from thread_observability.api.schemas import NeighborsResponse
from ..test_nodes import _setup_three_router_partition  # type: ignore[import-not-found]


def test_neighbors_empty_reporter_contract(client) -> None:
    """An unknown EUI must return a valid, empty response — never an error."""
    r = client.get("/v1/neighbors/" + "ff" * 8)
    assert r.status_code == 200
    body = NeighborsResponse.model_validate(r.json())
    assert body.reporter_eui64 == "ff" * 8
    assert body.neighbor_count == 0
    assert body.route_count == 0
    assert body.neighbors == []
    assert body.routes == []


def test_neighbors_three_routers_contract(client, store) -> None:
    """Router B knows the OTBR directly and Router C via itself."""
    otbr, rb, rc = _setup_three_router_partition(store)
    # Seed a neighbor_table entry so neighbors[] is non-empty.
    store.replace_links_for_reporter(rb, "neighbor_table", [
        {
            "neighbor_eui64": otbr,
            "rssi_avg": -55,
            "lqi_in": 240,
            "is_child": False,
            "rx_on_when_idle": True,
            "full_thread_device": True,
        },
        {
            "neighbor_eui64": rc,
            "rssi_avg": -70,
            "lqi_in": 180,
            "is_child": False,
        },
    ])
    r = client.get(f"/v1/neighbors/{rb}")
    assert r.status_code == 200
    body = NeighborsResponse.model_validate(r.json())
    assert body.reporter_eui64 == rb
    assert body.reporter_name == "Router B"
    assert body.neighbor_count == 2
    assert body.route_count == 3
    # Routes must have next_hop_router_id and (where resolvable) next_hop_eui64.
    rc_route = next(rt for rt in body.routes if rt.neighbor_eui64 == rc)
    assert rc_route.next_hop_router_id == 12
    assert rc_route.next_hop_eui64 == rc
    assert rc_route.next_hop_name == "Router C"
