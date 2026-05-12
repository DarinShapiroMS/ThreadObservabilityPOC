"""Contract tests for ``/v1/topology``.

These assert the wire shape every consumer (dashboard, MCP, AI) sees.
The graph rendering layer must be able to trust ``edge_class`` and the
node list without re-deriving them.
"""

from __future__ import annotations

from thread_observability.api.schemas import TopologyResponse
from ..test_nodes import _setup_three_router_partition  # type: ignore[import-not-found]


def test_topology_empty_contract(client) -> None:
    """With no nodes, topology must still be a valid response (not a stub)."""
    r = client.get("/v1/topology")
    assert r.status_code == 200
    body = TopologyResponse.model_validate(r.json())
    assert body.node_count == 0
    assert body.link_count == 0
    assert body.split is False


def test_topology_three_routers_contract(client, store) -> None:
    _setup_three_router_partition(store)
    r = client.get("/v1/topology")
    assert r.status_code == 200
    body = TopologyResponse.model_validate(r.json())
    assert body.node_count == 3
    # Every link must declare an ``edge_class`` from the allowed set.
    allowed = {"peer", "child", "route", "other"}
    for ln in body.links:
        assert ln.edge_class in allowed, f"unexpected edge_class={ln.edge_class!r}"
    # At least one route_table edge (router B reports route to router C).
    assert any(ln.edge_class == "route" for ln in body.links)


def test_topology_include_phantoms_query(client, store) -> None:
    """The ``include_phantoms`` query param must not break the contract."""
    _setup_three_router_partition(store)
    r = client.get("/v1/topology", params={"include_phantoms": "true"})
    assert r.status_code == 200
    TopologyResponse.model_validate(r.json())
