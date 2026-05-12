"""Contract tests for ``/v1/routes/{eui64}``.

These lock down the server-side route walker's public shape. The
dashboard previously reconstructed this in JavaScript; MCP / AI
consumers depend on the wire format now.
"""

from __future__ import annotations

from thread_observability.api.schemas import (
    ROUTE_ISSUE_CODES,
    RouteWalkResponse,
)
from ..test_nodes import _setup_three_router_partition  # type: ignore[import-not-found]


def test_routes_no_otbr_contract(client) -> None:
    """Empty store has no OTBR → ``complete=False`` with a structured issue."""
    r = client.get("/v1/routes/" + "aa" * 8)
    assert r.status_code == 200
    body = RouteWalkResponse.model_validate(r.json())
    assert body.complete is False
    assert body.otbr_eui64 is None
    assert any(i.code == "no_otbr" for i in body.issues)
    # Every issue code must be one we recognise (forward-compat allows
    # additions to ROUTE_ISSUE_CODES but never silent typos).
    for i in body.issues:
        assert i.code in ROUTE_ISSUE_CODES, f"unknown issue code: {i.code!r}"


def test_routes_direct_contract(client, store) -> None:
    """Router B → OTBR is a 2-hop direct chain."""
    otbr, rb, _rc = _setup_three_router_partition(store)
    r = client.get(f"/v1/routes/{rb}")
    assert r.status_code == 200
    body = RouteWalkResponse.model_validate(r.json())
    assert body.complete is True
    assert body.otbr_eui64 == otbr
    assert body.hop_count == 2
    assert body.hops[0].eui64 == rb
    assert body.hops[-1].eui64 == otbr
    assert body.hops[-1].is_otbr is True


def test_routes_multihop_contract(client, store) -> None:
    """Router C → OTBR forwards through Router B."""
    otbr, rb, rc = _setup_three_router_partition(store)
    r = client.get(f"/v1/routes/{rc}")
    assert r.status_code == 200
    body = RouteWalkResponse.model_validate(r.json())
    assert body.complete is True
    assert body.hop_count == 3
    assert [h.eui64 for h in body.hops] == [rc, rb, otbr]


def test_routes_self_is_otbr_contract(client, store) -> None:
    """Asking for the OTBR's own path returns a single-hop chain with a hint."""
    otbr, _rb, _rc = _setup_three_router_partition(store)
    r = client.get(f"/v1/routes/{otbr}")
    assert r.status_code == 200
    body = RouteWalkResponse.model_validate(r.json())
    assert body.complete is True
    assert body.hop_count == 1
    assert any(i.code == "self_is_otbr" for i in body.issues)


def test_routes_path_param_is_normalised(client, store) -> None:
    """EUI64s arrive lowercased to the store regardless of URL casing."""
    otbr, rb, _ = _setup_three_router_partition(store)
    r = client.get(f"/v1/routes/{rb.upper()}")
    assert r.status_code == 200
    body = RouteWalkResponse.model_validate(r.json())
    # Server must resolve the route regardless of casing in the URL.
    assert body.complete is True
    assert body.otbr_eui64 == otbr
