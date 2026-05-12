"""Property-based tests for the server-side route walker.

Hand-written tests cover a fixed set of mesh shapes; these tests
generate random partitions and assert *invariants* that must hold for
every possible input:

* The walker always terminates (cycle detection works).
* The returned hop list is acyclic.
* If a chain reaches the OTBR, ``complete=True``; otherwise the
  ``issues`` list contains a structured reason.
* Source EUI ⇒ first hop's EUI (the chain starts where we asked).

These are the kinds of properties that catch off-by-one and edge-case
bugs that hand-written tests miss.
"""

from __future__ import annotations

import string

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from thread_observability.api.schemas import ROUTE_ISSUE_CODES
from thread_observability.pipeline import routing


def _eui_for(i: int) -> str:
    """Stable EUI64 for router index ``i``. 16 hex chars, deterministic."""
    return f"{i:016x}"


# ----------------------------- Strategies -----------------------------------

# A "partition shape" is: (n_routers, next_hop_choices).
# - n_routers: 2..8 routers (router 0 is always the OTBR).
# - next_hop_choices[i]: for router i>=1, which router (by index) it forwards
#   to in order to reach the OTBR. May point to any router (including
#   itself, creating an unreachable / loop case the walker must handle).


@st.composite
def partition_shape(draw):
    n = draw(st.integers(min_value=2, max_value=8))
    # Each non-OTBR router picks a next-hop index in [0, n). Index 0 is the
    # OTBR (the "good" choice). Anything else may or may not reach.
    next_hops = [draw(st.integers(min_value=0, max_value=n - 1)) for _ in range(n - 1)]
    return n, next_hops


def _seed_partition(store, n: int, next_hops: list[int]) -> list[str]:
    """Build a partition of ``n`` routers in ``store``. Returns EUI list.

    Wipes the store first so successive Hypothesis examples don't leak
    leftover routers / links into each other.
    """
    store.reset_data()
    pid = 0xC0FFEE_01
    euis = [_eui_for(i) for i in range(n)]
    # OTBR.
    store.upsert_node_metadata(eui64=euis[0], friendly_name="OTBR", role="border_router")
    store.set_node_diagnostics(euis[0], partition_id=pid, routing_role="leader")
    store.set_node_router_id(euis[0], 1)
    # Other routers.
    for i in range(1, n):
        store.upsert_node_metadata(eui64=euis[i], friendly_name=f"R{i}", role="router")
        store.set_node_diagnostics(euis[i], partition_id=pid, routing_role="router")
        store.set_node_router_id(euis[i], i + 1)  # router_ids: OTBR=1, R1=2, R2=3, ...
    # Build route_table entries: every router has one row pointing to OTBR
    # via its chosen next hop. Mirror the test_nodes fixture pattern.
    for i in range(1, n):
        nh_idx = next_hops[i - 1]
        nh_router_id = (nh_idx + 1) if nh_idx > 0 else 1
        store.replace_links_for_reporter(
            euis[i], "route_table",
            [{
                "neighbor_eui64": euis[0],
                "path_cost": 1 if nh_idx == 0 else 2,
                "next_hop_router_id": nh_router_id,
                "router_id": 1,
                "link_established": nh_idx == 0,
            }],
            partition_id=pid,
        )
    return euis


# ------------------------------- Tests --------------------------------------


@settings(max_examples=50, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(shape=partition_shape())
def test_walker_always_terminates(store, shape) -> None:
    """No matter the next-hop graph, the walker must return in finite time.

    A loop or unreachable chain must yield ``complete=False`` with a
    structured issue, not an infinite loop or a stack overflow.
    """
    n, next_hops = shape
    euis = _seed_partition(store, n, next_hops)
    # Walk from every non-OTBR router; just exercising the code paths is
    # enough — if the walker hangs, the test times out.
    for source in euis[1:]:
        result = routing.walk_route_to_otbr(source, store=store)
        # Bounded hop list.
        assert len(result["hops"]) <= routing.MAX_HOPS + 1
        # Issue codes must be from the known set.
        for issue in result["issues"]:
            assert issue["code"] in ROUTE_ISSUE_CODES


@settings(max_examples=50, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(shape=partition_shape())
def test_walker_hops_are_acyclic(store, shape) -> None:
    """The returned hop chain must never repeat an EUI (cycle detection)."""
    n, next_hops = shape
    euis = _seed_partition(store, n, next_hops)
    for source in euis[1:]:
        result = routing.walk_route_to_otbr(source, store=store)
        seen = [h["eui64"] for h in result["hops"]]
        assert len(seen) == len(set(seen)), (
            f"duplicate hop in chain from {source}: {seen}"
        )


@settings(max_examples=50, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(shape=partition_shape())
def test_walker_chain_starts_at_source(store, shape) -> None:
    """The first hop in the returned chain is always the source EUI."""
    n, next_hops = shape
    euis = _seed_partition(store, n, next_hops)
    for source in euis[1:]:
        result = routing.walk_route_to_otbr(source, store=store)
        if result["hops"]:
            assert result["hops"][0]["eui64"] == source


@settings(max_examples=50, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(shape=partition_shape())
def test_walker_completeness_iff_last_hop_is_otbr(store, shape) -> None:
    """``complete=True`` iff the final hop is the OTBR."""
    n, next_hops = shape
    euis = _seed_partition(store, n, next_hops)
    otbr_eui = euis[0]
    for source in euis[1:]:
        result = routing.walk_route_to_otbr(source, store=store)
        if result["complete"]:
            assert result["hops"], "complete=True but hops is empty"
            assert result["hops"][-1]["eui64"] == otbr_eui
            assert result["hops"][-1]["is_otbr"] is True
        else:
            # Must have at least one issue describing why we didn't get there.
            assert result["issues"], (
                f"complete=False but no issues reported for {source}: {result}"
            )


@settings(max_examples=30, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(source=st.text(alphabet=string.hexdigits.lower(), min_size=16, max_size=16))
def test_walker_unknown_source_does_not_crash(store, source) -> None:
    """Asking about a totally unknown EUI returns a structured response."""
    # No nodes in the store at all.
    result = routing.walk_route_to_otbr(source, store=store)
    assert result["complete"] is False
    assert any(i["code"] == "no_otbr" for i in result["issues"])
