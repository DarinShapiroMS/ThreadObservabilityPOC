"""Pydantic response contracts for the HTTP API.

These models are the *contract* between the API and every consumer
(dashboard JS, MCP tools, AI reasoning, external scripts). They are
currently used by contract tests only — endpoint handlers still return
plain ``dict[str, object]`` for forward compatibility.

Design rules:

* ``extra="allow"``. Models declare the fields a consumer can rely on.
  Adding fields server-side is not a breaking change; removing or
  retyping a declared field is. This lets us iterate without churning
  every test on every release.
* Optional means "may be None or absent depending on whether the upstream
  source provided it". Required means "the API guarantees this field on
  every successful response".
* No business logic. These are read-only shape declarations.

If you change a model here, you are changing the public API surface.
Update the changelog accordingly.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class _Base(BaseModel):
    """Base for all response contracts.

    ``extra='allow'`` so adding optional fields on the server does not
    require updating every contract test in lockstep.
    """

    model_config = ConfigDict(extra="allow")


# -- /health, /v1/health/snapshot -------------------------------------------


class HealthResponse(_Base):
    status: str


# -- /v1/topology -----------------------------------------------------------


class TopologyNode(_Base):
    eui64: str
    friendly_name: str | None = None
    role: str | None = None
    routing_role: str | None = None
    partition_id: int | None = None
    is_phantom: bool


class TopologyLink(_Base):
    # Server uses ``from``/``to`` (matches Cytoscape and the historical
    # wire format). ``edge_class`` is one of: peer | child | route | other.
    edge_class: str
    source: str  # "neighbor_table" | "route_table"
    tags: list[str]


class TopologyResponse(_Base):
    node_count: int
    link_count: int
    split: bool
    partitions: list[dict[str, Any]]
    nodes: list[TopologyNode]
    links: list[TopologyLink]


# -- /v1/routes/{eui64} -----------------------------------------------------


class RouteHop(_Base):
    eui64: str
    name: str | None = None
    router_id: int | None = None
    path_cost: int | None = None
    lqi_in: int | None = None
    lqi_out: int | None = None
    link_established: bool | None = None
    is_otbr: bool


# Recognised issue codes. Tests assert membership rather than equality so
# we can add new codes without breaking the contract.
ROUTE_ISSUE_CODES = frozenset({
    "no_otbr",
    "no_route_to_otbr",
    "loop_detected",
    "unknown_next_hop",
    "different_partition",
    "max_hops_exceeded",
    "self_is_otbr",
})


class RouteIssue(_Base):
    code: str
    detail: str | None = None


class RouteWalkResponse(_Base):
    source_eui64: str
    otbr_eui64: str | None = None
    complete: bool
    hop_count: int
    hops: list[RouteHop]
    issues: list[RouteIssue]


# -- /v1/neighbors/{eui64} --------------------------------------------------


class NeighborRow(_Base):
    neighbor_eui64: str
    # Most other fields are optional because they come from variable
    # subsets of Matter cluster-53 attributes.


class RouteRow(_Base):
    neighbor_eui64: str
    next_hop_router_id: int | None = None
    next_hop_eui64: str | None = None
    next_hop_name: str | None = None


class NeighborsResponse(_Base):
    reporter_eui64: str
    reporter_name: str | None = None
    neighbor_count: int
    route_count: int
    neighbors: list[NeighborRow]
    routes: list[RouteRow]


# -- /v1/partitions ---------------------------------------------------------


class PartitionSummary(_Base):
    partition_id: int
    leader_eui64: str | None = None
    member_count: int
    members: list[str]


class PartitionsResponse(_Base):
    partition_count: int
    split: bool
    partitions: list[PartitionSummary]


# -- /v1/issues/active ------------------------------------------------------


class IssuesResponse(_Base):
    count: int
    issues: list[dict[str, Any]]


# -- /v1/phantoms -----------------------------------------------------------


class PhantomsResponse(_Base):
    count: int
    phantoms: list[dict[str, Any]]


# -- /v1/dev/status ---------------------------------------------------------


class NodeCounts(_Base):
    total: int
    online: int
    offline: int
    unregistered: int
    phantom: int


class DevStatusPartitions(_Base):
    """The ``partitions`` sub-object inside ``/v1/dev/status``.

    Differs from :class:`PartitionsResponse` because ``/v1/dev/status``
    enriches it with a human-readable ``summary`` field.
    """

    partition_count: int
    summary: str
    partitions: list[dict[str, Any]]


class DevStatusResponse(_Base):
    addon_version: str
    checked_at: str
    otbr_eui64: str | None = None
    node_counts: NodeCounts
    partitions: DevStatusPartitions
    all_nodes: list[dict[str, Any]]
    # ``pipeline`` shape is variable (depends on whether any tick has run)
    # so we only assert the top-level container is a dict.
    pipeline: dict[str, Any]


__all__ = [
    "HealthResponse",
    "TopologyNode",
    "TopologyLink",
    "TopologyResponse",
    "RouteHop",
    "RouteIssue",
    "RouteWalkResponse",
    "ROUTE_ISSUE_CODES",
    "NeighborRow",
    "RouteRow",
    "NeighborsResponse",
    "PartitionSummary",
    "PartitionsResponse",
    "IssuesResponse",
    "PhantomsResponse",
    "NodeCounts",
    "DevStatusPartitions",
    "DevStatusResponse",
]
