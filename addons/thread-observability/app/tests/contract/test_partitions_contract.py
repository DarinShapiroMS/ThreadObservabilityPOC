"""Contract tests for ``/v1/partitions`` and ``/v1/issues/active``."""

from __future__ import annotations

from thread_observability.api.schemas import (
    IssuesResponse,
    PartitionsResponse,
)
from ..test_nodes import _setup_three_router_partition  # type: ignore[import-not-found]


def test_partitions_empty_contract(client) -> None:
    r = client.get("/v1/partitions")
    assert r.status_code == 200
    body = PartitionsResponse.model_validate(r.json())
    assert body.partition_count == 0
    assert body.split is False
    assert body.partitions == []


def test_partitions_single_contract(client, store) -> None:
    _setup_three_router_partition(store)
    r = client.get("/v1/partitions")
    assert r.status_code == 200
    body = PartitionsResponse.model_validate(r.json())
    assert body.partition_count == 1
    assert body.split is False
    assert body.partitions[0].member_count == 3
    # The leader is the OTBR (set as routing_role='leader' in the fixture).
    assert body.partitions[0].leader_eui64 is not None


def test_issues_active_contract(client) -> None:
    r = client.get("/v1/issues/active")
    assert r.status_code == 200
    body = IssuesResponse.model_validate(r.json())
    assert body.count == len(body.issues)
