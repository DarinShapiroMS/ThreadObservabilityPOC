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


def test_partitions_contract_collapses_recommissioned_alias_rows(client, store) -> None:
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
    store.set_node_diagnostics(live_eui, partition_id=2222, routing_role="leader")
    store.apply_availability([
        (stale_eui, False, "ha_entity"),
        (live_eui, True, "ha_entity"),
    ])
    store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)

    r = client.get("/v1/partitions")
    assert r.status_code == 200
    body = PartitionsResponse.model_validate(r.json())
    assert body.partition_count == 1
    assert body.split is False
    assert len(body.partitions) == 1
    assert body.partitions[0].partition_id == 2222
    assert body.partitions[0].leader_eui64 == live_eui
    assert body.partitions[0].members == [live_eui]


def test_issues_active_contract(client) -> None:
    r = client.get("/v1/issues/active")
    assert r.status_code == 200
    body = IssuesResponse.model_validate(r.json())
    assert body.count == len(body.issues)
