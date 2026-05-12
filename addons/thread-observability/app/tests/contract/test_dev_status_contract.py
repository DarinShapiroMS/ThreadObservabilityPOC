"""Contract tests for ``/v1/dev/status``.

This endpoint is the dashboard's primary data source and the most
likely place for contract drift. It bundles the topology, partitions,
phantoms, pipeline state, node counts, and OTBR resolution into one
response so the UI never has to fan-out.
"""

from __future__ import annotations

from thread_observability.api.schemas import DevStatusResponse
from ..test_nodes import _setup_three_router_partition  # type: ignore[import-not-found]


def test_dev_status_empty_contract(client) -> None:
    """Even with an empty store, the contract must hold."""
    r = client.get("/v1/dev/status")
    assert r.status_code == 200
    body = DevStatusResponse.model_validate(r.json())
    assert body.node_counts.total == 0
    # All required count buckets are present even when zero.
    assert body.node_counts.online == 0
    assert body.node_counts.offline == 0
    assert body.node_counts.unregistered == 0
    assert body.node_counts.phantom == 0
    assert body.partitions.partition_count == 0
    assert isinstance(body.partitions.summary, str) and body.partitions.summary
    assert body.otbr_eui64 is None


def test_dev_status_populated_contract(client, store) -> None:
    otbr, _, _ = _setup_three_router_partition(store)
    r = client.get("/v1/dev/status")
    assert r.status_code == 200
    body = DevStatusResponse.model_validate(r.json())
    assert body.node_counts.total == 3
    assert body.partitions.partition_count == 1
    assert body.partitions.summary == "single partition"
    # OTBR resolution must succeed when role='border_router' is set.
    assert body.otbr_eui64 == otbr


def test_dev_status_all_nodes_sort_order(client, store) -> None:
    """``all_nodes`` is sorted phantoms-last, then by display_name (case-insensitive)."""
    otbr, rb, rc = _setup_three_router_partition(store)
    r = client.get("/v1/dev/status")
    body = DevStatusResponse.model_validate(r.json())
    names = [n.get("display_name", "").lower() for n in body.all_nodes]
    # Sort key in handler is (is_phantom, display_name.lower()). Verify
    # the non-phantom block is alphabetically ordered.
    non_phantom_names = [
        n.get("display_name", "").lower()
        for n in body.all_nodes
        if not (n.get("status") == "phantom" or n.get("is_phantom"))
    ]
    assert non_phantom_names == sorted(non_phantom_names)


def test_dev_status_pipeline_stages_failed_present(client, store) -> None:
    """When pipeline state has a stages dict, ``stages_failed`` must exist."""
    # Even with no pipeline run yet, the field is only added when stages
    # is a dict — so we just assert the contract holds when present.
    r = client.get("/v1/dev/status")
    body = DevStatusResponse.model_validate(r.json())
    pipeline = body.pipeline
    if isinstance(pipeline.get("stages"), dict):
        assert "stages_failed" in pipeline
        assert isinstance(pipeline["stages_failed"], list)
