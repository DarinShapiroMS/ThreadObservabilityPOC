"""Parametrized tests over JSON-driven mesh scenarios.

Every file in ``fixtures/*.json`` describes a mesh shape and the
assertions that must hold for it. Adding a new mesh quirk is a new
fixture file, not new test code.

Run a single scenario with::

    pytest tests/scenarios -k single_otbr_three_routers
"""

from __future__ import annotations

from pathlib import Path

import pytest

from thread_observability.api.schemas import (
    DevStatusResponse,
    PartitionsResponse,
    RouteWalkResponse,
    TopologyResponse,
)

from .loader import discover_fixtures, load_scenario, seed_store


def _ids(paths: list[Path]) -> list[str]:
    return [p.stem for p in paths]


@pytest.fixture(params=discover_fixtures(), ids=_ids(discover_fixtures()))
def scenario(request, store):
    """Load a scenario JSON and seed the store with it.

    Yields ``(scenario_dict, store)`` so individual tests can both read
    the expectations and query the database directly.
    """
    path: Path = request.param
    data = load_scenario(path)
    seed_store(store, data)
    return data


def test_scenario_topology(scenario, client) -> None:
    expect = (scenario.get("expectations") or {}).get("topology")
    if not expect:
        pytest.skip("scenario has no topology expectations")
    r = client.get("/v1/topology")
    assert r.status_code == 200
    body = TopologyResponse.model_validate(r.json())
    if "node_count" in expect:
        assert body.node_count == expect["node_count"], scenario["name"]
    if "link_count" in expect:
        assert body.link_count == expect["link_count"], scenario["name"]
    if "split" in expect:
        assert body.split is expect["split"], scenario["name"]


def test_scenario_partitions(scenario, client) -> None:
    expect = (scenario.get("expectations") or {}).get("partitions")
    if not expect:
        pytest.skip("scenario has no partition expectations")
    r = client.get("/v1/partitions")
    assert r.status_code == 200
    body = PartitionsResponse.model_validate(r.json())
    if "partition_count" in expect:
        assert body.partition_count == expect["partition_count"], scenario["name"]
    if "split" in expect:
        assert body.split is expect["split"], scenario["name"]


def test_scenario_routes(scenario, client) -> None:
    expectations = (scenario.get("expectations") or {}).get("routes") or []
    if not expectations:
        pytest.skip("scenario has no route expectations")
    for spec in expectations:
        r = client.get(f"/v1/routes/{spec['source']}")
        assert r.status_code == 200
        body = RouteWalkResponse.model_validate(r.json())
        ctx = f"{scenario['name']} :: source={spec['source']}"
        if "complete" in spec:
            assert body.complete is spec["complete"], ctx
        if "hop_count" in spec:
            assert body.hop_count == spec["hop_count"], ctx
        for code in spec.get("issue_codes") or []:
            assert any(i.code == code for i in body.issues), (
                f"{ctx}: expected issue code {code!r}, got "
                f"{[i.code for i in body.issues]}"
            )


def test_scenario_dev_status(scenario, client) -> None:
    expect = (scenario.get("expectations") or {}).get("dev_status")
    if not expect:
        pytest.skip("scenario has no dev_status expectations")
    r = client.get("/v1/dev/status")
    assert r.status_code == 200
    body = DevStatusResponse.model_validate(r.json())
    if "otbr_eui64" in expect:
        assert body.otbr_eui64 == expect["otbr_eui64"], scenario["name"]
    if "partition_summary" in expect:
        assert body.partitions.summary == expect["partition_summary"], scenario["name"]
    if "node_counts" in expect:
        for k, v in expect["node_counts"].items():
            assert getattr(body.node_counts, k) == v, f"{scenario['name']} :: {k}"
