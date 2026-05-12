"""Tests for OTBR REST API ingestion."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from thread_observability.pipeline import otbr_rest
from thread_observability.storage.sqlite_store import SQLiteStore


OTBR_EUI = "e6a381123456789a"

SAMPLE_NODE_LEADER: dict[str, Any] = {
    "State": "leader",
    "ExtAddress": "e6:a3:81:12:34:56:78:9a",
    "Rloc16": 0x0400,
    "NetworkName": "OpenThreadDemo",
    "NumOfRouter": 5,
    "LeaderData": {
        "PartitionId": 0xABCDEF01,
        "LeaderRouterId": 1,
        "Weighting": 64,
        "DataVersion": 12,
        "StableDataVersion": 8,
    },
}


def _run(coro):
    return asyncio.run(coro)


def _reset_module_state() -> None:
    otbr_rest._cached_base_url = None


@pytest.fixture(autouse=True)
def _clear_state(monkeypatch: pytest.MonkeyPatch):
    _reset_module_state()
    monkeypatch.delenv("OTBR_REST_BASE_URL", raising=False)
    yield
    _reset_module_state()


def test_normalize_eui_strips_separators_and_lowercases() -> None:
    assert otbr_rest._normalize_eui("E6:A3:81:12:34:56:78:9A") == OTBR_EUI
    assert otbr_rest._normalize_eui("0xE6A381123456789A") == OTBR_EUI
    assert otbr_rest._normalize_eui(OTBR_EUI) == OTBR_EUI


def test_extract_helpers() -> None:
    eui = otbr_rest._extract_ext_address(SAMPLE_NODE_LEADER)
    assert eui == OTBR_EUI
    assert otbr_rest._extract_state(SAMPLE_NODE_LEADER) == "leader"
    pid, leader_rid, weight = otbr_rest._extract_leader_data(SAMPLE_NODE_LEADER)
    assert pid == 0xABCDEF01
    assert leader_rid == 1
    assert weight == 64


def test_candidate_urls_env_override_first(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTBR_REST_BASE_URL", "http://example.test:9999/api/")
    urls = otbr_rest._candidate_base_urls()
    assert urls[0] == "http://example.test:9999/api"
    # Defaults still appended.
    assert any("supervisor" in u for u in urls)


def test_ingest_once_persists_otbr_node(
    store: SQLiteStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen_bases: list[str] = []

    async def fake_fetch(base_url: str, *, timeout: float = 10.0) -> dict[str, Any]:
        seen_bases.append(base_url)
        # First candidate "fails", second "succeeds" — proves probe loop works.
        if "supervisor" in base_url:
            raise RuntimeError("not reachable in test")
        return SAMPLE_NODE_LEADER

    monkeypatch.setattr(otbr_rest, "fetch_otbr_node", fake_fetch)

    res = _run(otbr_rest.ingest_once(store=store))

    assert res["error"] is None
    assert res["eui64"] == OTBR_EUI
    assert res["partition_id"] == 0xABCDEF01
    assert res["leader_router_id"] == 1
    assert res["routing_role"] == "leader"
    assert res["state"] == "leader"
    assert res["active_routers"] == 5
    assert res["base_url"] in seen_bases
    # Probe tried at least 2 candidates (supervisor failed → next succeeded).
    assert len(seen_bases) >= 2

    # Node was upserted with friendly_name + role.
    n = store.get_node(OTBR_EUI)
    assert n is not None
    assert n["friendly_name"] == "Thread Border Router"
    assert n["role"] == "border_router"
    assert n["routing_role"] == "leader"
    assert n["partition_id"] == 0xABCDEF01
    assert n["leader_router_id"] == 1
    assert n["active_routers"] == 5
    assert n["last_referenced_at"] is not None


def test_ingest_once_preserves_user_renamed_friendly_name(
    store: SQLiteStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pre-seed a user-set friendly name.
    store.upsert_node_metadata(eui64=OTBR_EUI, friendly_name="My HA Yellow OTBR")

    async def fake_fetch(base_url: str, *, timeout: float = 10.0) -> dict[str, Any]:
        return SAMPLE_NODE_LEADER

    monkeypatch.setattr(otbr_rest, "fetch_otbr_node", fake_fetch)
    _run(otbr_rest.ingest_once(store=store))

    n = store.get_node(OTBR_EUI)
    assert n is not None
    # User rename preserved — we did NOT overwrite with default.
    assert n["friendly_name"] == "My HA Yellow OTBR"
    assert n["role"] == "border_router"


def test_ingest_once_reports_error_when_all_candidates_fail(
    store: SQLiteStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_fetch(base_url: str, *, timeout: float = 10.0) -> dict[str, Any]:
        raise RuntimeError("connection refused")

    monkeypatch.setattr(otbr_rest, "fetch_otbr_node", fake_fetch)

    res = _run(otbr_rest.ingest_once(store=store))

    assert res["error"] is not None
    assert "unreachable" in res["error"].lower()
    assert res["eui64"] is None
    # No node row was created.
    assert store.list_nodes() == []


def test_ingest_once_skips_payload_missing_ext_address(
    store: SQLiteStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_fetch(base_url: str, *, timeout: float = 10.0) -> dict[str, Any]:
        return {"State": "leader"}  # no ExtAddress

    monkeypatch.setattr(otbr_rest, "fetch_otbr_node", fake_fetch)

    res = _run(otbr_rest.ingest_once(store=store))

    assert res["error"] is not None
    assert store.list_nodes() == []


def test_state_to_routing_role_mapping() -> None:
    assert otbr_rest._STATE_TO_ROUTING_ROLE["leader"] == "leader"
    assert otbr_rest._STATE_TO_ROUTING_ROLE["router"] == "router"
    assert otbr_rest._STATE_TO_ROUTING_ROLE["detached"] == "unassigned"
