"""Contract tests for the trivial health endpoints."""

from __future__ import annotations

from thread_observability.api.schemas import HealthResponse


def test_root_health_contract(client) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    HealthResponse.model_validate(r.json())


def test_api_root_contract(client) -> None:
    r = client.get("/api")
    assert r.status_code == 200
    body = r.json()
    # Required identification fields.
    assert body["service"] == "core"
    assert body["name"] == "thread-observability"
    assert "version" in body and isinstance(body["version"], str)


def test_health_snapshot_contract(client) -> None:
    """``/v1/health/snapshot`` must always return a dict (never raise)."""
    r = client.get("/v1/health/snapshot")
    assert r.status_code == 200
    assert isinstance(r.json(), dict)
