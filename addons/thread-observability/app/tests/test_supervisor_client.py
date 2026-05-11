"""Tests for Supervisor API client edge cases used by MCP tools."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from thread_observability.api import supervisor_client as sc


def test_update_addon_no_update_available(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_reload_store() -> dict[str, str]:
        return {"status": "ok"}

    async def fake_get_json(path: str) -> dict[str, object]:
        assert path == "/addons/self/info"
        return {
            "slug": "9e5048e8_thread-observability",
            "version": "0.9.1",
            "version_latest": "0.9.1",
            "update_available": False,
        }

    async def should_not_post(path: str, json_body: dict[str, object] | None = None) -> dict[str, object]:
        raise AssertionError(f"unexpected POST call: {path}, {json_body}")

    monkeypatch.setattr(sc, "reload_store", fake_reload_store)
    monkeypatch.setattr(sc, "_get_json", fake_get_json)
    monkeypatch.setattr(sc, "_post", should_not_post)

    result = asyncio.run(sc.update_addon())
    assert result["status"] == "ok"
    assert result["performed"] is False
    assert result["reason"] == "no_update_available"
    assert result["current"] == "0.9.1"
    assert result["latest"] == "0.9.1"


def test_update_addon_403_maps_to_no_update(monkeypatch: pytest.MonkeyPatch) -> None:
    states = [
        {
            "slug": "9e5048e8_thread-observability",
            "version": "0.9.0",
            "version_latest": "0.9.1",
            "update_available": True,
        },
        {
            "slug": "9e5048e8_thread-observability",
            "version": "0.9.1",
            "version_latest": "0.9.1",
            "update_available": False,
        },
    ]

    async def fake_reload_store() -> dict[str, str]:
        return {"status": "ok"}

    async def fake_get_json(path: str) -> dict[str, object]:
        assert path == "/addons/self/info"
        return states.pop(0)

    async def fake_post(path: str, json_body: dict[str, object] | None = None) -> dict[str, object]:
        request = httpx.Request("POST", f"http://supervisor{path}")
        response = httpx.Response(403, request=request)
        raise httpx.HTTPStatusError("forbidden", request=request, response=response)

    monkeypatch.setattr(sc, "reload_store", fake_reload_store)
    monkeypatch.setattr(sc, "_get_json", fake_get_json)
    monkeypatch.setattr(sc, "_post", fake_post)

    result = asyncio.run(sc.update_addon())
    assert result["status"] == "ok"
    assert result["performed"] is False
    assert result["reason"] == "no_update_available"
    assert result["current"] == "0.9.1"
    assert result["latest"] == "0.9.1"


def test_rebuild_addon_accepts_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_post(path: str, json_body: dict[str, object] | None = None) -> dict[str, object]:
        request = httpx.Request("POST", f"http://supervisor{path}")
        raise httpx.ReadError("connection closed", request=request)

    monkeypatch.setattr(sc, "_post", fake_post)

    result = asyncio.run(sc.rebuild_addon())
    assert result["status"] == "accepted"
    assert result["action"] == "rebuild"
    assert "interrupted" in result["note"]


def test_update_addon_accepts_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_reload_store() -> dict[str, str]:
        return {"status": "ok"}

    async def fake_get_json(path: str) -> dict[str, object]:
        assert path == "/addons/self/info"
        return {
            "slug": "9e5048e8_thread-observability",
            "version": "0.9.1",
            "version_latest": "0.9.2",
            "update_available": True,
        }

    async def fake_post(path: str, json_body: dict[str, object] | None = None) -> dict[str, object]:
        request = httpx.Request("POST", f"http://supervisor{path}")
        raise httpx.ReadError("connection closed", request=request)

    monkeypatch.setattr(sc, "reload_store", fake_reload_store)
    monkeypatch.setattr(sc, "_get_json", fake_get_json)
    monkeypatch.setattr(sc, "_post", fake_post)

    result = asyncio.run(sc.update_addon())
    assert result["status"] == "accepted"
    assert result["action"] == "update"
    assert result["performed"] is True
