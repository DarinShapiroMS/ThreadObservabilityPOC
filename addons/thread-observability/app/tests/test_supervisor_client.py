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
        if path == "/addons/self/info":
            return {
                "slug": "9e5048e8_thread-observability",
                "version": "0.9.1",
                "version_latest": "0.9.1",
                "update_available": False,
            }
        if path == "/store/addons":
            return {"addons": [
                {"slug": "thread-observability", "installed": True, "repository": "9e5048e8"},
            ]}
        raise AssertionError(f"unexpected GET: {path}")

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
    assert result["store_slug"] == "thread-observability"
    assert result["endpoint"] == "/store/addons/thread-observability/update"


def test_update_addon_resolves_store_slug_and_posts(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    async def fake_reload_store() -> dict[str, str]:
        return {"status": "ok"}

    async def fake_get_json(path: str) -> dict[str, object]:
        if path == "/addons/self/info":
            return {
                "slug": "9e5048e8_thread-observability",
                "version": "0.9.1",
                "version_latest": "0.9.2",
                "update_available": True,
            }
        if path == "/store/addons":
            return {"addons": [
                {"slug": "thread-observability", "installed": True, "repository": "9e5048e8"},
                {"slug": "core_openthread_border_router", "installed": True, "repository": "core"},
            ]}
        raise AssertionError(f"unexpected GET: {path}")

    async def fake_post(path: str, json_body: dict[str, object] | None = None) -> dict[str, object]:
        calls.append(path)
        return {"status": "ok"}

    monkeypatch.setattr(sc, "reload_store", fake_reload_store)
    monkeypatch.setattr(sc, "_get_json", fake_get_json)
    monkeypatch.setattr(sc, "_post", fake_post)

    result = asyncio.run(sc.update_addon())
    assert calls == ["/store/addons/thread-observability/update"], (
        "must NOT call /addons/self/update; must use resolved store slug"
    )
    assert result["status"] == "ok"
    assert result["performed"] is True
    assert result["store_slug"] == "thread-observability"
    assert result["repository"] == "9e5048e8"


def test_update_addon_dry_run_does_not_post(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_reload_store() -> dict[str, str]:
        return {"status": "ok"}

    async def fake_get_json(path: str) -> dict[str, object]:
        if path == "/addons/self/info":
            return {
                "slug": "9e5048e8_thread-observability",
                "version": "0.9.1",
                "version_latest": "0.9.2",
                "update_available": True,
            }
        if path == "/store/addons":
            return {"addons": [{"slug": "thread-observability", "installed": True, "repository": "9e5048e8"}]}
        raise AssertionError(f"unexpected GET: {path}")

    async def should_not_post(path: str, json_body: dict[str, object] | None = None) -> dict[str, object]:
        raise AssertionError(f"dry_run must not POST: {path}")

    monkeypatch.setattr(sc, "reload_store", fake_reload_store)
    monkeypatch.setattr(sc, "_get_json", fake_get_json)
    monkeypatch.setattr(sc, "_post", should_not_post)

    result = asyncio.run(sc.update_addon(dry_run=True))
    assert result["status"] == "dry_run"
    assert result["performed"] is False
    assert result["update_available"] is True
    assert result["endpoint"] == "/store/addons/thread-observability/update"


def test_update_addon_falls_back_to_self_slug_when_store_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    """If /store/addons can't be read, fall back to the self_slug rather than raising."""

    async def fake_reload_store() -> dict[str, str]:
        return {"status": "ok"}

    async def fake_get_json(path: str) -> dict[str, object]:
        if path == "/addons/self/info":
            return {
                "slug": "9e5048e8_thread-observability",
                "version": "0.9.1",
                "version_latest": "0.9.1",
                "update_available": False,
            }
        if path == "/store/addons":
            raise httpx.ConnectError("supervisor unreachable")
        raise AssertionError(f"unexpected GET: {path}")

    monkeypatch.setattr(sc, "reload_store", fake_reload_store)
    monkeypatch.setattr(sc, "_get_json", fake_get_json)

    result = asyncio.run(sc.update_addon(dry_run=True))
    assert result["store_slug"] == "9e5048e8_thread-observability"
    assert result["endpoint"] == "/store/addons/9e5048e8_thread-observability/update"


def test_update_addon_transport_error_is_not_silent_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Transport errors during the actual update POST must be surfaced, not coerced to 'accepted'."""

    async def fake_reload_store() -> dict[str, str]:
        return {"status": "ok"}

    async def fake_get_json(path: str) -> dict[str, object]:
        if path == "/addons/self/info":
            return {
                "slug": "9e5048e8_thread-observability",
                "version": "0.9.1",
                "version_latest": "0.9.2",
                "update_available": True,
            }
        if path == "/store/addons":
            return {"addons": [{"slug": "thread-observability", "installed": True, "repository": "9e5048e8"}]}
        raise AssertionError(f"unexpected GET: {path}")

    async def fake_post(path: str, json_body: dict[str, object] | None = None) -> dict[str, object]:
        request = httpx.Request("POST", f"http://supervisor{path}")
        raise httpx.ReadError("connection closed", request=request)

    monkeypatch.setattr(sc, "reload_store", fake_reload_store)
    monkeypatch.setattr(sc, "_get_json", fake_get_json)
    monkeypatch.setattr(sc, "_post", fake_post)

    result = asyncio.run(sc.update_addon())
    assert result["status"] == "transport_error"
    assert result["performed"] == "unknown"
    assert result["error_class"] == "ReadError"
    assert "ha_get_supervisor_logs" in result["note"]


def test_update_addon_http_error_includes_status_and_body(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_reload_store() -> dict[str, str]:
        return {"status": "ok"}

    async def fake_get_json(path: str) -> dict[str, object]:
        if path == "/addons/self/info":
            return {
                "slug": "9e5048e8_thread-observability",
                "version": "0.9.1",
                "version_latest": "0.9.2",
                "update_available": True,
            }
        if path == "/store/addons":
            return {"addons": [{"slug": "thread-observability", "installed": True, "repository": "9e5048e8"}]}
        raise AssertionError(f"unexpected GET: {path}")

    async def fake_post(path: str, json_body: dict[str, object] | None = None) -> dict[str, object]:
        request = httpx.Request("POST", f"http://supervisor{path}")
        response = httpx.Response(400, request=request, content=b'{"message": "missing image"}')
        raise httpx.HTTPStatusError("bad request", request=request, response=response)

    monkeypatch.setattr(sc, "reload_store", fake_reload_store)
    monkeypatch.setattr(sc, "_get_json", fake_get_json)
    monkeypatch.setattr(sc, "_post", fake_post)

    result = asyncio.run(sc.update_addon())
    assert result["status"] == "http_error"
    assert result["http_status"] == 400
    assert "missing image" in result["response_body"]


def test_rebuild_addon_accepts_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_post(path: str, json_body: dict[str, object] | None = None) -> dict[str, object]:
        request = httpx.Request("POST", f"http://supervisor{path}")
        raise httpx.ReadError("connection closed", request=request)

    monkeypatch.setattr(sc, "_post", fake_post)

    result = asyncio.run(sc.rebuild_addon())
    assert result["status"] == "accepted"
    assert result["action"] == "rebuild"
    assert "interrupted" in result["note"]
