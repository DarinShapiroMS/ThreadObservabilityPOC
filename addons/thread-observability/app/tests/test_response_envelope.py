"""Phase 1 temporal-honesty envelope: {data, meta} for read tools."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import pytest

from thread_observability.api import mcp_tools
from thread_observability.pipeline import runner as runner_mod


def _stub_runner_state(monkeypatch: pytest.MonkeyPatch, **overrides: Any) -> None:
    state = {
        "running": False,
        "current_stage": None,
        "started_at": 1_700_000_000.0,
        "finished_at": 1_700_000_005.0,
        "duration_seconds": 5.0,
        "stages": {"otbr_log_ingest": {"ok": True, "duration_seconds": 0.1}},
        "error": None,
        "tick_count": 42,
        "next_tick_after": None,
        "interval_seconds": 30,
    }
    state.update(overrides)
    monkeypatch.setattr(runner_mod, "_last_tick", state)


def test_read_tool_response_is_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_runner_state(monkeypatch)

    async def fake_dispatch(name: str, args: dict[str, Any]) -> dict[str, Any]:
        return {"nodes": [{"eui64": "abc"}], "count": 1}

    monkeypatch.setattr(mcp_tools, "_dispatch_tool", fake_dispatch)
    out = asyncio.run(mcp_tools._dispatch_and_wrap("list_all_nodes", {}))

    assert set(out.keys()) == {"data", "meta"}
    assert out["data"] == {"nodes": [{"eui64": "abc"}], "count": 1}
    meta = out["meta"]
    assert meta["tool"] == "list_all_nodes"
    assert meta["data_source"] == "sqlite_cache"
    assert meta["pipeline_tick"]["tick_count"] == 42
    assert meta["stale_after_s"] == 60.0
    # as_of is ISO-8601 parseable
    datetime.fromisoformat(meta["as_of"])
    # cache_age_s computed and non-negative
    assert meta["cache_age_s"] is not None and meta["cache_age_s"] >= 0


def test_write_tool_response_passes_through(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_runner_state(monkeypatch)

    async def fake_dispatch(name: str, args: dict[str, Any]) -> dict[str, Any]:
        return {"action": "restart", "result": {"status": "ok"}, "requested_at": "now"}

    monkeypatch.setattr(mcp_tools, "_dispatch_tool", fake_dispatch)
    out = asyncio.run(mcp_tools._dispatch_and_wrap("ha_restart_addon", {}))

    assert "data" not in out
    assert "meta" not in out
    assert out["action"] == "restart"


def test_read_tool_error_response_still_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_runner_state(monkeypatch)

    async def fake_dispatch(name: str, args: dict[str, Any]) -> dict[str, Any]:
        return {"error": "boom"}

    monkeypatch.setattr(mcp_tools, "_dispatch_tool", fake_dispatch)
    out = asyncio.run(mcp_tools._dispatch_and_wrap("get_health_snapshot", {}))

    assert out["data"] == {"error": "boom"}
    assert out["meta"]["tool"] == "get_health_snapshot"


def test_all_documented_read_tools_in_registry() -> None:
    """Every name in _READ_TOOLS must exist in TOOL_DEFS."""
    registered = {t["name"] for t in mcp_tools.TOOL_DEFS}
    missing = mcp_tools._READ_TOOLS - registered
    assert not missing, f"Read tools not registered: {sorted(missing)}"


def test_get_config_redacts_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    """ha_admin_token and influx.token must never appear in plaintext."""
    from thread_observability import config as cfg_mod

    fake = cfg_mod.ThreadObsConfig(
        ha_admin_token="SECRET-LLT-XYZ",
        influx=cfg_mod.InfluxConfig(token="SECRET-INFLUX-TOKEN"),
    )
    monkeypatch.setattr(mcp_tools, "get_config", lambda: fake)

    out = asyncio.run(mcp_tools._dispatch_tool("get_config", {}))
    assert out["ha_admin_token"] == "***"
    assert out["influx"]["token"] == "***"
    # Confirm no plaintext leak anywhere in the dumped payload
    import json as _json
    blob = _json.dumps(out)
    assert "SECRET-LLT-XYZ" not in blob
    assert "SECRET-INFLUX-TOKEN" not in blob


def test_pipeline_tick_persistence(store) -> None:  # type: ignore[no-untyped-def]
    rowid = store.record_pipeline_tick({
        "started_at": 1_700_000_000.0,
        "finished_at": 1_700_000_010.0,
        "duration_seconds": 10.0,
        "stages": {
            "otbr_log_ingest": {"ok": True, "duration_seconds": 0.1},
            "matter_discovery": {"ok": False, "error": "ws timeout"},
        },
        "error": "stages failed: matter_discovery",
    })
    assert rowid > 0

    rows = store.get_recent_pipeline_ticks(limit=5)
    assert len(rows) == 1
    r = rows[0]
    assert r["ok_count"] == 1
    assert r["fail_count"] == 1
    assert r["duration_s"] == 10.0
    assert "matter_discovery" in r["stages"]
    assert r["error"] == "stages failed: matter_discovery"
