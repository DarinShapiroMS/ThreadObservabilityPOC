"""MCP JSON-RPC 2.0 server + REST API for Thread Observability add-on."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from . import supervisor_client
from ..config import get_config
from ..health import build_health_snapshot as _build_health_snapshot
from ..pipeline import otbr_adapter
from ..pipeline import reasoner as reasoner_mod
from ..pipeline import topology as topology_mod
from ..pipeline import seed as seed_mod
from ..storage import influx_store as ts_store
from ..storage.sqlite_store import get_store

MCP_PROTOCOL_VERSION = "2024-11-05"
LOG_PATH = Path(os.getenv("THREAD_OBS_LOG_FILE", "/data/thread-observability/addon.log"))
LOG_TAIL_LINES = 200


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _tail_log(n: int = LOG_TAIL_LINES) -> list[str]:
    """Return up to n lines from the tail of the add-on log file."""
    candidates = [
        LOG_PATH,
        Path("/run/uncaught-logs/current"),
    ]
    for path in candidates:
        if path.exists():
            try:
                lines = path.read_text(errors="replace").splitlines()
                return lines[-n:]
            except OSError:
                continue
    return ["[no log file found]"]


# ---------------------------------------------------------------------------
# REST tool registry (also used by MCP JSON-RPC handler)
# ---------------------------------------------------------------------------

class ToolCallRequest(BaseModel):
    arguments: dict[str, Any] = Field(default_factory=dict)


TOOL_DEFS: list[dict[str, Any]] = [
    {
        "name": "get_network_topology",
        "description": (
            "Return current Thread network topology snapshot (nodes and links) "
            "computed deterministically from the SQLite event log."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "freshness_minutes": {
                    "type": "integer",
                    "description": "Window (minutes) for inferring current parent links. Default 60.",
                    "default": 60,
                    "minimum": 1,
                    "maximum": 1440,
                }
            },
            "required": [],
        },
    },
    {
        "name": "list_active_issues",
        "description": "Return all currently-open Thread network issues from the SQLite issues table.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_health_snapshot",
        "description": (
            "Return current health snapshot: node counts by status (healthy / stale / offline), "
            "active issue counts, and data freshness age."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_reasoner",
        "description": (
            "Run the deterministic anomaly reasoner once over the SQLite event log. "
            "Opens new issues and auto-closes issues whose triggering condition no "
            "longer holds. Returns the summary with opened/still_open/closed issue ids."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "close_issue",
        "description": "Manually close an active issue by id.",
        "inputSchema": {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "required": ["id"],
        },
    },
    {
        "name": "seed_demo_topology",
        "description": (
            "DEV: populate SQLite with a deterministic demo topology (5 nodes, 4 links) "
            "plus a couple of anomaly-triggering event patterns. Idempotent. Use to "
            "validate the UI / reasoner before real ingestion lands."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "include_anomalies": {"type": "boolean", "default": True},
            },
            "required": [],
        },
    },
    {
        "name": "get_recent_logs",
        "description": "Return recent add-on log lines from the add-on's internal file logger.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "lines": {
                    "type": "integer",
                    "description": "Number of log lines to return (default 100, max 200).",
                    "default": 100,
                }
            },
            "required": [],
        },
    },
    {
        "name": "ha_get_addon_state",
        "description": (
            "Return Supervisor's view of this add-on: install state, current version, "
            "latest available version, boot/watchdog flags, ingress URL, and raw info. "
            "Use this from VS Code to verify a deploy without opening the HA UI."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "ha_get_addon_logs",
        "description": (
            "Return the tail of the Supervisor container log for this add-on. "
            "Captures s6-overlay/startup output that the in-process Python logger misses. "
            "Use this to diagnose crash loops or boot failures."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lines": {
                    "type": "integer",
                    "description": "Lines to return (default 200, max 1000).",
                    "default": 200,
                }
            },
            "required": [],
        },
    },
    {
        "name": "ha_get_supervisor_logs",
        "description": (
            "Return the tail of the Home Assistant Supervisor's own log. "
            "Useful for diagnosing why Supervisor rejected or killed the add-on "
            "(permissions, port conflicts, AppArmor, image pull failures)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lines": {
                    "type": "integer",
                    "description": "Lines to return (default 200, max 1000).",
                    "default": 200,
                }
            },
            "required": [],
        },
    },
    {
        "name": "ha_restart_addon",
        "description": (
            "Ask Supervisor to restart this add-on (fast; no image rebuild). "
            "Use after config or option changes to verify behaviour without a full deploy."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "ha_rebuild_addon",
        "description": (
            "Ask Supervisor to rebuild this add-on from its repository source, then restart. "
            "Use after pushing a new commit so VS Code can complete the change\u2192deploy\u2192observe "
            "loop without manual uninstall/reinstall."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "ha_check_for_update",
        "description": (
            "Force Supervisor to re-scan add-on repositories, then report current vs "
            "latest version. Returns {current, latest, update_available, auto_update, state}. "
            "Use right after pushing a new version bump to avoid waiting for Supervisor's "
            "periodic poll."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "ha_update_addon",
        "description": (
            "Update this add-on to the latest version available in the store "
            "(equivalent to clicking 'Update' in the HA UI). Supervisor pulls the new "
            "image / rebuilds from source and restarts. Pair with ha_check_for_update first."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "ha_set_auto_update",
        "description": (
            "Enable or disable Supervisor's auto-update flag for this add-on."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "enabled": {"type": "boolean", "description": "True to enable, false to disable."}
            },
            "required": ["enabled"],
        },
    },
    {
        "name": "ha_reinstall_addon",
        "description": (
            "Uninstall then reinstall this add-on from the store. Destructive: clears the "
            "add-on container and terminates the MCP process making the call (the HTTP "
            "response will be cut short). Treat connection-reset as expected success and "
            "poll ha_get_addon_state afterwards."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_storage_stats",
        "description": (
            "Return SQLite store stats (schema version, file size, row counts per table, "
            "oldest/newest event timestamps) plus the active time-series backend."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "query_events",
        "description": (
            "Return canonical events from the SQLite event log, newest first. "
            "Optional filters: eui64, event_type, since (ISO-8601 timestamp). "
            "Use to verify ingestion or to drill into a specific node's recent activity."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "eui64":      {"type": "string"},
                "event_type": {"type": "string"},
                "since":      {"type": "string", "description": "ISO-8601 timestamp"},
                "limit":      {"type": "integer", "default": 100, "minimum": 1, "maximum": 1000},
            },
            "required": [],
        },
    },
    {
        "name": "insert_test_event",
        "description": (
            "DEV: insert a synthetic canonical event into the SQLite store. Used to "
            "verify the storage layer end-to-end before real ingestion lands."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "eui64": {"type": "string", "default": "0000000000000001"},
                "type":  {"type": "string", "default": "attach"},
                "rssi":  {"type": "integer"},
                "lqi":   {"type": "integer"},
            },
            "required": [],
        },
    },
    {
        "name": "get_config",
        "description": "Return the typed add-on configuration (merged from /data/options.json plus env overrides).",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_timeseries_health",
        "description": "Probe the time-series backend (Influx if configured, else SQLite fallback) and return status.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "list_otbr_candidates",
        "description": (
            "Return Supervisor add-ons that look like OpenThread Border Router hosts "
            "(slug or name contains 'openthread', 'otbr', or 'silabs-multiprotocol'). "
            "Use to discover the slug to feed into set_otbr_slug."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "set_otbr_slug",
        "description": (
            "Set the OTBR add-on slug used by the background ingestion loop. Resets the "
            "cursor so the next poll will re-scan all currently-available log lines."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"slug": {"type": "string"}},
            "required": ["slug"],
        },
    },
    {
        "name": "ingest_now",
        "description": (
            "Run one OTBR ingestion pass synchronously: fetch logs from Supervisor, "
            "parse new lines, insert canonical events. Returns line/event counts."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"slug": {"type": "string", "description": "Optional slug override."}},
            "required": [],
        },
    },
    {
        "name": "get_ingest_state",
        "description": (
            "Return the current OTBR ingestion state: configured slug, lines processed, "
            "events inserted, last event timestamp, last run timestamp, last error."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
]

_TOOL_MAP = {t["name"]: t for t in TOOL_DEFS}


async def _dispatch_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Execute a tool and return its result payload."""
    if name == "get_network_topology":
        try:
            freshness = int(arguments.get("freshness_minutes", 60))
            return topology_mod.build_topology(freshness_minutes=freshness)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "list_active_issues":
        try:
            issues = get_store().list_active_issues()
            return {"count": len(issues), "issues": issues}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "get_health_snapshot":
        try:
            return _build_health_snapshot()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "run_reasoner":
        try:
            return reasoner_mod.run_reasoner()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "close_issue":
        try:
            ok = get_store().close_issue(int(arguments["id"]))
            return {"closed": ok, "id": int(arguments["id"])}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "seed_demo_topology":
        try:
            include = bool(arguments.get("include_anomalies", True))
            return seed_mod.seed_demo_topology(include_anomalies=include)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "get_recent_logs":
        n = min(int(arguments.get("lines", 100)), LOG_TAIL_LINES)
        lines = _tail_log(n)
        return {"lines": lines, "count": len(lines), "source": str(LOG_PATH)}

    # ---- Supervisor-backed dev-loop tools ---------------------------------
    if name == "ha_get_addon_state":
        try:
            return await supervisor_client.get_addon_info()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "hint": "Supervisor unreachable; running outside HA?"}
    if name == "ha_get_addon_logs":
        n = max(1, min(int(arguments.get("lines", 200)), 1000))
        try:
            lines = await supervisor_client.get_addon_logs(n)
            return {"lines": lines, "count": len(lines), "source": "supervisor:/addons/self/logs"}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "ha_get_supervisor_logs":
        n = max(1, min(int(arguments.get("lines", 200)), 1000))
        try:
            lines = await supervisor_client.get_supervisor_logs(n)
            return {"lines": lines, "count": len(lines), "source": "supervisor:/supervisor/logs"}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "ha_restart_addon":
        try:
            res = await supervisor_client.restart_addon()
            return {"action": "restart", "result": res, "requested_at": _utc_now()}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "ha_rebuild_addon":
        try:
            res = await supervisor_client.rebuild_addon()
            return {"action": "rebuild", "result": res, "requested_at": _utc_now()}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "ha_check_for_update":
        try:
            return await supervisor_client.check_for_update()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "ha_update_addon":
        try:
            res = await supervisor_client.update_addon()
            return {"action": "update", "result": res, "requested_at": _utc_now()}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "ha_set_auto_update":
        enabled = bool(arguments.get("enabled", False))
        try:
            res = await supervisor_client.set_auto_update(enabled)
            return {"action": "set_auto_update", "enabled": enabled, "result": res}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "ha_reinstall_addon":
        try:
            res = await supervisor_client.reinstall_addon("thread-observability")
            return {"action": "reinstall", "result": res, "requested_at": _utc_now()}
        except Exception as exc:  # noqa: BLE001
            # Connection reset mid-uninstall is the expected success path.
            return {"action": "reinstall", "note": "connection terminated (expected)",
                    "error": str(exc)}

    # ---- Storage / config tools (Phase 1) ---------------------------------
    if name == "get_storage_stats":
        try:
            stats = get_store().stats()
            try:
                ts_health = await ts_store.timeseries_health()
            except Exception as exc:  # noqa: BLE001
                ts_health = {"backend": "unknown", "error": str(exc)}
            return {"sqlite": stats, "timeseries": ts_health}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "query_events":
        try:
            events = get_store().query_events(
                eui64=arguments.get("eui64"),
                event_type=arguments.get("event_type"),
                since=arguments.get("since"),
                limit=int(arguments.get("limit", 100)),
            )
            return {"events": events, "count": len(events)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "insert_test_event":
        try:
            eid = get_store().insert_event(
                eui64=arguments.get("eui64", "0000000000000001"),
                type=arguments.get("type", "attach"),
                rssi=arguments.get("rssi"),
                lqi=arguments.get("lqi"),
                payload={"source": "insert_test_event"},
            )
            return {"inserted_event_id": eid, "at": _utc_now()}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "get_config":
        try:
            cfg = get_config()
            # Avoid leaking the influx token in MCP output.
            payload = cfg.model_dump()
            if payload.get("influx", {}).get("token"):
                payload["influx"]["token"] = "***"
            return payload
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "get_timeseries_health":
        try:
            return await ts_store.timeseries_health()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    # ---- OTBR ingestion tools (Phase 2.5) ---------------------------------
    if name == "list_otbr_candidates":
        try:
            cands = await otbr_adapter.list_candidates()
            return {"count": len(cands), "candidates": cands}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "candidates": []}
    if name == "set_otbr_slug":
        try:
            slug = str(arguments.get("slug", "")).strip()
            if not slug:
                return {"error": "slug required"}
            return otbr_adapter.set_slug(slug)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "ingest_now":
        try:
            slug = arguments.get("slug")
            return await otbr_adapter.ingest_once(slug=slug)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "get_ingest_state":
        try:
            return otbr_adapter.get_state()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    raise ValueError(f"Unknown tool: {name}")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

def create_mcp_app() -> FastAPI:
    app = FastAPI(title="Thread Observability MCP", version="0.1.0")

    # ── simple REST convenience endpoints ────────────────────────────────────

    @app.get("/")
    def root() -> dict[str, str]:
        return {"service": "mcp", "name": "thread-observability", "version": "0.8.0"}

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "mcp", "checked_at": _utc_now()}

    @app.get("/mcp/tools")
    def list_tools_rest() -> dict[str, object]:
        return {"tools": TOOL_DEFS, "count": len(TOOL_DEFS)}

    @app.post("/mcp/call/{tool_name}")
    async def call_tool_rest(tool_name: str, request: ToolCallRequest) -> dict[str, object]:
        if tool_name not in _TOOL_MAP:
            raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_name}")
        result = await _dispatch_tool(tool_name, request.arguments)
        return {"tool": tool_name, "result": result, "called_at": _utc_now()}

    # ── MCP JSON-RPC 2.0 endpoint (VS Code MCP client) ───────────────────────

    @app.post("/mcp")
    async def mcp_jsonrpc(request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}},
                status_code=400,
            )

        req_id = body.get("id")
        method = body.get("method", "")
        params = body.get("params", {})

        def ok(result: Any) -> JSONResponse:
            return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": result})

        def err(code: int, message: str) -> JSONResponse:
            return JSONResponse({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})

        if method == "initialize":
            return ok({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "thread-observability", "version": "0.8.0"},
            })

        if method == "notifications/initialized":
            return JSONResponse({}, status_code=204)

        if method == "tools/list":
            return ok({"tools": TOOL_DEFS})

        if method == "tools/call":
            tool_name = params.get("name", "")
            arguments = params.get("arguments", {})
            if tool_name not in _TOOL_MAP:
                return err(-32602, f"Unknown tool: {tool_name}")
            result = await _dispatch_tool(tool_name, arguments)
            import json as _json
            return ok({"content": [{"type": "text", "text": _json.dumps(result, default=str)}]})

        return err(-32601, f"Method not found: {method}")

    return app
