"""MCP JSON-RPC 2.0 server + REST API for Thread Observability add-on."""

from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from . import supervisor_client
from . import triage as triage_mod
from . import counter_series as counter_series_mod
from ..config import get_config
from ..health import build_health_snapshot as _build_health_snapshot
from ..pipeline import nodes as nodes_mod
from ..pipeline import otbr_adapter
from ..pipeline import topology as topology_mod
from ..utils.datetime import utc_now_iso
from ..storage import influx_store as ts_store
from ..storage.sqlite_store import get_store

MCP_PROTOCOL_VERSION = "2024-11-05"
LOG_PATH = Path(os.getenv("THREAD_OBS_LOG_FILE", "/data/thread-observability/addon.log"))
LOG_TAIL_LINES = 200


def _find_repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "documentation").exists():
            return parent
    return current.parents[6]


REPO_ROOT = _find_repo_root()
ADDON_CONFIG_PATH = REPO_ROOT / "addons" / "thread-observability" / "config.yaml"
GLOSSARY_PATH = REPO_ROOT / "documentation" / "glossary.md"
RESOURCE_DEFS: list[dict[str, str]] = [
    {
        "uri": "thread-observability://glossary",
        "name": "glossary",
        "title": "Thread and Matter glossary",
        "description": (
            "Shared background for Thread, Matter, and Home Assistant terms used across the MCP tool catalog, "
            "including spec links and field meanings such as RLOC16, partition_id, LQI, and MAC/MLE counters."
        ),
        "mimeType": "text/markdown",
    },
]
_RESOURCE_BY_NAME = {row["name"]: row for row in RESOURCE_DEFS}
_RESOURCE_BY_URI = {row["uri"]: row for row in RESOURCE_DEFS}


def _read_addon_version() -> str:
    try:
        match = re.search(r"^version:\s*([^\s#]+)", ADDON_CONFIG_PATH.read_text(encoding="utf-8"), re.MULTILINE)
    except OSError:
        match = None
    return str(match.group(1)).strip() if match else "unknown"


ADDON_VERSION = _read_addon_version()


def _utc_now() -> str:
    return utc_now_iso()


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


def _read_resource_text(resource_name_or_uri: str) -> tuple[dict[str, str], str]:
    resource = _RESOURCE_BY_NAME.get(resource_name_or_uri) or _RESOURCE_BY_URI.get(resource_name_or_uri)
    if resource is None:
        raise KeyError(resource_name_or_uri)
    if resource["name"] == "glossary":
        try:
            return resource, GLOSSARY_PATH.read_text(encoding="utf-8")
        except OSError as exc:
            raise FileNotFoundError(f"Unable to read glossary resource: {exc}") from exc
    raise KeyError(resource_name_or_uri)


# ---------------------------------------------------------------------------
# REST tool registry (also used by MCP JSON-RPC handler)
# ---------------------------------------------------------------------------

class ToolCallRequest(BaseModel):
    arguments: dict[str, Any] = Field(default_factory=dict)


TOOL_DEFS: list[dict[str, Any]] = [
    {
        "name": "get_mesh_state",
        "description": (
            "Use when: starting a triage session or answering 'what does the mesh look like right now?'. "
            "Returns the live Thread mesh: nodes + links + partition_id, computed deterministically from "
            "the SQLite event log and most-recent Matter discovery tick. Phantom nodes are excluded by default. "
            "Returns: {nodes:[{eui64, role, partition_id, parent_eui64, last_rssi, last_lqi, status, ...}], "
            "links:[...], partition_id, computed_at, node_count, link_count}. "
            "Caveats: derived from the latest persisted pipeline state. Check meta.cache_age_s on the response; if stale, call ingest_now to force "
            "a refresh."
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
                },
                "include_phantoms": {
                    "type": "boolean",
                    "description": "If true, include phantom (stale-reference) nodes in the snapshot. Default false.",
                    "default": False,
                },
            },
            "required": [],
        },
    },
    {
        "name": "list_active_issues",
        "description": (
            "Return all currently-open Thread network issues from the SQLite issues table. "
            "NOTE: Issue detection is currently paused pending a redesign of the rule set "
            "(see tracking issue #5). Until new rules ship, this tool returns an empty list "
            "with `status: \"placeholder\"`. Do NOT infer \"all clear\" from the empty list — "
            "instead, reason from the raw data (topology, partitions, links, nodes)."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },    {
        "name": "get_health_snapshot",
        "description": (
            "Return current health snapshot: node counts by status (healthy / stale / offline), "
            "active issue counts, and data freshness age."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "close_issue",
        "description": "Manually close an active issue by id.",
        "inputSchema": {
            "type": "object",
            "properties": {"id": {"type": "integer", "description": "Open issue id to close."}},
            "required": ["id"],
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
            "Return the tail of the Supervisor container log for an add-on. "
            "Defaults to this add-on (self) when ``slug`` is omitted; pass a "
            "Supervisor add-on slug (e.g. ``core_openthread_border_router``, "
            "``core_matter_server``) to fetch that add-on's container log instead. "
            "Captures s6-overlay/startup output that the in-process Python logger misses. "
            "Use this to diagnose crash loops, boot failures, or correlate "
            "OTBR/Matter server events with Thread mesh state."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lines": {
                    "type": "integer",
                    "description": "Lines to return (default 200, max 1000).",
                    "default": 200,
                },
                "slug": {
                    "type": "string",
                    "description": (
                        "Supervisor add-on slug. Omit (or null) for this "
                        "add-on's own logs."
                    ),
                },
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
            "image / rebuilds from source and restarts. Resolves the store-side slug "
            "from /store/addons (NOT /addons/self/info, whose slug carries a repo-hash "
            "prefix that the store endpoint rejects on some installs, silently clearing "
            "the install). Pass dry_run=true to verify the resolved endpoint without "
            "dispatching the update. Pair with ha_check_for_update first."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "dry_run": {
                    "type": "boolean",
                    "description": (
                        "If true, resolve the slug and report what endpoint would be "
                        "called, without POSTing. Default false."
                    ),
                }
            },
            "required": [],
        },
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
        "name": "list_thread_datasets",
        "description": (
            "Return the Thread Border Router credential datasets known to Home "
            "Assistant (network_name, extended_pan_id, channel, source, preferred). "
            "Pair with get_node_metadata or analyze_node to determine whether a node "
            "reporting an unexpected extended_pan_id is on a stale Thread dataset. "
            "Cached for 5 minutes."
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
        "name": "get_chat_stats",
        "description": (
            "Use when: reviewing dashboard chat usage and grounding behavior without inspecting raw messages. "
            "Returns aggregate turn counts, latency, tool-call counts, error breakdown, and a small recent-turn summary. Read-only."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": "ISO-8601 lower bound; default all retained chat telemetry."},
            },
            "required": [],
        },
    },
    {
        "name": "query_history",
        "description": (
            "Tier 4 unified timeline. Return a single newest-first stream that "
            "merges canonical events, issue open/close lifecycle, and observer "
            "(addon/OTBR/Matter Server) outage windows over a time range. Each "
            "row is normalized to {ts, source, kind, eui64?, severity?, "
            "details, ref_id} so an AI consultant can correlate Thread-side, "
            "issue-side and observer-side activity in one round-trip. Filter "
            "by eui64, kind list, or source list."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "since": {
                    "type": "string",
                    "description": "ISO-8601 lower bound (inclusive). Required.",
                },
                "until": {
                    "type": "string",
                    "description": "ISO-8601 upper bound (inclusive). Defaults to now.",
                },
                "eui64": {"type": "string", "description": "Optional EUI-64 to limit the merged timeline to one node."},
                "kinds": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Optional kind allow-list. Examples: "
                        "['attach','parent_change'], ['issue.opened','issue.closed'], "
                        "['observer.outage','observer.outage.ended']."
                    ),
                },
                "sources": {
                    "type": "array",
                    "items": {"type": "string", "enum": list(["events", "issues", "observer_events"])},
                    "description": "Optional source allow-list. Defaults to all three.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum rows to return, newest first. Default 500.",
                    "default": 500,
                    "minimum": 1,
                    "maximum": 5000,
                },
            },
            "required": ["since"],
        },
    },
    {
        "name": "get_topology_history_entry",
        "description": (
            "Tier 4. Return a persisted topology snapshot row. Pass "
            "``snapshot_id`` to fetch one by id, or ``at`` (ISO-8601) "
            "to fetch the most-recent snapshot captured on or before "
            "that time. With no arguments, returns the newest snapshot."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "snapshot_id": {"type": "integer", "minimum": 1, "description": "Exact topology snapshot id to fetch."},
                "at": {"type": "string", "description": "ISO-8601 timestamp"},
            },
            "required": [],
        },
    },
    {
        "name": "list_topology_history",
        "description": (
            "Tier 4. List topology snapshot summaries (id, captured_at, "
            "hash, partition_id, node_count, link_count) newest-first. "
            "Snapshot bodies are NOT returned — use ``get_topology_history_entry`` "
            "or ``diff_topology_history`` to drill in."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": "ISO-8601 lower bound"},
                "until": {"type": "string", "description": "ISO-8601 upper bound"},
                "limit": {
                    "type": "integer",
                    "description": "Maximum snapshot summaries to return. Default 100.",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 1000,
                },
            },
            "required": [],
        },
    },
    {
        "name": "diff_topology_history",
        "description": (
            "Tier 4. Return a structured diff between two topology "
            "snapshots: added/removed nodes, per-node role/partition/parent "
            "transitions, and added/removed links. ``snapshot_id_a`` is the "
            "older / baseline, ``snapshot_id_b`` is the newer / candidate."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "snapshot_id_a": {"type": "integer", "minimum": 1, "description": "Older or baseline snapshot id."},
                "snapshot_id_b": {"type": "integer", "minimum": 1, "description": "Newer or comparison snapshot id."},
            },
            "required": ["snapshot_id_a", "snapshot_id_b"],
        },
    },
    {
        "name": "list_playbooks",
        "description": (
            "Tier 4. Return summaries (id, title, applies_to) of every "
            "Thread/Matter playbook in the bundled corpus. Use "
            "``lookup_playbook`` to fetch full entries."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "lookup_playbook",
        "description": (
            "Tier 4. Return playbook entries matching one of: an exact "
            "``playbook_id``; an issue ``kind`` (returns every playbook "
            "whose applies_to includes the kind); or a free-text "
            "``query`` (case-insensitive substring across id/title/"
            "summary). Each entry includes summary, evidence_to_collect, "
            "remediation_steps, references."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "playbook_id": {"type": "string", "description": "Exact playbook id to fetch."},
                "kind": {"type": "string", "description": "Issue kind to match against a playbook's applies_to list."},
                "query": {"type": "string", "description": "Case-insensitive free-text search across playbook id, title, and summary."},
            },
            "required": [],
        },
    },
    {
        "name": "analyze_node",
        "description": (
            "Use when: drilling into a single suspected-bad EUI-64. One-call structured payload: node metadata, parent + neighbors, "
            "open issues, recent closed issues, unified timeline (events + issue lifecycle + observer events), per-node baselines "
            "(parent_change rate this period vs. previous, status_change count), and full playbook entries matching the union of issue kinds. "
            "Prefer over composing list_all_nodes + list_active_issues + query_history + lookup_playbook by hand. "
            "Returns: rich JSON keyed by section. Caveats: timeline_hours and baseline_days are capped; very large windows truncate."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "eui64": {"type": "string", "description": "Target node EUI-64 to analyze."},
                "timeline_hours": {
                    "type": "integer",
                    "description": "How many recent hours of unified timeline to include. Default 24.",
                    "default": 24,
                    "minimum": 1,
                    "maximum": 720,
                },
                "baseline_days": {
                    "type": "integer",
                    "description": "How many historical days to use for baseline rate comparisons. Default 7.",
                    "default": 7,
                    "minimum": 1,
                    "maximum": 90,
                },
            },
            "required": ["eui64"],
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
            "properties": {"slug": {"type": "string", "description": "Supervisor add-on slug to treat as the OTBR log source."}},
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
    {
        "name": "list_all_nodes",
        "description": (
            "Use when: enumerating every known Thread node (including phantoms) or building a device-by-device inventory. "
            "Returns: {nodes:[{eui64, friendly_name, role, area, device_id, status, first_seen, last_seen, last_rssi, last_lqi, ...}], count}. "
            "Ordered most-recently-seen first. Use ``status_filter='phantom'`` to drill into stale-reference cleanup candidates. "
            "Caveats: sourced from the latest persisted pipeline state; check meta.cache_age_s."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "status_filter": {
                    "type": "string",
                    "enum": ["healthy", "stale", "offline", "phantom"],
                    "description": "Restrict to nodes whose status matches this value.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "sync_ha_devices",
        "description": (
            "Use when: HA shows a Thread device the addon hasn't seen yet, or after a fresh commission, or when phantom "
            "counts look wrong. Queries the HA device registry for Thread/Zigbee devices and correlates IEEE addresses "
            "with extracted EUI64 nodes. Auto-populates friendly_name and device_id for matching nodes. "
            "Returns: {matched, updated, ...}. "
            "Caveats: This is a mutation (writes friendly_name/device_id back to SQLite); not a read tool."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    # ---- Phase 3 triage tools ---------------------------------------------
    {
        "name": "start_triage",
        "description": (
            "Use when: starting any new investigation, or as the first call in a session. Returns the consolidated "
            "environment (addon/HA/OTBR/Matter/network/pipeline versions) plus the health snapshot plus active issues "
            "plus a `recommended_next` list of up to 3 follow-up tool calls chosen from the catalog. "
            "Returns: {as_of, environment, health, active_issues_count, active_issues[<=10], recommended_next[<=3]}. "
            "Caveats: snapshot from SQLite cache; refresh by waiting one pipeline tick."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_environment",
        "description": (
            "Use when: you need versions/identity of every relevant component in one shot — addon version, HA Core "
            "version, Supervisor version, OTBR add-on state, Matter Server add-on state, Thread network identity "
            "(name/pan_id/channel/leader), and pipeline runner state. "
            "Returns: {addon, home_assistant, otbr, matter_server, network, pipeline}. "
            "Caveats: Supervisor calls may fail outside the HA container; those sections fall back to `{error: ...}`."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_pipeline_health",
        "description": (
            "Use when: data looks stale, the dashboard is empty, or the model needs to know whether the pipeline is "
            "actually running. Returns the last N pipeline ticks (newest first) plus a summary including "
            "consecutive_failed_ticks, stages_currently_failing, avg_duration_seconds, and the current runner state. "
            "Returns: {summary: {...}, recent_ticks: [...]}. "
            "Caveats: only ticks recorded in schema v18+ are visible; backfill is not retroactive."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 20, "description": "Maximum recent pipeline ticks to return. Default 20."},
            },
            "required": [],
        },
    },
    # ---- Phase 4 counter time-series tools --------------------------------
    {
        "name": "get_counter_series",
        "description": (
            "Use when: investigating whether a node's MAC/MLE counters are climbing (tx_retry, tx_err_cca, parent_change, "
            "attach_attempt). Returns the time-series of selected counter values for one node over [since, until], plus "
            "per-counter deltas (last - first). Detects counter resets (re-attach) and reports them explicitly instead "
            "of misreading them as a huge negative spike. "
            "Returns: {eui64, since, until, resolution, series: [{observed_at, counters}, ...], deltas: {<name>: {delta, reset_detected, first, last}}}. "
            "Caveats: requires Phase 4 schema (v19+); samples only exist for ticks recorded after upgrade."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "eui64": {"type": "string", "description": "Target node EUI-64 whose counter samples should be returned."},
                "counter_names": {"type": "array", "items": {"type": "string"}, "description": "Optional subset of counter names to include; omit for the default diagnostic set."},
                "since": {"type": "string", "description": "ISO-8601; default 6h ago"},
                "until": {"type": "string", "description": "ISO-8601; default now"},
                "resolution": {"type": "string", "enum": ["raw", "5min"], "default": "raw", "description": "Return raw stored samples or a 5-minute rollup."},
            },
            "required": ["eui64"],
        },
    },
    {
        "name": "compare_node_counters",
        "description": (
            "Use when: a node looks unhealthy and you want to know whether a peer on the same partition is degrading "
            "the same way. Returns counter series for two nodes side-by-side over the same window, plus a peer_summary "
            "flagging counters where one side's delta is at least 2x the other. "
            "Returns: {a: {series, deltas}, b: {series, deltas}, peer_summary: {flagged, flagged_count}}. "
            "Caveats: requires Phase 4 schema (v19+); use list_all_nodes to find a healthy peer first."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "eui64_a": {"type": "string", "description": "First node EUI-64 to compare."},
                "eui64_b": {"type": "string", "description": "Second node EUI-64 to compare against the first."},
                "counter_names": {"type": "array", "items": {"type": "string"}, "description": "Optional subset of counters to compare; omit for the default diagnostic set."},
                "since": {"type": "string", "description": "ISO-8601 lower bound for both series; default 6h ago."},
                "until": {"type": "string", "description": "ISO-8601 upper bound for both series; default now."},
                "resolution": {"type": "string", "enum": ["raw", "5min"], "default": "raw", "description": "Return raw samples or 5-minute rollups for both nodes."},
            },
            "required": ["eui64_a", "eui64_b"],
        },
    },
    # ---- Phase 4 Background Diagnostics tools -----------------------------
    {
        "name": "get_assessment_state",
        "description": (
            "Use when: you need to know whether Background Diagnostics is currently scheduled, when the next "
            "assessment will run, and how much of today's call budget has been used. Returns the live scheduler "
            "snapshot (state, next_assessment_at, budget). Read-only."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "list_assessment_findings",
        "description": (
            "Use when: surfacing or reviewing AI-flagged conditions on the Thread mesh. Returns finding rows "
            "(headline, severity, confidence, evidence, suggested_starter_prompt, node_eui64) for the requested "
            "state (default: open). Read-only."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "state": {
                    "type": "string",
                    "enum": ["open", "cleared", "dismissed", "all"],
                    "default": "open",
                    "description": "Which finding state bucket to return. Default open.",
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50, "description": "Maximum findings to return. Default 50."},
            },
            "required": [],
        },
    },
    {
        "name": "mark_finding_outcome",
        "description": (
            "Use when: the user (or a downstream agent) wants to confirm whether an AI-surfaced finding was "
            "actionable. Records an outcome (resolved / wrong / ignored_dismissed) for the finding and updates the "
            "finding's state. Powers the precision metrics returned by ``get_assessment_quality``."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "finding_id": {"type": "string", "description": "Assessment finding id to update."},
                "outcome": {
                    "type": "string",
                    "enum": ["resolved", "wrong", "ignored_dismissed"],
                    "description": "Outcome to record for the finding.",
                },
                "notes": {"type": "string", "description": "Optional operator notes explaining why the outcome was chosen."},
            },
            "required": ["finding_id", "outcome"],
        },
    },
    {
        "name": "get_assessment_quality",
        "description": (
            "Use when: reviewing how the Background Diagnostics AI has been performing — precision estimate, "
            "outcome breakdown, and any signal types whose false-positive rate looks high. Read-only."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": "ISO-8601; default 7d ago"},
            },
            "required": [],
        },
    },
]

_TOOL_MAP = {t["name"]: t for t in TOOL_DEFS}


def _build_partition_state(include_phantoms: bool = False) -> dict[str, Any]:
    """Summarize current Thread partition state from the nodes table.

    Phantom-only partitions are excluded by default so a long-stale
    re-commissioned EUI doesn't trigger a false "network split" reading.
    """
    s = get_store()
    nodes = s.list_nodes()
    live_euis = {
        n["eui64"] for n in nodes if n.get("eui64") and n.get("status") != "phantom"
    }
    partitions: dict[int, list[str]] = {}
    leaders: dict[int, str] = {}
    diag_seen: list[str] = []
    for n in nodes:
        pid = n.get("partition_id")
        if not isinstance(pid, int):
            continue
        eui = n.get("eui64")
        if not eui:
            continue
        if not include_phantoms and n.get("status") == "phantom":
            continue
        partitions.setdefault(pid, []).append(eui)
        if n.get("routing_role") == "leader":
            leaders.setdefault(pid, eui)
        ts = n.get("diag_updated_at")
        if ts:
            diag_seen.append(ts)

    # Drop partitions that ended up with no live members.
    if not include_phantoms:
        partitions = {
            pid: members for pid, members in partitions.items()
            if any(m in live_euis for m in members)
        }

    # A real Thread partition has a leader. A "partition" of 1 member with
    # no leader is almost always a stale `partition_id` left over on a node
    # whose router has departed — the node will be swept to phantom by the
    # next cycle, but until then it makes the network look split. Drop it
    # unless the caller explicitly asked for phantoms.
    if not include_phantoms:
        suspicious = [
            pid for pid, members in partitions.items()
            if leaders.get(pid) is None and len(members) <= 1
        ]
        for pid in suspicious:
            partitions.pop(pid, None)

    events = s.query_events(event_type="partition_change", limit=10)
    last_change = events[0].get("ts") if events else None

    partition_summary = [
        {
            "partition_id": pid,
            "leader_eui64": leaders.get(pid),
            "member_count": len(members),
            "members": members,
        }
        for pid, members in sorted(partitions.items())
    ]
    return {
        "partition_count": len(partitions),
        "split": len(partitions) > 1,
        "partitions": partition_summary,
        "last_change_at": last_change,
        "last_observed_at": max(diag_seen) if diag_seen else None,
        "recent_changes": events,
    }


def _build_phantom_list() -> dict[str, Any]:
    """Return phantom nodes with everything a human needs to find them in HA."""
    s = get_store()
    rows = s.list_phantom_nodes()
    out: list[dict[str, Any]] = []
    for r in rows:
        device_id = r.get("device_id")
        # HA's devices UI is reachable at /config/devices/device/<device_id>
        # (relative — the user pastes it into their HA URL).
        ha_path = f"/config/devices/device/{device_id}" if device_id else None
        out.append({
            "eui64": r.get("eui64"),
            "friendly_name": r.get("friendly_name"),
            "device_id": device_id,
            "area": r.get("area"),
            "role": r.get("role"),
            "routing_role": r.get("routing_role"),
            "partition_id": r.get("partition_id"),
            "last_seen": r.get("last_seen"),
            "last_referenced_at": r.get("last_referenced_at"),
            "available": r.get("available"),
            "availability_source": r.get("availability_source"),
            "availability_checked_at": r.get("availability_checked_at"),
            "ha_device_path": ha_path,
        })
    return {
        "count": len(out),
        "phantoms": out,
        "cleanup_hint": (
            "These nodes have not been seen in any router's NeighborTable or "
            "RouteTable for >24h. To remove from Home Assistant: Settings → "
            "Devices & Services → Devices → (find by friendly_name) → 3-dot "
            "menu → Delete. Or paste ha_device_path into your HA URL."
        ),
    }


# ---------------------------------------------------------------------------
# Phase 1 temporal-honesty envelope
# ---------------------------------------------------------------------------

# Read-only tools whose responses are wrapped in ``{data, meta}`` so callers
# can see exactly when the underlying SQLite cache was last refreshed and
# which pipeline tick produced it. Write/mutating tools are passed through
# unwrapped because they already include their own "requested_at" /
# "performed" / "action" fields and are not snapshots of cached state.
_READ_TOOLS: frozenset[str] = frozenset({
    "get_mesh_state",
    "list_active_issues",
    "get_health_snapshot",
    "get_recent_logs",
    "ha_get_addon_state",
    "ha_get_addon_logs",
    "ha_get_supervisor_logs",
    "ha_check_for_update",
    "list_thread_datasets",
    "get_storage_stats",
    "get_chat_stats",
    "query_history",
    "get_topology_history_entry",
    "list_topology_history",
    "diff_topology_history",
    "list_playbooks",
    "lookup_playbook",
    "analyze_node",
    "get_config",
    "get_timeseries_health",
    "list_otbr_candidates",
    "get_ingest_state",
    "list_all_nodes",
    "start_triage",
    "get_environment",
    "get_pipeline_health",
    "get_counter_series",
    "compare_node_counters",
})


def _meta(name: str) -> dict[str, Any]:
    """Build the ``meta`` block describing freshness of a read response.

    The block is intentionally small and serialisation-safe: just enough
    for a caller to answer "is this data stale, and which tick produced it?".
    """
    from ..pipeline.runner import get_runner_state

    state = get_runner_state()
    finished_at = state.get("finished_at")
    started_at = state.get("started_at")
    interval = state.get("interval_seconds")

    now_ts = datetime.now(tz=UTC).timestamp()
    cache_age_s: float | None = None
    if isinstance(finished_at, (int, float)):
        cache_age_s = round(max(0.0, now_ts - float(finished_at)), 3)

    def _iso(v: Any) -> str | None:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v, tz=UTC).isoformat()
        if isinstance(v, str):
            return v
        return None

    stale_after_s: float | None = None
    if isinstance(interval, (int, float)):
        # Heuristic: two pipeline intervals before we'd consider the
        # cache stale (one missed tick is normal; two means trouble).
        stale_after_s = float(interval) * 2.0

    return {
        "tool": name,
        "as_of": _utc_now(),
        "data_source": "persisted_state",
        "cache_age_s": cache_age_s,
        "stale_after_s": stale_after_s,
        "pipeline_tick": {
            "tick_count": state.get("tick_count"),
            "started_at": _iso(started_at),
            "finished_at": _iso(finished_at),
            "duration_seconds": state.get("duration_seconds"),
            "current_stage": state.get("current_stage"),
            "running": state.get("running"),
            "error": state.get("error"),
        },
    }


async def _dispatch_and_wrap(
    name: str, arguments: dict[str, Any]
) -> dict[str, Any]:
    """Dispatch a tool and wrap read responses in ``{data, meta}``.

    Write/mutating tools (those NOT in ``_READ_TOOLS``) are returned
    unchanged so existing ``{action, result, requested_at}`` shapes
    surface untouched.
    """
    result = await _dispatch_tool(name, arguments)
    if name not in _READ_TOOLS:
        return result
    return {"data": result, "meta": _meta(name)}


async def _dispatch_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Execute a tool and return its result payload."""
    if name == "get_mesh_state":
        try:
            freshness = int(arguments.get("freshness_minutes", 60))
            include_phantoms = bool(arguments.get("include_phantoms", False))
            return topology_mod.build_topology(
                freshness_minutes=freshness,
                include_phantoms=include_phantoms,
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "list_active_issues":
        # Mirrors /v1/issues/active. Issue detection is paused
        # pending redesign (#5); return an explicit placeholder so AI
        # consumers don't infer "all clear" from an empty list.
        from ..pipeline.reasoner import ISSUES_PAUSED, ISSUES_PAUSED_NOTE
        if ISSUES_PAUSED:
            return {
                "count": 0,
                "issues": [],
                "status": "placeholder",
                "note": ISSUES_PAUSED_NOTE,
            }
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
    if name == "close_issue":
        try:
            ok = get_store().close_issue(int(arguments["id"]))
            return {"closed": ok, "id": int(arguments["id"])}
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
        slug = arguments.get("slug") or None
        if slug is not None:
            slug = str(slug).strip() or None
        try:
            lines = await supervisor_client.get_addon_logs(n, slug=slug)
            source = (
                f"supervisor:/addons/{slug}/logs" if slug
                else "supervisor:/addons/self/logs"
            )
            return {"lines": lines, "count": len(lines), "source": source, "slug": slug}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "slug": slug}
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
        dry_run = bool(arguments.get("dry_run", False))
        try:
            res = await supervisor_client.update_addon(dry_run=dry_run)
            return {"result": res, "requested_at": _utc_now()}
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
    if name == "list_thread_datasets":
        try:
            return await supervisor_client.list_thread_datasets()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

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
    if name == "get_chat_stats":
        try:
            return get_store().get_chat_turn_stats(since=arguments.get("since"))
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "query_history":
        try:
            from ..pipeline import timeline as timeline_mod

            since = arguments.get("since")
            if not since:
                return {"error": "missing required argument: since"}
            return timeline_mod.query_timeline(
                get_store(),
                since=since,
                until=arguments.get("until"),
                eui64=arguments.get("eui64"),
                kinds=arguments.get("kinds") or None,
                sources=arguments.get("sources") or None,
                limit=int(arguments.get("limit", 500)),
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "get_topology_history_entry":
        try:
            sid = arguments.get("snapshot_id")
            if sid is not None:
                snap = get_store().get_topology_snapshot(int(sid))
            else:
                snap = get_store().get_latest_topology_snapshot(
                    at=arguments.get("at")
                )
            return snap or {"snapshot": None}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "list_topology_history":
        try:
            snaps = get_store().list_topology_snapshots(
                since=arguments.get("since"),
                until=arguments.get("until"),
                limit=int(arguments.get("limit", 100)),
            )
            return {"snapshots": snaps, "count": len(snaps)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "diff_topology_history":
        try:
            from ..pipeline import topology_snapshot as ts_mod

            a = arguments.get("snapshot_id_a")
            b = arguments.get("snapshot_id_b")
            if a is None or b is None:
                return {
                    "error": "missing required arguments: snapshot_id_a, snapshot_id_b"
                }
            return ts_mod.diff_topology(
                get_store(),
                snapshot_id_a=int(a),
                snapshot_id_b=int(b),
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "list_playbooks":
        try:
            from ..pipeline import playbooks as pb_mod

            return pb_mod.list_playbooks()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "lookup_playbook":
        try:
            from ..pipeline import playbooks as pb_mod

            return pb_mod.lookup_playbook(
                kind=arguments.get("kind"),
                playbook_id=arguments.get("playbook_id"),
                query=arguments.get("query"),
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "analyze_node":
        try:
            from ..pipeline import analyze_node as an_mod

            eui = arguments.get("eui64")
            if not eui:
                return {"error": "missing required argument: eui64"}
            return an_mod.analyze_node(
                eui,
                store=get_store(),
                timeline_hours=int(arguments.get("timeline_hours", 24)),
                baseline_days=int(arguments.get("baseline_days", 7)),
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "get_config":
        try:
            cfg = get_config()
            # Redact every secret-bearing field before returning. The MCP
            # port has no auth, so a leaked token here = a leaked token.
            payload = cfg.model_dump()
            if payload.get("ha_admin_token"):
                payload["ha_admin_token"] = "***"
            if payload.get("influx", {}).get("token"):
                payload["influx"]["token"] = "***"
            if payload.get("ai", {}).get("api_key"):
                payload["ai"]["api_key"] = "***"
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

    # ---- Node metadata tools ----------------------------------
    if name == "list_all_nodes":
        try:
            status_filter = arguments.get("status_filter")
            include_phantoms = status_filter == "phantom"
            nodes = nodes_mod.list_nodes_enriched(
                include_signal_strength=True,
                include_phantoms=include_phantoms,
            )
            if status_filter:
                if status_filter == "phantom":
                    nodes = [n for n in nodes if n.get("status") == "phantom"]
                else:
                    nodes = [
                        n for n in nodes
                        if nodes_mod.infer_node_status(n) == status_filter
                    ]
            return {"nodes": nodes, "count": len(nodes)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "nodes": []}
    if name == "sync_ha_devices":
        try:
            from ..pipeline import device_discovery
            return await device_discovery.discover_and_sync()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "matched": 0, "updated": 0}

    # ---- Phase 3 triage tools -----------------------------------------
    if name == "start_triage":
        try:
            from .http_api import ADDON_VERSION
            return await triage_mod.start_triage(addon_version=ADDON_VERSION)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "get_environment":
        try:
            from .http_api import ADDON_VERSION
            return await triage_mod.get_environment(addon_version=ADDON_VERSION)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "get_pipeline_health":
        try:
            limit = int(arguments.get("limit", 20))
            return triage_mod.get_pipeline_health(limit=limit)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    # ---- Phase 4 counter time-series tools ----------------------------
    if name == "get_counter_series":
        try:
            return counter_series_mod.get_counter_series(
                eui64=str(arguments.get("eui64") or ""),
                counter_names=arguments.get("counter_names") or None,
                since=arguments.get("since"),
                until=arguments.get("until"),
                resolution=str(arguments.get("resolution") or "raw"),
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "compare_node_counters":
        try:
            return counter_series_mod.compare_node_counters(
                eui64_a=str(arguments.get("eui64_a") or ""),
                eui64_b=str(arguments.get("eui64_b") or ""),
                counter_names=arguments.get("counter_names") or None,
                since=arguments.get("since"),
                until=arguments.get("until"),
                resolution=str(arguments.get("resolution") or "raw"),
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    if name == "get_assessment_state":
        from ..services.assessment.scheduler import (
            AssessmentScheduler,
            ScheduleConfig,
        )
        cfg = ScheduleConfig.from_dict(
            get_config().assessment.model_dump()
        )
        sched = AssessmentScheduler(config=cfg)
        return sched.snapshot().as_dict()

    if name == "list_assessment_findings":
        state_arg = arguments.get("state") or "open"
        state_filter: str | None = state_arg if state_arg != "all" else None
        limit = int(arguments.get("limit") or 50)
        rows = get_store().list_assessment_findings(state=state_filter, limit=limit)
        return {"findings": rows, "count": len(rows)}

    if name == "mark_finding_outcome":
        from ..services.assessment import feedback as feedback_mod
        try:
            rec = feedback_mod.mark_outcome(
                finding_id=str(arguments.get("finding_id") or ""),
                outcome=str(arguments.get("outcome") or ""),
                notes=arguments.get("notes"),
            )
            return {"recorded": True, "feedback": rec}
        except (LookupError, ValueError) as exc:
            return {"recorded": False, "error": str(exc)}

    if name == "get_assessment_quality":
        from ..services.assessment import feedback as feedback_mod
        return feedback_mod.quality_summary(since=arguments.get("since"))

    raise ValueError(f"Unknown tool: {name}")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

def create_mcp_app() -> FastAPI:
    app = FastAPI(title="Thread Observability MCP", version=ADDON_VERSION)
    sse_sessions: dict[str, asyncio.Queue[dict[str, Any]]] = {}

    def _jsonrpc_ok(req_id: Any, result: Any) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": req_id, "result": result}

    def _jsonrpc_error(req_id: Any, code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}

    async def _handle_mcp_jsonrpc(body: dict[str, Any]) -> tuple[dict[str, Any], int]:
        req_id = body.get("id")
        method = body.get("method", "")
        params = body.get("params", {})

        if method == "initialize":
            return _jsonrpc_ok(
                req_id,
                {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {"tools": {}, "resources": {}},
                    "serverInfo": {"name": "thread-observability", "version": ADDON_VERSION},
                    "transport": {
                        "legacy_jsonrpc_post": "/mcp",
                        "sse": "/mcp/sse",
                        "message_post_template": "/mcp/messages/{session_id}",
                        "streamable_http_post": "/mcp/stream",
                    },
                },
            ), 200

        if method == "notifications/initialized":
            return {}, 204

        if method == "tools/list":
            return _jsonrpc_ok(req_id, {"tools": TOOL_DEFS}), 200

        if method == "resources/list":
            return _jsonrpc_ok(req_id, {"resources": RESOURCE_DEFS}), 200

        if method == "resources/read":
            resource_name = params.get("uri") or params.get("name")
            if not resource_name:
                return _jsonrpc_error(req_id, -32602, "Missing resource uri"), 200
            try:
                resource, contents = _read_resource_text(resource_name)
            except KeyError:
                return _jsonrpc_error(req_id, -32602, f"Unknown resource: {resource_name}"), 200
            except FileNotFoundError as exc:
                return _jsonrpc_error(req_id, -32603, str(exc)), 200
            return _jsonrpc_ok(
                req_id,
                {
                    "contents": [
                        {
                            "uri": resource["uri"],
                            "mimeType": resource["mimeType"],
                            "text": contents,
                        }
                    ]
                },
            ), 200

        if method == "tools/call":
            tool_name = params.get("name", "")
            arguments = params.get("arguments", {})
            if tool_name not in _TOOL_MAP:
                return _jsonrpc_error(req_id, -32602, f"Unknown tool: {tool_name}"), 200
            result = await _dispatch_and_wrap(tool_name, arguments)
            return _jsonrpc_ok(req_id, {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}), 200

        return _jsonrpc_error(req_id, -32601, f"Method not found: {method}"), 200

    def _encode_sse(event: str, data: dict[str, Any]) -> bytes:
        return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n".encode("utf-8")

    def _register_sse_session() -> tuple[str, asyncio.Queue[dict[str, Any]], dict[str, str]]:
        session_id = uuid.uuid4().hex
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        sse_sessions[session_id] = queue
        endpoint_payload = {
            "session_id": session_id,
            "url": f"/mcp/messages/{session_id}",
        }
        return session_id, queue, endpoint_payload

    app.state.mcp_sse_sessions = sse_sessions
    app.state.register_mcp_sse_session = _register_sse_session
    app.state.encode_mcp_sse_event = _encode_sse

    # ── simple REST convenience endpoints ────────────────────────────────────

    @app.get("/")
    def root() -> dict[str, str]:
        return {"service": "mcp", "name": "thread-observability", "version": ADDON_VERSION}

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "mcp", "checked_at": _utc_now()}

    @app.get("/mcp/tools")
    def list_tools_rest() -> dict[str, object]:
        return {"tools": TOOL_DEFS, "count": len(TOOL_DEFS)}

    @app.get("/mcp/resources")
    def list_resources_rest() -> dict[str, object]:
        return {"resources": RESOURCE_DEFS, "count": len(RESOURCE_DEFS)}

    @app.get("/mcp/resources/{resource_name}")
    def read_resource_rest(resource_name: str) -> dict[str, object]:
        try:
            resource, contents = _read_resource_text(resource_name)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown resource: {resource_name}") from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"resource": resource, "contents": contents, "read_at": _utc_now()}

    @app.get("/mcp/sse")
    async def open_mcp_sse(request: Request) -> StreamingResponse:
        session_id, queue, endpoint_payload = _register_sse_session()

        async def event_stream() -> Any:
            try:
                yield _encode_sse("endpoint", endpoint_payload)
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        payload = await asyncio.wait_for(queue.get(), timeout=15.0)
                    except TimeoutError:
                        yield _encode_sse("ping", {"ts": _utc_now()})
                        continue
                    yield _encode_sse("message", payload)
            finally:
                sse_sessions.pop(session_id, None)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.post("/mcp/messages/{session_id}")
    async def post_mcp_sse_message(session_id: str, request: Request) -> JSONResponse:
        queue = sse_sessions.get(session_id)
        if queue is None:
            raise HTTPException(status_code=404, detail=f"Unknown MCP SSE session: {session_id}")
        try:
            body = await request.json()
        except Exception:
            payload = _jsonrpc_error(None, -32700, "Parse error")
            await queue.put(payload)
            return JSONResponse(payload, status_code=400)
        payload, status_code = await _handle_mcp_jsonrpc(body)
        if status_code != 204 and body.get("id") is not None:
            await queue.put(payload)
        return JSONResponse({"accepted": True, "session_id": session_id}, status_code=202)

    @app.post("/mcp/stream")
    async def mcp_streamable_http(request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}},
                status_code=400,
            )
        payload, status_code = await _handle_mcp_jsonrpc(body)
        if status_code == 204:
            return JSONResponse({}, status_code=204)
        return JSONResponse(payload, status_code=status_code)

    @app.post("/mcp/call/{tool_name}")
    async def call_tool_rest(tool_name: str, request: ToolCallRequest) -> dict[str, object]:
        if tool_name not in _TOOL_MAP:
            raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_name}")
        result = await _dispatch_and_wrap(tool_name, request.arguments)
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

        payload, status_code = await _handle_mcp_jsonrpc(body)
        if status_code == 204:
            return JSONResponse({}, status_code=204)
        return JSONResponse(payload, status_code=status_code)

    return app
