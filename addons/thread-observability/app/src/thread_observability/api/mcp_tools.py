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
from ..pipeline import nodes as nodes_mod
from ..pipeline import otbr_adapter
from ..pipeline import reasoner as reasoner_mod
from ..pipeline import topology as topology_mod
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
            "computed deterministically from the SQLite event log. By default, "
            "phantom nodes (no recent reference in any router's tables) are "
            "excluded."
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
        "name": "get_partition_state",
        "description": (
            "Return current Thread partition state. A healthy network has a single "
            "partition_id across all routers; multiple distinct partition_ids "
            "indicate a network split (mesh has fragmented into isolated groups). "
            "Phantom-only partitions are excluded by default."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "include_phantoms": {
                    "type": "boolean",
                    "description": "If true, include phantom nodes / phantom-only partitions. Default false.",
                    "default": False,
                },
            },
            "required": [],
        },
    },
    {
        "name": "list_phantom_nodes",
        "description": (
            "List nodes flagged as phantom — they exist in the SQLite nodes table "
            "(usually because they're in the HA device registry) but have not been "
            "observed in any router's NeighborTable or RouteTable within the staleness "
            "window. Returned rows include everything needed to find the device in "
            "Home Assistant for manual deletion: friendly_name, device_id, "
            "area, last_referenced_at, and a constructed HA deep-link path."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "list_active_issues",
        "description": "Return all currently-open Thread network issues from the SQLite issues table.",
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
        "name": "query_timeline",
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
                "eui64": {"type": "string"},
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
                    "default": 500,
                    "minimum": 1,
                    "maximum": 5000,
                },
            },
            "required": ["since"],
        },
    },
    {
        "name": "get_topology_snapshot",
        "description": (
            "Tier 4. Return a persisted topology snapshot row. Pass "
            "``snapshot_id`` to fetch one by id, or ``at`` (ISO-8601) "
            "to fetch the most-recent snapshot captured on or before "
            "that time. With no arguments, returns the newest snapshot."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "snapshot_id": {"type": "integer", "minimum": 1},
                "at": {"type": "string", "description": "ISO-8601 timestamp"},
            },
            "required": [],
        },
    },
    {
        "name": "list_topology_snapshots",
        "description": (
            "Tier 4. List topology snapshot summaries (id, captured_at, "
            "hash, partition_id, node_count, link_count) newest-first. "
            "Snapshot bodies are NOT returned — use ``get_topology_snapshot`` "
            "or ``diff_topology`` to drill in."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": "ISO-8601 lower bound"},
                "until": {"type": "string", "description": "ISO-8601 upper bound"},
                "limit": {
                    "type": "integer",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 1000,
                },
            },
            "required": [],
        },
    },
    {
        "name": "diff_topology",
        "description": (
            "Tier 4. Return a structured diff between two topology "
            "snapshots: added/removed nodes, per-node role/partition/parent "
            "transitions, and added/removed links. ``snapshot_id_a`` is the "
            "older / baseline, ``snapshot_id_b`` is the newer / candidate."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "snapshot_id_a": {"type": "integer", "minimum": 1},
                "snapshot_id_b": {"type": "integer", "minimum": 1},
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
                "playbook_id": {"type": "string"},
                "kind": {"type": "string"},
                "query": {"type": "string"},
            },
            "required": [],
        },
    },
    {
        "name": "analyze_node",
        "description": (
            "Tier 4 consultant tool. One-call structured payload for a "
            "single EUI-64: node metadata, parent + neighbors, open "
            "issues, recent closed issues, unified Tier 4 timeline "
            "(events + issue lifecycle + observer events), simple "
            "per-node baselines (parent_change rate this period vs. "
            "previous, status_change count), and full playbook entries "
            "matching the union of issue kinds. Use this instead of "
            "calling get_node_metadata + list_active_issues + "
            "query_events + lookup_playbook separately."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "eui64": {"type": "string"},
                "timeline_hours": {
                    "type": "integer",
                    "default": 24,
                    "minimum": 1,
                    "maximum": 720,
                },
                "baseline_days": {
                    "type": "integer",
                    "default": 7,
                    "minimum": 1,
                    "maximum": 90,
                },
            },
            "required": ["eui64"],
        },
    },
    {
        "name": "get_node_flap_history",
        "description": (
            "Return the per-node status_change history (online/offline/phantom "
            "transitions) emitted by recompute_node_statuses. Each call returns "
            "the most-recent transitions (newest first) plus an aggregate "
            "flap_counts map keyed by EUI. Use this to rank flappers: a high "
            "total in a short window points to an unstable router or to a "
            "device with multiple stale Matter operational identities. Available "
            "since v0.9.41; transitions before that release are not recorded."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "eui64": {
                    "type": "string",
                    "description": "Restrict history to a single EUI-64 (lower-case hex).",
                },
                "since": {
                    "type": "string",
                    "description": "ISO-8601 timestamp; only transitions at or after this time are returned.",
                },
                "limit": {
                    "type": "integer",
                    "default": 500,
                    "minimum": 1,
                    "maximum": 5000,
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_link_flap_history",
        "description": (
            "Return the per-edge link_acquired/link_lost history emitted by the "
            "Matter cluster-53 link sweep. Each call returns the most-recent "
            "transitions plus a flap_counts map keyed by the unordered "
            "(reporter, neighbor) pair so a symmetric flap surfaces once. Use "
            "this to rank unstable edges in the mesh: a high total over a short "
            "window indicates a marginal RF link or a sleepy child whose parent "
            "keeps timing it out. Available since v0.9.42; transitions before "
            "that release are not recorded."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "reporter_eui64": {
                    "type": "string",
                    "description": "Restrict to events where this EUI was the reporter.",
                },
                "neighbor_eui64": {
                    "type": "string",
                    "description": "Restrict to events where this EUI was the neighbor.",
                },
                "source": {
                    "type": "string",
                    "enum": ["neighbor_table", "route_table"],
                    "description": "Restrict to one Matter link source.",
                },
                "since": {
                    "type": "string",
                    "description": "ISO-8601 timestamp; only transitions at or after this time.",
                },
                "limit": {
                    "type": "integer",
                    "default": 500,
                    "minimum": 1,
                    "maximum": 5000,
                },
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
    {
        "name": "get_node_metadata",
        "description": (
            "Return enriched metadata for a Thread node: friendly name, role, area, "
            "device_id, first/last seen times, current status (healthy/stale/offline), "
            "and latest RSSI/LQI readings."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"eui64": {"type": "string", "description": "16-char hex node EUI64"}},
            "required": ["eui64"],
        },
    },
    {
        "name": "set_node_friendly_name",
        "description": (
            "Set or update a node's friendly name (e.g., 'Living Room Coordinator'). "
            "Returns the updated node record. Use this to make node identities human-readable."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "eui64": {"type": "string"},
                "name": {"type": "string"},
            },
            "required": ["eui64", "name"],
        },
    },
    {
        "name": "list_all_nodes",
        "description": (
            "Return all Thread network nodes with enrichment: friendly names, role, "
            "area, device_id, status (healthy/stale/offline), and first/last seen. "
            "Ordered by most-recently-seen first."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "discover_thread_devices",
        "description": (
            "Query Home Assistant's device registry for Thread/Zigbee devices and "
            "correlate IEEE addresses with extracted EUI64 nodes. Auto-populates "
            "friendly_name and device_id for matching nodes. Returns match summary."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
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
    "get_network_topology",
    "get_partition_state",
    "list_phantom_nodes",
    "list_active_issues",
    "get_health_snapshot",
    "get_recent_logs",
    "ha_get_addon_state",
    "ha_get_addon_logs",
    "ha_get_supervisor_logs",
    "ha_check_for_update",
    "list_thread_datasets",
    "get_storage_stats",
    "query_events",
    "query_timeline",
    "get_topology_snapshot",
    "list_topology_snapshots",
    "diff_topology",
    "list_playbooks",
    "lookup_playbook",
    "analyze_node",
    "get_node_flap_history",
    "get_link_flap_history",
    "get_config",
    "get_timeseries_health",
    "list_otbr_candidates",
    "get_ingest_state",
    "get_node_metadata",
    "list_all_nodes",
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
        "data_source": "sqlite_cache",
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
    if name == "get_network_topology":
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
        try:
            issues = get_store().list_active_issues()
            return {"count": len(issues), "issues": issues}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "get_partition_state":
        try:
            include_phantoms = bool(arguments.get("include_phantoms", False))
            return _build_partition_state(include_phantoms=include_phantoms)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "list_phantom_nodes":
        try:
            return _build_phantom_list()
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
    if name == "query_timeline":
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
    if name == "get_topology_snapshot":
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
    if name == "list_topology_snapshots":
        try:
            snaps = get_store().list_topology_snapshots(
                since=arguments.get("since"),
                until=arguments.get("until"),
                limit=int(arguments.get("limit", 100)),
            )
            return {"snapshots": snaps, "count": len(snaps)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "diff_topology":
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
    if name == "get_node_flap_history":
        try:
            return get_store().get_node_flap_history(
                eui64=arguments.get("eui64"),
                since=arguments.get("since"),
                limit=int(arguments.get("limit", 500)),
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "get_link_flap_history":
        try:
            return get_store().get_link_flap_history(
                reporter_eui64=arguments.get("reporter_eui64"),
                neighbor_eui64=arguments.get("neighbor_eui64"),
                source=arguments.get("source"),
                since=arguments.get("since"),
                limit=int(arguments.get("limit", 500)),
            )
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

    # ---- Node metadata tools (Phase 3) ----------------------------------
    if name == "get_node_metadata":
        try:
            eui64 = str(arguments.get("eui64", "")).strip()
            if not eui64:
                return {"error": "eui64 required"}
            return nodes_mod.get_node_summary(eui64, include_signal_strength=True)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "set_node_friendly_name":
        try:
            eui64 = str(arguments.get("eui64", "")).strip()
            name = str(arguments.get("name", "")).strip()
            if not eui64 or not name:
                return {"error": "eui64 and name required"}
            ok = get_store().set_node_friendly_name(eui64, name)
            if not ok:
                return {"error": f"node {eui64} not found"}
            return nodes_mod.get_node_summary(eui64, include_signal_strength=True)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
    if name == "list_all_nodes":
        try:
            return {
                "nodes": nodes_mod.list_nodes_enriched(include_signal_strength=True),
                "count": len(get_store().list_nodes()),
            }
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "nodes": []}
    if name == "discover_thread_devices":
        try:
            from ..pipeline import device_discovery
            return await device_discovery.discover_and_sync()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "matched": 0, "updated": 0}

    raise ValueError(f"Unknown tool: {name}")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

def create_mcp_app() -> FastAPI:
    app = FastAPI(title="Thread Observability MCP", version="0.1.0")

    # ── simple REST convenience endpoints ────────────────────────────────────

    @app.get("/")
    def root() -> dict[str, str]:
        return {"service": "mcp", "name": "thread-observability", "version": "0.9.5"}

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
                "serverInfo": {"name": "thread-observability", "version": "0.9.5"},
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
            result = await _dispatch_and_wrap(tool_name, arguments)
            import json as _json
            return ok({"content": [{"type": "text", "text": _json.dumps(result, default=str)}]})

        return err(-32601, f"Method not found: {method}")

    return app
