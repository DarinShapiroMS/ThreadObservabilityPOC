"""Triage / environment / pipeline-health tools (Phase 3).

Three high-signal entry points for a small-context model:

* :func:`start_triage` — single first-call tool. Returns environment + health +
  active issues + recommended next tool calls.
* :func:`get_environment` — bundles every version/identity surface today
  scattered across multiple tools.
* :func:`get_pipeline_health` — assesses whether SQLite-cached data is fresh
  and whether any pipeline stage has been failing silently.
"""

from __future__ import annotations

import platform
import sys
from datetime import UTC, datetime
from typing import Any

from ..health import build_health_snapshot
from ..pipeline.runner import get_runner_state
from ..storage.sqlite_store import get_store
from ..utils.datetime import parse_iso_datetime, utc_now_iso
from . import supervisor_client


# ---------------------------------------------------------------------------
# Environment bundle
# ---------------------------------------------------------------------------

async def get_environment(*, addon_version: str | None = None) -> dict[str, Any]:
    """Return a single bundle of every version/identity surface.

    Sections:
      addon         — this add-on (version, schema_version, mcp_protocol, python)
      home_assistant — Core + Supervisor versions, arch, channel
      otbr          — OpenThread Border Router add-on slug/version/state
      matter_server — Matter Server add-on slug/version/state
      network       — Thread network identity (name, pan_id, channel, leader)
      pipeline      — last tick id / completed_at / duration / failing stages
    """
    addon_section = _build_addon_section(addon_version)
    ha_section = await _build_ha_section()
    otbr_section, matter_section = await _build_companion_sections()
    network_section = _build_network_section()
    pipeline_section = _build_pipeline_section()

    return {
        "addon": addon_section,
        "home_assistant": ha_section,
        "otbr": otbr_section,
        "matter_server": matter_section,
        "network": network_section,
        "pipeline": pipeline_section,
    }


def _build_addon_section(addon_version: str | None) -> dict[str, Any]:
    store = get_store()
    return {
        "version": addon_version or "unknown",
        "schema_version": store.schema_version,
        "mcp_protocol_version": "2024-11-05",
        "python_version": platform.python_version(),
        "platform": f"{platform.system()} {platform.release()}",
        "executable": sys.executable,
    }


async def _build_ha_section() -> dict[str, Any]:
    try:
        core = await supervisor_client.get_core_info()
    except Exception as exc:  # noqa: BLE001
        core = {"error": str(exc)}
    try:
        sup = await supervisor_client.get_supervisor_info()
    except Exception as exc:  # noqa: BLE001
        sup = {"error": str(exc)}
    return {
        "core_version": core.get("version"),
        "core_state": core.get("state"),
        "core_arch": core.get("arch"),
        "supervisor_version": sup.get("version"),
        "supervisor_arch": sup.get("arch"),
        "supervisor_channel": sup.get("channel"),
        "timezone": sup.get("timezone"),
    }


_OTBR_NAME_HINTS = ("openthread", "otbr")
_MATTER_NAME_HINTS = ("matter_server", "matter-server", "matter server")


async def _build_companion_sections() -> tuple[dict[str, Any], dict[str, Any]]:
    """Locate OTBR and Matter Server addons via Supervisor /addons."""
    try:
        addons_list = await supervisor_client._get_json("/addons")  # noqa: SLF001
    except Exception as exc:  # noqa: BLE001
        err = {"error": str(exc)}
        return err, err
    items = addons_list.get("addons") if isinstance(addons_list, dict) else None
    if not isinstance(items, list):
        items = []
    otbr = _match_addon(items, _OTBR_NAME_HINTS)
    matter = _match_addon(items, _MATTER_NAME_HINTS)
    return otbr, matter


def _match_addon(items: list[dict[str, Any]], hints: tuple[str, ...]) -> dict[str, Any]:
    for it in items:
        slug = str(it.get("slug") or "").lower()
        name = str(it.get("name") or "").lower()
        for h in hints:
            if h in slug or h in name:
                return {
                    "slug": it.get("slug"),
                    "name": it.get("name"),
                    "version": it.get("version"),
                    "version_latest": it.get("version_latest"),
                    "state": it.get("state"),
                    "update_available": it.get("update_available"),
                }
    return {"slug": None, "found": False}


def _build_network_section() -> dict[str, Any]:
    store = get_store()
    networks = store.list_network_data()
    nodes = store.list_nodes()
    leaders: list[str] = [
        n["eui64"]
        for n in nodes
        if n.get("routing_role") == "leader" and n.get("eui64")
    ]
    partitions = sorted({pid for n in nodes if isinstance((pid := n.get("partition_id")), int)})
    primary: dict[str, Any] = {}
    if networks:
        first = networks[0]
        primary = {
            "network_name": first.get("network_name"),
            "extended_pan_id": first.get("extended_pan_id"),
            "pan_id": first.get("pan_id"),
            "channel": first.get("channel"),
            "partition_id": first.get("partition_id"),
            "mesh_local_prefix": first.get("mesh_local_prefix"),
            "observed_at": first.get("observed_at"),
        }
    return {
        **primary,
        "leader_eui64": leaders[0] if leaders else None,
        "all_leaders": leaders,
        "all_partitions": partitions,
        "dataset_count": len(networks),
    }


def _build_pipeline_section() -> dict[str, Any]:
    state = get_runner_state()
    return {
        "tick_count": state.get("tick_count"),
        "running": state.get("running"),
        "current_stage": state.get("current_stage"),
        "interval_seconds": state.get("interval_seconds"),
        "last_started_at": _maybe_iso(state.get("started_at")),
        "last_finished_at": _maybe_iso(state.get("finished_at")),
        "last_duration_seconds": state.get("duration_seconds"),
        "last_error": state.get("error"),
    }


def _maybe_iso(v: Any) -> str | None:
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, tz=UTC).isoformat()
    if isinstance(v, str):
        return v
    return None


# ---------------------------------------------------------------------------
# Pipeline health
# ---------------------------------------------------------------------------

def get_pipeline_health(*, limit: int = 20) -> dict[str, Any]:
    """Summarize the last N pipeline ticks.

    Returns:
      summary:
        last_tick_completed_at, last_tick_duration_s, consecutive_failed_ticks,
        stages_currently_failing, avg_duration_seconds, recent_tick_count
      recent_ticks: [the last N tick rows, newest first]
    """
    store = get_store()
    ticks = store.get_recent_pipeline_ticks(limit=max(1, min(int(limit), 200)))
    summary = _summarize_ticks(ticks)
    summary["current"] = _build_pipeline_section()
    return {"summary": summary, "recent_ticks": ticks}


def _summarize_ticks(ticks: list[dict[str, Any]]) -> dict[str, Any]:
    if not ticks:
        return {
            "recent_tick_count": 0,
            "consecutive_failed_ticks": 0,
            "stages_currently_failing": [],
            "avg_duration_seconds": None,
            "last_tick_completed_at": None,
            "last_tick_duration_s": None,
        }
    consecutive_failed = 0
    for t in ticks:  # newest first
        fail_count = int(t.get("fail_count") or 0)
        if fail_count > 0 or t.get("error"):
            consecutive_failed += 1
        else:
            break
    stages_failing: list[str] = []
    latest = ticks[0]
    stages = latest.get("stages") or {}
    if isinstance(stages, dict):
        for name, info in stages.items():
            if isinstance(info, dict) and info.get("ok") is False:
                stages_failing.append(name)
    durations = [
        float(t["duration_s"])
        for t in ticks
        if isinstance(t.get("duration_s"), (int, float))
    ]
    avg = round(sum(durations) / len(durations), 4) if durations else None
    return {
        "recent_tick_count": len(ticks),
        "consecutive_failed_ticks": consecutive_failed,
        "stages_currently_failing": stages_failing,
        "avg_duration_seconds": avg,
        "last_tick_completed_at": latest.get("completed_at"),
        "last_tick_duration_s": latest.get("duration_s"),
    }


# ---------------------------------------------------------------------------
# start_triage — first-call entry point
# ---------------------------------------------------------------------------

async def start_triage(*, addon_version: str | None = None) -> dict[str, Any]:
    """First-call tool for any triage session.

    Returns environment + health summary + active issues + the top 3 recommended
    next tool calls to drill in. The model uses `recommended_next` instead of
    choosing among 31 tools blind.
    """
    environment = await get_environment(addon_version=addon_version)
    health = build_health_snapshot()
    issues = get_store().list_active_issues()
    recommended = _build_recommendations(issues, health, environment)
    return {
        "as_of": utc_now_iso(),
        "environment": environment,
        "health": health,
        "active_issues_count": len(issues),
        "active_issues": issues[:10],  # cap to keep the response light
        "recommended_next": recommended,
    }


def _build_recommendations(
    issues: list[dict[str, Any]],
    health: dict[str, Any],
    environment: dict[str, Any],
) -> list[dict[str, Any]]:
    """Pick up to 3 high-value next-tool calls based on what the snapshot found."""
    out: list[dict[str, Any]] = []

    # 1. Issues drive drill-down via analyze_node on the affected EUI.
    for issue in issues:
        if len(out) >= 3:
            break
        eui = issue.get("eui64")
        kind = issue.get("kind") or "issue"
        if eui:
            out.append({
                "tool": "analyze_node",
                "arguments": {"eui64": eui},
                "reason": f"{kind} issue open on this node",
            })
        else:
            out.append({
                "tool": "lookup_playbook",
                "arguments": {"kind": kind},
                "reason": f"{kind} issue active without bound EUI — read playbook",
            })

    # 2. Pipeline stale or failing → recommend get_pipeline_health.
    pipeline = environment.get("pipeline") or {}
    last_finished = pipeline.get("last_finished_at")
    interval = pipeline.get("interval_seconds") or 0
    stale = False
    if last_finished:
        try:
            finished_dt = parse_iso_datetime(str(last_finished))
            if finished_dt is None:
                raise ValueError("invalid timestamp")
            age = (datetime.now(tz=UTC) - finished_dt).total_seconds()
            stale = interval and age > (interval * 3)
        except ValueError:
            stale = False
    if stale and len(out) < 3:
        out.append({
            "tool": "get_pipeline_health",
            "arguments": {},
            "reason": "pipeline appears stale (no recent tick); check stage failures",
        })

    # 3. No active issues + healthy pipeline → recommend the broad mesh view.
    if not out:
        out.append({
            "tool": "get_mesh_state",
            "arguments": {},
            "reason": "no active issues; review live mesh state",
        })

    return out[:3]
