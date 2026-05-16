"""Tier 4: bundled `analyze_node` consultant tool.

Composes node metadata, current topology context, open + recent issues,
the unified Tier 4 timeline (Phase A), simple per-node baselines, and
matched playbook entries (Phase C) into a single response so an LLM
consultant can reason over a node with one MCP round-trip instead of
ten.

This is pure composition — no schema change.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from ..storage.sqlite_store import SQLiteStore, get_store
from . import playbooks as pb_mod
from . import timeline as timeline_mod


DEFAULT_TIMELINE_HOURS = 24
DEFAULT_BASELINE_DAYS = 7
DEFAULT_RECENT_ISSUE_LIMIT = 5


def _count_events_in_window(
    store: SQLiteStore,
    *,
    eui64: str,
    event_type: str,
    since: str,
) -> int:
    return len(
        store.query_events(
            eui64=eui64,
            event_type=event_type,
            since=since,
            limit=1000,
        )
    )


def _median(values: list[int]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _evidence_implicates_eui(evidence: Any, eui64: str) -> bool:
    """Return True if a global issue's evidence references ``eui64``.

    Currently scans the standard shape used by ``partition_split``::

        {"partitions": [{"members": [eui, ...], ...}, ...]}

    Plus a flat ``"members"`` array at the top level. Other global
    issue kinds can extend this without changing callers — we look
    for any obvious list-of-EUI in the evidence tree without
    over-fitting to one schema.
    """
    if not isinstance(evidence, dict):
        return False
    # Direct member list.
    members = evidence.get("members")
    if isinstance(members, list) and eui64 in members:
        return True
    involved = evidence.get("involved_eui64s")
    if isinstance(involved, list) and eui64 in involved:
        return True
    # Nested partitions[].members (partition_split shape).
    partitions = evidence.get("partitions")
    if isinstance(partitions, list):
        for part in partitions:
            if not isinstance(part, dict):
                continue
            part_members = part.get("members")
            if isinstance(part_members, list) and eui64 in part_members:
                return True
            sample = part.get("members_sample")
            if isinstance(sample, list) and eui64 in sample:
                return True
    recent_changes = evidence.get("recent_partition_changes")
    if isinstance(recent_changes, list):
        for row in recent_changes:
            if isinstance(row, dict) and row.get("eui64") == eui64:
                return True
    return False


def analyze_node(
    eui64: str,
    *,
    store: SQLiteStore | None = None,
    timeline_hours: int = DEFAULT_TIMELINE_HOURS,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a structured consultant payload for ``eui64``.

    Keys in the result:

    * ``node`` — full row from the ``nodes`` table (or ``None`` if not
      registered). When ``None`` we still return the other sections so
      a caller can see "phantom but referenced in events" cases.
    * ``parent`` — best-known parent eui64 + that parent's row, derived
      from the most recent neighbor_table entry with ``is_child=1`` or
      from a fallback attach/parent_change event.
    * ``neighbors`` — links rows where this node is the reporter.
    * ``open_issues`` — open issues for this eui (with suppression
      evidence preserved).
    * ``recent_issues`` — last N closed issues for this eui.
    * ``timeline`` — Tier 4 unified timeline rows (events + issue
      lifecycle + observer events) over the last ``timeline_hours``.
    * ``baselines`` — counts over the last ``baseline_days``:
      parent_change_count, attach_failed_count, offline_window_count,
      and a same-period comparison vs. the trailing window.
    * ``playbooks`` — full playbook entries matching the union of
      open + recent issue kinds.
    """
    s = store or get_store()
    now_dt = now or datetime.now(tz=UTC)
    timeline_since = (now_dt - timedelta(hours=timeline_hours)).isoformat()
    baseline_since = (now_dt - timedelta(days=baseline_days)).isoformat()

    node = s.get_node(eui64)

    # --- topology context ---------------------------------------------
    all_links = s.list_links()
    neighbors = [ln for ln in all_links if ln.get("reporter_eui64") == eui64]
    # Parent: who reports me with is_child=1? (Routers see only their direct
    # children, so this is authoritative when present.)
    parent_eui: str | None = None
    for ln in all_links:
        if ln.get("neighbor_eui64") == eui64 and ln.get("is_child"):
            parent_eui = ln.get("reporter_eui64")
            break
    parent_row = s.get_node(parent_eui) if parent_eui else None

    # --- issues -------------------------------------------------------
    # Direct match: issues whose eui64 column equals this node.
    direct_open = [i for i in s.list_active_issues() if i.get("eui64") == eui64]
    # v0.9.45: global issues (eui64 IS NULL) can still implicate a
    # specific node via their evidence — e.g. ``partition_split`` lists
    # the EUIs in each partition and a node sitting alone in a minority
    # partition is clearly the affected device. Bind those in by
    # scanning the evidence for membership references. This keeps the
    # consultant view useful for global-kind issues without changing
    # the issues schema.
    implicated_open: list[dict[str, Any]] = []
    for issue in s.list_active_issues():
        if issue.get("eui64"):
            continue
        if any(i.get("id") == issue.get("id") for i in direct_open):
            continue
        if _evidence_implicates_eui(issue.get("evidence"), eui64):
            implicated_open.append({**issue, "implicated_via": "evidence"})
    open_issues = direct_open + implicated_open
    # Recent closed issues — query a wide window and take last N.
    window_issues = s.list_issues_in_window(
        since=baseline_since,
        until=now_dt.isoformat(),
        eui64=eui64,
    )
    recent_issues = [
        i for i in window_issues if i.get("closed_at") is not None
    ][:DEFAULT_RECENT_ISSUE_LIMIT]

    # --- timeline -----------------------------------------------------
    tl = timeline_mod.query_timeline(
        s,
        since=timeline_since,
        until=now_dt.isoformat(),
        eui64=eui64,
        limit=500,
    )

    # --- baselines ----------------------------------------------------
    # Two windows: trailing baseline_days, and the matching prior period
    # of the same length immediately before it. Lets the consultant say
    # "this is N% higher than last week."
    prior_since = (now_dt - timedelta(days=2 * baseline_days)).isoformat()
    prior_until = baseline_since
    parent_change_recent = len(
        s.query_events(
            eui64=eui64,
            event_type="parent_change",
            since=baseline_since,
            limit=1000,
        )
    )
    parent_change_prior_all = s.query_events(
        eui64=eui64,
        event_type="parent_change",
        since=prior_since,
        limit=1000,
    )
    parent_change_prior = sum(
        1 for e in parent_change_prior_all if (e.get("ts") or "") < prior_until
    )
    status_change_recent = len(
        s.query_events(
            eui64=eui64,
            event_type="status_change",
            since=baseline_since,
            limit=1000,
        )
    )

    baselines = {
        "window_days": baseline_days,
        "parent_change_count_recent": parent_change_recent,
        "parent_change_count_prior": parent_change_prior,
        "parent_change_delta": parent_change_recent - parent_change_prior,
        "status_change_count_recent": status_change_recent,
    }

    # --- same-partition peer comparison (v0.11.28 local batch) -------
    peer_comparison: dict[str, Any] | None = None
    partition_id = node.get("partition_id") if isinstance(node, dict) else None
    if partition_id is not None:
        peers = [
            other for other in s.list_nodes()
            if other.get("eui64") != eui64 and other.get("partition_id") == partition_id
        ]
        if peers:
            peer_parent_changes = [
                {
                    "eui64": str(other.get("eui64") or ""),
                    "friendly_name": other.get("friendly_name"),
                    "parent_change_count_recent": _count_events_in_window(
                        s,
                        eui64=str(other.get("eui64") or ""),
                        event_type="parent_change",
                        since=baseline_since,
                    ),
                    "status_change_count_recent": _count_events_in_window(
                        s,
                        eui64=str(other.get("eui64") or ""),
                        event_type="status_change",
                        since=baseline_since,
                    ),
                }
                for other in peers
                if other.get("eui64")
            ]
            peer_parent_values = [row["parent_change_count_recent"] for row in peer_parent_changes]
            peer_status_values = [row["status_change_count_recent"] for row in peer_parent_changes]
            more_unstable = parent_change_recent > max(peer_parent_values, default=0)
            peer_comparison = {
                "partition_id": partition_id,
                "peer_count": len(peer_parent_changes),
                "subject_parent_change_count_recent": parent_change_recent,
                "peer_parent_change_count_median_recent": _median(peer_parent_values),
                "subject_status_change_count_recent": status_change_recent,
                "peer_status_change_count_median_recent": _median(peer_status_values),
                "more_unstable_than_partition_peers": more_unstable,
                "top_partition_peers_by_parent_change": sorted(
                    peer_parent_changes,
                    key=lambda row: row["parent_change_count_recent"],
                    reverse=True,
                )[:5],
            }

    # --- physical_identity (v0.9.46) ---------------------------------
    # Detect duplicate hardware: the same vendor_id/product_id/serial_number
    # tuple observed under multiple EUI64s. This happens when a device is
    # re-commissioned (Matter generates a new EUI64 each time) without
    # the old identity being cleaned up. Surfacing the duplicates lets
    # the consultant flag stale ghost rows.
    physical_identity: dict[str, Any] | None = None
    if node:
        vid = node.get("vendor_id")
        pid = node.get("product_id")
        sn = node.get("serial_number")
        if vid is not None and pid is not None and sn:
            other_instances = []
            for other in s.list_nodes():
                if other.get("eui64") == eui64:
                    continue
                if (
                    other.get("vendor_id") == vid
                    and other.get("product_id") == pid
                    and other.get("serial_number") == sn
                ):
                    other_instances.append({
                        "eui64": other.get("eui64"),
                        "friendly_name": other.get("friendly_name"),
                        "status": other.get("status"),
                        "last_seen": other.get("last_seen"),
                    })
            physical_identity = {
                "vendor_id": vid,
                "product_id": pid,
                "serial_number": sn,
                "duplicate_count": len(other_instances) + 1,
                "other_instances": other_instances,
            }

    # --- playbooks ----------------------------------------------------
    kinds = {
        i.get("kind") for i in open_issues if i.get("kind")
    } | {i.get("kind") for i in recent_issues if i.get("kind")}
    matched_playbooks = pb_mod.lookup_for_kinds(kinds) if kinds else []

    return {
        "eui64": eui64,
        "as_of": now_dt.isoformat(),
        "node": node,
        "parent": {"eui64": parent_eui, "row": parent_row} if parent_eui else None,
        "neighbors": neighbors,
        "open_issues": open_issues,
        "recent_issues": recent_issues,
        "timeline": tl,
        "baselines": baselines,
        "peer_comparison": peer_comparison,
        "playbooks": matched_playbooks,
        "matched_issue_kinds": sorted(k for k in kinds if k),
        "physical_identity": physical_identity,
    }
