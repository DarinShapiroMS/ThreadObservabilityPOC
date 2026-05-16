"""Deterministic Thread "issues" reasoner (redesigned rules).

An issue is a falsifiable claim about state that narrows diagnosis. Each
open issue is continually re-observed on each reasoner run (bumping its
``last_seen_at``) and auto-closed when its predicate no longer holds.

Rule set (v2 - redesigned):

* ``real_partition_split`` (warn/crit) — multiple *live* partitions plus
  evidence of a device transitioning between partitions recently, with no
  router-router neighbor link bridging partitions.
* ``dead_link_reference`` (warn) — a router references an unknown EUI64 in
  NeighborTable/RouteTable, persisted across N ingestion ticks.
* ``route_to_otbr_unreachable`` (warn/crit) — walking the forwarding path
  from a router to the OTBR terminates in a loop, unknown next hop, or
  missing route-table entry.

Severity is a function of actionability × freshness. Rules compute a base
severity from actionability, then the reasoner demotes long-lived issues so
new anomalies rise to the top.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import json

from ..storage.sqlite_store import SQLiteStore, get_store
from . import routing as routing_mod

# Optional emergency switch for ops. The API/MCP endpoints do not return a
# placeholder when paused; they simply surface an empty list.
ISSUES_PAUSED = False

# --- rule parameters ---------------------------------------------------------

PARTITION_CHANGE_WINDOW_MIN = 30

DEAD_LINK_MIN_TICKS = 3
DEAD_LINK_OBS_EXPIRY_MIN = 10

# Freshness demotion thresholds. The intent is "actionability × freshness",
# not "noise": a month-old issue is less urgent than the same anomaly
# appearing in the last ingest.
FRESH_DEMOTE_WARN_AFTER_HOURS = 24
FRESH_DEMOTE_INFO_AFTER_DAYS = 30


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:  # noqa: BLE001
        return None


def _demote_severity(*, base: str, first_seen_at: str | None, now: datetime) -> str:
    """Demote long-lived issues so fresh ones sort higher."""
    base_norm = str(base or "").strip().lower()
    if base_norm not in {"crit", "warn", "info"}:
        base_norm = "warn"

    first_dt = _parse_iso(first_seen_at)
    if first_dt is None:
        return base_norm

    age = now - first_dt
    if age >= timedelta(days=FRESH_DEMOTE_INFO_AFTER_DAYS):
        return "info"
    if age >= timedelta(hours=FRESH_DEMOTE_WARN_AFTER_HOURS):
        return "warn" if base_norm == "crit" else "info"
    return base_norm


def _issue_key(kind: str, eui64: str | None) -> tuple[str, str | None]:
    return (str(kind or "").strip(), (str(eui64).strip().lower() if eui64 else None))


def _compute_real_partition_split(
    *,
    store: SQLiteStore,
    now: datetime,
) -> dict[str, Any] | None:
    cutoff = _iso(now - timedelta(minutes=PARTITION_CHANGE_WINDOW_MIN))

    with store._lock:  # noqa: SLF001
        node_rows = store._conn.execute(  # noqa: SLF001
            """
            SELECT eui64, partition_id, routing_role, role, status, extended_pan_id, network_name
              FROM nodes
             WHERE partition_id IS NOT NULL
               AND COALESCE(status, '') != 'phantom'
            """
        ).fetchall()
        change_rows = store._conn.execute(  # noqa: SLF001
            "SELECT ts, eui64, payload_json FROM events"
            " WHERE type = 'partition_change' AND ts >= ?"
            " ORDER BY ts DESC, id DESC",
            (cutoff,),
        ).fetchall()
        link_rows = store._conn.execute(  # noqa: SLF001
            """
            SELECT reporter_eui64, neighbor_eui64
              FROM links
             WHERE source = 'neighbor_table'
               AND neighbor_known = 1
            """
        ).fetchall()

    nodes = [dict(r) for r in node_rows]
    node_by_eui: dict[str, dict[str, Any]] = {
        str(n.get("eui64")).lower(): n for n in nodes if n.get("eui64")
    }

    partitions: dict[int, list[str]] = {}
    leaders: dict[int, str] = {}
    for n in nodes:
        pid = n.get("partition_id")
        eui = n.get("eui64")
        if not isinstance(pid, int) or not isinstance(eui, str) or not eui:
            continue
        partitions.setdefault(pid, []).append(eui.lower())
        if n.get("routing_role") == "leader":
            leaders.setdefault(pid, eui.lower())

    if len(partitions) <= 1:
        return None

    recent_changes: list[dict[str, Any]] = []
    changed_euis: set[str] = set()
    for row in change_rows:
        eui = str(row["eui64"] or "").lower()
        if not eui:
            continue
        try:
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        except Exception:  # noqa: BLE001
            payload = {}
        if not isinstance(payload, dict):
            continue
        from_pid = payload.get("from")
        to_pid = payload.get("to")
        if not isinstance(from_pid, int) or not isinstance(to_pid, int):
            continue
        if from_pid == to_pid:
            continue
        if from_pid not in partitions or to_pid not in partitions:
            continue
        changed_euis.add(eui)
        recent_changes.append(
            {
                "ts": row["ts"],
                "eui64": eui,
                "from_partition_id": from_pid,
                "to_partition_id": to_pid,
            }
        )
        if len(recent_changes) >= 10:
            break

    if not recent_changes:
        return None

    # Guard against false-positive "multiple partitions" caused by stale or
    # mismatched partition stamps: if we see any router-router neighbor edge
    # crossing partition IDs, treat the partition identifiers as suspect and
    # do not fire this issue.
    bridging_router_links: list[dict[str, Any]] = []
    for row in link_rows:
        a = str(row["reporter_eui64"] or "").lower()
        b = str(row["neighbor_eui64"] or "").lower()
        if not a or not b:
            continue
        na = node_by_eui.get(a)
        nb = node_by_eui.get(b)
        if not na or not nb:
            continue
        ra = na.get("routing_role")
        rb = nb.get("routing_role")
        if ra not in {"router", "leader"} or rb not in {"router", "leader"}:
            continue
        pa = na.get("partition_id")
        pb = nb.get("partition_id")
        if pa is None or pb is None or pa == pb:
            continue
        bridging_router_links.append({"a": a, "b": b, "a_partition_id": pa, "b_partition_id": pb})
        if len(bridging_router_links) >= 5:
            break

    if bridging_router_links:
        return None

    partition_summary: list[dict[str, Any]] = []
    for pid, members in sorted(partitions.items()):
        sample = members[:50]
        # Capture partition identity from any node that reported it.
        epid = None
        netname = None
        for e in sample:
            n = node_by_eui.get(e)
            if not n:
                continue
            if epid is None and n.get("extended_pan_id"):
                epid = n.get("extended_pan_id")
            if netname is None and n.get("network_name"):
                netname = n.get("network_name")
        partition_summary.append(
            {
                "partition_id": pid,
                "leader_eui64": leaders.get(pid),
                "member_count": len(members),
                "members_sample": sample,
                "network_name": netname,
                "extended_pan_id": epid,
            }
        )

    evidence = {
        "observation": {
            "partition_count": len(partitions),
            "recent_partition_change_count": len(recent_changes),
            "window_minutes": PARTITION_CHANGE_WINDOW_MIN,
            "router_bridge_link_count": 0,
        },
        "partitions": partition_summary,
        "recent_partition_changes": recent_changes,
        "involved_eui64s": sorted(set(changed_euis) | {e for e in leaders.values() if e}),
        "cleared_when": (
            "partition_count <= 1 OR no partition_change events in the last "
            f"{PARTITION_CHANGE_WINDOW_MIN} minutes OR a router-router neighbor link bridges partitions"
        ),
    }
    return {"kind": "real_partition_split", "eui64": None, "base_severity": "crit", "evidence": evidence}


def _compute_dead_link_reference(
    *,
    store: SQLiteStore,
    now_iso: str,
) -> list[dict[str, Any]]:
    stale_links = store.list_stale_links()
    mature_by_reporter: dict[str, list[dict[str, Any]]] = {}
    for link in stale_links:
        reporter = str(link.get("reporter_eui64") or "").lower()
        neighbor = str(link.get("neighbor_eui64") or "").lower()
        source = str(link.get("source") or "").strip() or "unknown"
        if not reporter or not neighbor:
            continue
        obs_key = f"dead_link_reference|{reporter}|{neighbor}|{source}"
        obs = store.bump_issue_observation(
            obs_key=obs_key,
            kind="dead_link_reference",
            subject_eui64=reporter,
            payload={
                "reporter_eui64": reporter,
                "neighbor_eui64": neighbor,
                "source": source,
                "partition_id": link.get("partition_id"),
            },
            now=now_iso,
        )
        if int(obs.get("seen_count") or 0) < DEAD_LINK_MIN_TICKS:
            continue
        mature_by_reporter.setdefault(reporter, []).append(
            {
                "neighbor_eui64": neighbor,
                "source": source,
                "partition_id": link.get("partition_id"),
                "seen_count": int(obs.get("seen_count") or 0),
                "first_seen_at": obs.get("first_seen_at"),
                "last_seen_at": obs.get("last_seen_at"),
            }
        )

    out: list[dict[str, Any]] = []
    for reporter, refs in mature_by_reporter.items():
        refs.sort(key=lambda r: (-int(r.get("seen_count") or 0), str(r.get("neighbor_eui64") or "")))
        evidence = {
            "observation": {
                "mature_reference_count": len(refs),
                "min_ticks": DEAD_LINK_MIN_TICKS,
            },
            "references": refs[:25],
            "involved_eui64s": sorted({reporter} | {r["neighbor_eui64"] for r in refs if r.get("neighbor_eui64")}),
            "cleared_when": "no unknown-neighbor link references persist for >= min_ticks",
        }
        out.append(
            {
                "kind": "dead_link_reference",
                "eui64": reporter,
                "base_severity": "warn",
                "evidence": evidence,
            }
        )
    return out


def _compute_route_to_otbr_unreachable(
    *,
    store: SQLiteStore,
) -> list[dict[str, Any]]:
    # Only routers/leaders participate in route-to-OTBR next-hop chains.
    nodes = [
        n for n in store.list_nodes()
        if n.get("eui64")
        and n.get("routing_role") in {"router", "leader"}
        and n.get("status") != "phantom"
    ]
    out: list[dict[str, Any]] = []
    for n in nodes:
        eui = str(n.get("eui64") or "").lower()
        if not eui:
            continue
        walk = routing_mod.walk_route_to_otbr(eui, store=store)
        issues = walk.get("issues") if isinstance(walk.get("issues"), list) else []
        codes = {str(i.get("code") or "") for i in issues if isinstance(i, dict)}
        interesting = [
            i for i in issues
            if isinstance(i, dict)
            and str(i.get("code") or "") in {"loop_detected", "unknown_next_hop", "no_route_to_otbr", "max_hops_exceeded"}
        ]
        if not interesting:
            continue
        base = "crit" if ("loop_detected" in codes or "unknown_next_hop" in codes) else "warn"
        evidence = {
            "observation": {
                "issue_codes": sorted({str(i.get("code") or "") for i in interesting}),
                "issue_count": len(interesting),
            },
            "route_walk": walk,
            "involved_eui64s": sorted({eui, str(walk.get("otbr_eui64") or "").lower()} - {""}),
            "cleared_when": "walk_route_to_otbr reports no loop/unknown-next-hop/no-route issues",
        }
        out.append(
            {
                "kind": "route_to_otbr_unreachable",
                "eui64": eui,
                "base_severity": base,
                "evidence": evidence,
            }
        )
    return out


def run_reasoner(
    *,
    now: datetime | None = None,
    store: SQLiteStore | None = None,
) -> dict[str, Any]:
    """Run every rule once and reconcile open issues."""
    s = store or get_store()
    now_dt = now or datetime.now(tz=UTC)
    now_iso = _iso(now_dt)

    if ISSUES_PAUSED:
        return {
            "status": "paused",
            "opened": [],
            "closed": [],
            "still_open": [],
            "computed_at": now_iso,
        }

    managed_kinds = {
        "real_partition_split",
        "dead_link_reference",
        "route_to_otbr_unreachable",
    }

    # Snapshot existing open issues for managed kinds so severity demotion can
    # read ``first_seen_at`` and we can tell whether an id is newly opened.
    existing: dict[tuple[str, str | None], dict[str, Any]] = {}
    for issue in s.list_active_issues():
        kind = str(issue.get("kind") or "")
        if kind not in managed_kinds:
            continue
        existing[_issue_key(kind, issue.get("eui64"))] = issue

    observations: list[dict[str, Any]] = []
    split = _compute_real_partition_split(store=s, now=now_dt)
    if split is not None:
        observations.append(split)
    observations.extend(_compute_dead_link_reference(store=s, now_iso=now_iso))
    observations.extend(_compute_route_to_otbr_unreachable(store=s))

    opened: list[int] = []
    still_open: list[int] = []
    seen_keys: set[tuple[str, str | None]] = set()

    for obs in observations:
        kind = str(obs.get("kind") or "")
        eui64 = obs.get("eui64")
        base = str(obs.get("base_severity") or "warn")
        evidence = obs.get("evidence") if isinstance(obs.get("evidence"), dict) else {}
        key = _issue_key(kind, eui64)
        prior = existing.get(key)
        severity = _demote_severity(
            base=base,
            first_seen_at=(prior.get("first_seen_at") if isinstance(prior, dict) else None),
            now=now_dt,
        )
        issue_id = s.open_issue(kind=kind, severity=severity, eui64=eui64, evidence=evidence)
        seen_keys.add(key)
        if prior is None:
            opened.append(issue_id)
        else:
            still_open.append(issue_id)

    closed: list[int] = []
    for key, issue in existing.items():
        if key in seen_keys:
            continue
        try:
            iid = int(issue.get("id"))
        except Exception:  # noqa: BLE001
            continue
        if s.close_issue(iid):
            closed.append(iid)

    # Best-effort cleanup of dead_link observation rows.
    try:
        s.sweep_issue_observations(
            kind="dead_link_reference",
            last_seen_before=_iso(now_dt - timedelta(minutes=DEAD_LINK_OBS_EXPIRY_MIN)),
        )
    except Exception:  # noqa: BLE001
        pass

    return {
        "status": "ok",
        "opened": opened,
        "closed": closed,
        "still_open": still_open,
        "computed_at": now_iso,
        "rules": {
            "real_partition_split": {"window_minutes": PARTITION_CHANGE_WINDOW_MIN},
            "dead_link_reference": {"min_ticks": DEAD_LINK_MIN_TICKS},
            "route_to_otbr_unreachable": {},
        },
    }

