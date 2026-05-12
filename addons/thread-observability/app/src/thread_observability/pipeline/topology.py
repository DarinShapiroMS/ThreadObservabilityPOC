"""Topology graph builder for Thread Observability.

Builds a deterministic network snapshot from the SQLite ``nodes`` and
``links`` tables. Links are populated by ``discover_and_sync`` from the
Matter Thread Network Diagnostics cluster (NeighborTable + RouteTable),
which gives us true mesh adjacencies — not just IPv6 endpoints seen on
the wire.

Snapshot shape::

    {
        "computed_at": ISO8601,
        "freshness_minutes": int,
        "node_count": int,
        "link_count": int,
        "split": bool,                 # multiple distinct partition_ids
        "partitions": [
            {"partition_id": int, "leader_eui64": str|None,
             "member_count": int, "members": [eui64, ...]},
            ...
        ],
        "nodes": [
            {
                "eui64": str,
                "friendly_name": str | None,
                "role": str | None,
                "routing_role": str | None,
                "partition_id": int | None,
                "last_seen": ISO8601 | None,
                "parent_eui64": str | None,   # if known via is_child/event
                "last_rssi": int | None,
                "last_lqi": int | None,
                "stale": bool,
            },
            ...
        ],
        "links": [
            {
                "from": eui64,             # reporter
                "to":   eui64,             # neighbor
                "source": "neighbor_table"|"route_table",
                "rssi_avg": int | None,
                "lqi_in":   int | None,
                "lqi_out":  int | None,
                "is_child": int | None,
                "path_cost": int | None,
                "tags": [str, ...],        # "weak_link","high_error","asymmetric"
            },
            ...
        ],
    }
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from ..storage.sqlite_store import SQLiteStore, get_store

FRESHNESS_DEFAULT_MINUTES = 60
WEAK_LINK_RSSI_DBM = -85
HIGH_ERROR_PERCENT = 10  # frame_error_rate / message_error_rate threshold
ASYMMETRY_DB = 10        # |A->B rssi - B->A rssi| > this is asymmetric


def build_topology(
    *,
    freshness_minutes: int = FRESHNESS_DEFAULT_MINUTES,
    store: SQLiteStore | None = None,
) -> dict[str, Any]:
    """Return a topology snapshot computed from nodes + links tables."""
    s = store or get_store()
    now = datetime.now(tz=UTC)
    cutoff = (now - timedelta(minutes=freshness_minutes)).isoformat()

    with s._lock:  # noqa: SLF001 - intentional: same package
        rows = s._conn.execute(  # noqa: SLF001
            """
            SELECT
              n.eui64,
              n.friendly_name,
              n.role,
              n.routing_role,
              n.partition_id,
              n.leader_router_id,
              n.last_seen,
              (SELECT e.parent_eui64
                 FROM events e
                WHERE e.eui64 = n.eui64
                  AND e.type IN ('attach', 'parent_change')
                  AND e.ts >= ?
                ORDER BY e.ts DESC, e.id DESC
                LIMIT 1) AS parent_event_eui64,
              (SELECT e.rssi
                 FROM events e
                WHERE e.eui64 = n.eui64
                  AND e.rssi IS NOT NULL
                ORDER BY e.ts DESC, e.id DESC
                LIMIT 1) AS last_rssi,
              (SELECT e.lqi
                 FROM events e
                WHERE e.eui64 = n.eui64
                  AND e.lqi IS NOT NULL
                ORDER BY e.ts DESC, e.id DESC
                LIMIT 1) AS last_lqi
            FROM nodes n
            ORDER BY n.eui64
            """,
            (cutoff,),
        ).fetchall()

    all_links_raw = s.list_links()

    # Build asymmetry lookup: (reporter, neighbor, source) -> rssi_avg
    rssi_by_edge: dict[tuple[str, str, str], int] = {}
    for ln in all_links_raw:
        r = ln.get("rssi_avg")
        if isinstance(r, int):
            rssi_by_edge[(ln["reporter_eui64"], ln["neighbor_eui64"], ln["source"])] = r

    # Derive parent_eui64 from neighbor_table is_child=1 entries: if X reports Y
    # as a child, then Y's parent is X. (Mesh routers only see direct children.)
    parent_of: dict[str, str] = {}
    for ln in all_links_raw:
        if ln.get("source") == "neighbor_table" and ln.get("is_child"):
            parent_of[ln["neighbor_eui64"]] = ln["reporter_eui64"]

    nodes: list[dict[str, Any]] = []
    partitions: dict[int, list[str]] = {}
    leaders_by_partition: dict[int, str] = {}
    for row in rows:
        d = dict(row)
        last_seen = d.get("last_seen")
        stale = bool(last_seen and last_seen < cutoff)
        eui = d["eui64"]
        parent = parent_of.get(eui) or d.get("parent_event_eui64")
        pid = d.get("partition_id")
        if isinstance(pid, int):
            partitions.setdefault(pid, []).append(eui)
            if d.get("routing_role") == "leader":
                leaders_by_partition.setdefault(pid, eui)
        nodes.append(
            {
                "eui64": eui,
                "friendly_name": d.get("friendly_name"),
                "role": d.get("role"),
                "routing_role": d.get("routing_role"),
                "partition_id": pid,
                "leader_router_id": d.get("leader_router_id"),
                "last_seen": last_seen,
                "parent_eui64": parent,
                "last_rssi": d.get("last_rssi"),
                "last_lqi": d.get("last_lqi"),
                "stale": stale,
            }
        )

    # Project links with tags.
    links: list[dict[str, Any]] = []
    for ln in all_links_raw:
        rep = ln["reporter_eui64"]
        nei = ln["neighbor_eui64"]
        src = ln["source"]
        tags: list[str] = []
        rssi_avg = ln.get("rssi_avg")
        if isinstance(rssi_avg, int) and rssi_avg < WEAK_LINK_RSSI_DBM:
            tags.append("weak_link")
        fer = ln.get("frame_error_rate")
        mer = ln.get("message_error_rate")
        if (isinstance(fer, int) and fer > HIGH_ERROR_PERCENT) or (
            isinstance(mer, int) and mer > HIGH_ERROR_PERCENT
        ):
            tags.append("high_error")
        # Asymmetry: compare with the reverse-direction rssi if present.
        reverse = rssi_by_edge.get((nei, rep, src))
        if isinstance(rssi_avg, int) and isinstance(reverse, int):
            if abs(rssi_avg - reverse) > ASYMMETRY_DB:
                tags.append("asymmetric")
        links.append(
            {
                "from": rep,
                "to": nei,
                "source": src,
                "rssi_avg": rssi_avg,
                "rssi_last": ln.get("rssi_last"),
                "lqi_in": ln.get("lqi_in"),
                "lqi_out": ln.get("lqi_out"),
                "is_child": ln.get("is_child"),
                "age_seconds": ln.get("age_seconds"),
                "frame_error_rate": fer,
                "message_error_rate": mer,
                "path_cost": ln.get("path_cost"),
                "tags": tags,
            }
        )

    partition_summary = [
        {
            "partition_id": pid,
            "leader_eui64": leaders_by_partition.get(pid),
            "member_count": len(members),
            "members": members,
        }
        for pid, members in sorted(partitions.items())
    ]

    return {
        "computed_at": now.isoformat(),
        "freshness_minutes": freshness_minutes,
        "node_count": len(nodes),
        "link_count": len(links),
        "split": len(partitions) > 1,
        "partitions": partition_summary,
        "nodes": nodes,
        "links": links,
    }
