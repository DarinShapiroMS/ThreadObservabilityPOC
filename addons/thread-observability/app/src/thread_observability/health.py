"""Shared health-snapshot computation.

Produces a consolidated view of network state used by both the HTTP API
(`/v1/health/snapshot`) and the MCP `get_health_snapshot` tool. All
inputs come from SQLite so the result is deterministic and cheap.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from .pipeline.nodes import list_nodes_enriched
from .storage.sqlite_store import SQLiteStore, get_store
from .utils.datetime import parse_iso_datetime

STALE_THRESHOLD_MIN = 5
OFFLINE_THRESHOLD_MIN = 30


def build_health_snapshot(*, store: SQLiteStore | None = None) -> dict[str, Any]:
    s = store or get_store()
    now = datetime.now(tz=UTC)
    stale_cutoff = (now - timedelta(minutes=STALE_THRESHOLD_MIN)).isoformat()
    offline_cutoff = (now - timedelta(minutes=OFFLINE_THRESHOLD_MIN)).isoformat()
    node_summaries = list_nodes_enriched(store=s, include_signal_strength=False, include_phantoms=False)

    with s._lock:  # noqa: SLF001
        newest = s._conn.execute(  # noqa: SLF001
            "SELECT MAX(ts) FROM events"
        ).fetchone()[0]
        # v0.9.46: count physical-hardware-identity tuples that appear
        # on more than one EUI64 row. This is the live signal for
        # "device was re-commissioned and the old identity wasn't
        # cleaned up", which presents as ghost nodes in the topology.
        dup_phys_row = s._conn.execute(  # noqa: SLF001
            """
            SELECT COALESCE(SUM(c), 0) AS rows_in_dup_groups,
                   COUNT(*) AS dup_group_count
            FROM (
                SELECT COUNT(*) AS c
                FROM nodes
                WHERE vendor_id IS NOT NULL
                  AND product_id IS NOT NULL
                  AND serial_number IS NOT NULL
                  AND COALESCE(status, '') != 'phantom'
                GROUP BY vendor_id, product_id, serial_number
                HAVING c > 1
            )
            """
        ).fetchone()
        # v0.9.46: count distinct Thread network identities present on
        # non-phantom nodes. >1 means at least one device is on stale
        # credentials.
        distinct_epids_row = s._conn.execute(  # noqa: SLF001
            """
            SELECT COUNT(DISTINCT extended_pan_id) AS c
            FROM nodes
            WHERE extended_pan_id IS NOT NULL
              AND COALESCE(status, '') != 'phantom'
            """
        ).fetchone()

    online = sleeping = stale = offline = 0
    for row in node_summaries:
        status = str(row.get("status") or "online").strip().lower()
        if status == "sleeping":
            sleeping += 1
            continue
        last_seen = row.get("last_seen")
        if not last_seen:
            offline += 1
        elif str(last_seen) < offline_cutoff:
            offline += 1
        elif str(last_seen) < stale_cutoff:
            stale += 1
        else:
            online += 1

    active = s.list_active_issues()
    by_sev: dict[str, int] = {}
    for i in active:
        by_sev[i["severity"]] = by_sev.get(i["severity"], 0) + 1

    data_age: float | None = None
    if newest:
        parsed_newest = parse_iso_datetime(str(newest))
        if parsed_newest is not None:
            data_age = (now - parsed_newest).total_seconds()

    overall = "ok"
    if by_sev.get("crit"):
        overall = "critical"
    elif by_sev.get("warn") or offline:
        overall = "degraded"

    return {
        "computed_at": now.isoformat(),
        "status": overall,
        "data_age_seconds": data_age,
        "summary": {
            "healthy_nodes": online,
            "online_nodes": online,
            "sleeping_nodes": sleeping,
            "stale_nodes": stale,
            "offline_nodes": offline,
            "total_nodes": len(node_summaries),
            "duplicate_physical_device_groups": int(dup_phys_row["dup_group_count"] or 0),
            "duplicate_physical_device_rows": int(dup_phys_row["rows_in_dup_groups"] or 0),
            "distinct_thread_networks": int(distinct_epids_row["c"] or 0),
        },
        "active_issues": {
            "count": len(active),
            "by_severity": by_sev,
        },
    }
