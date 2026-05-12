"""Node metadata enrichment and display helpers.

Enriches canonical nodes from SQLite with friendly names, status inference,
RSSI/LQI trending, and device metadata. Provides both raw and UI-friendly
shapes for dashboards.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from ..storage.sqlite_store import SQLiteStore, get_store


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def get_node_display_name(node: dict[str, Any]) -> str:
    """Return friendly name or abbreviated EUI64."""
    if node.get("friendly_name"):
        return node["friendly_name"]
    eui = node.get("eui64", "")
    if len(eui) >= 4:
        return f"{eui[-4:].upper()}"
    return eui or "?"


def infer_node_status(node: dict[str, Any], stale_minutes: int = 60) -> str:
    """Infer status (healthy / stale / offline) based on last_seen.

    - healthy: last event within the last ``stale_minutes`` minutes
    - stale: last event older than ``stale_minutes`` but newer than 2x that
    - offline: no events or last event > 2x ``stale_minutes`` minutes ago
    """
    last_seen = node.get("last_seen")
    if not last_seen:
        return "offline"
    try:
        ts = datetime.fromisoformat(last_seen)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        age = datetime.now(tz=UTC) - ts
        threshold = timedelta(minutes=stale_minutes)
        if age < threshold:
            return "healthy"
        if age < threshold * 2:
            return "stale"
    except (ValueError, TypeError):
        pass
    return "offline"


def get_latest_signal_strength(node_eui64: str, store: SQLiteStore | None = None) -> dict[str, Any]:
    """Return RSSI/LQI for a node, preferring Matter cluster-53 link data.

    Matter NeighborTable entries (in the ``links`` table) give per-edge
    ``rssi_avg`` and ``lqi_in`` reported by routers about this node. We pick
    the strongest incoming edge (link where ``neighbor_eui64 = node``) as the
    representative signal. If no link data is available, fall back to the
    event log (legacy OTBR-log path).
    """
    s = store or get_store()

    # ---- prefer link-table data (Matter cluster 53) ----
    with s._lock:  # noqa: SLF001
        rows = s._conn.execute(  # noqa: SLF001
            "SELECT rssi_avg, rssi_last, lqi_in, lqi_out FROM links"
            " WHERE neighbor_eui64 = ?",
            (node_eui64,),
        ).fetchall()
    rssi_vals = [int(r["rssi_avg"]) for r in rows if r["rssi_avg"] is not None]
    lqi_vals = [int(r["lqi_in"]) for r in rows if r["lqi_in"] is not None]
    if rssi_vals or lqi_vals:
        best_rssi = max(rssi_vals) if rssi_vals else None  # highest = strongest
        best_lqi = max(lqi_vals) if lqi_vals else None
        return {
            "rssi": best_rssi,
            "lqi": best_lqi,
            "rssi_avg": (sum(rssi_vals) // len(rssi_vals)) if rssi_vals else None,
            "lqi_avg": (sum(lqi_vals) // len(lqi_vals)) if lqi_vals else None,
            "source": "links",
        }

    # ---- fallback: event log ----
    events = s.query_events(eui64=node_eui64, limit=100)
    rssi_samples = [e.get("rssi") for e in events if e.get("rssi") is not None]
    lqi_samples = [e.get("lqi") for e in events if e.get("lqi") is not None]
    return {
        "rssi": rssi_samples[0] if rssi_samples else None,
        "lqi": lqi_samples[0] if lqi_samples else None,
        "rssi_avg": sum(rssi_samples) // len(rssi_samples) if rssi_samples else None,
        "lqi_avg": sum(lqi_samples) // len(lqi_samples) if lqi_samples else None,
        "source": "events" if rssi_samples or lqi_samples else None,
    }


def get_node_summary(
    eui64: str,
    store: SQLiteStore | None = None,
    include_signal_strength: bool = True,
) -> dict[str, Any]:
    """Return a rich node summary with metadata, status, and signal info."""
    s = store or get_store()
    node = s.get_node(eui64)
    if not node:
        return {"eui64": eui64, "error": "node not found"}

    summary: dict[str, Any] = {
        "eui64": eui64,
        "friendly_name": node.get("friendly_name"),
        "display_name": get_node_display_name(node),
        "role": node.get("role"),
        "area": node.get("area_name") or node.get("area"),
        "area_id": node.get("area_id"),
        "area_name": node.get("area_name"),
        "manufacturer": node.get("manufacturer"),
        "model": node.get("model"),
        "sw_version": node.get("sw_version"),
        "hw_version": node.get("hw_version"),
        "ha_device_path": node.get("ha_device_path"),
        "device_id": node.get("device_id"),
        "first_seen": node.get("first_seen"),
        "last_seen": node.get("last_seen"),
        "status": node.get("status") or infer_node_status(node),
        "status_changed_at": node.get("status_changed_at"),
        "available": (
            None if node.get("available") is None
            else bool(node.get("available"))
        ),
        "availability_source": node.get("availability_source"),
        "availability_checked_at": node.get("availability_checked_at"),
        "last_referenced_at": node.get("last_referenced_at"),
    }

    if include_signal_strength:
        summary["signal_strength"] = get_latest_signal_strength(eui64, store=s)

    return summary


_ROUTING_ROLE_TO_KIND: dict[str | None, str] = {
    "leader": "router",
    "router": "router",
    "reed": "router",          # router-eligible end device — mains-powered
    "end_device": "fed",        # full thread device — mains, doesn't route
    "sleepy_end_device": "sed",
    "unassigned": "unknown",
    "unspecified": "unknown",
}


def classify_device_kind(routing_role: str | None) -> str:
    """Bucket a Matter routing_role into a UI-friendly device kind.

    - ``router``: mains-powered, participates in mesh routing (router/leader/reed)
    - ``fed``: full thread end device — mains, non-routing
    - ``sed``: sleepy end device — battery-powered
    - ``unknown``: not yet classified
    """
    return _ROUTING_ROLE_TO_KIND.get(routing_role, "unknown")


def _build_parent_map(s: SQLiteStore) -> dict[str, str]:
    """For every neighbor with ``is_child=1`` in any link row, record the
    reporter as that neighbor's parent. Returns ``{child_eui: parent_eui}``.
    """
    with s._lock:  # noqa: SLF001
        rows = s._conn.execute(  # noqa: SLF001
            "SELECT reporter_eui64, neighbor_eui64 FROM links"
            " WHERE is_child = 1 AND source = 'neighbor_table'"
        ).fetchall()
    return {r["neighbor_eui64"]: r["reporter_eui64"] for r in rows if r["neighbor_eui64"]}


def _build_router_peer_counts(s: SQLiteStore) -> dict[str, int]:
    """Count distinct router peers each reporter sees (is_child=0).

    Used to annotate router/leader rows with how many other routers they are
    directly meshed with in the partition.
    """
    with s._lock:  # noqa: SLF001
        rows = s._conn.execute(  # noqa: SLF001
            "SELECT reporter_eui64, COUNT(DISTINCT neighbor_eui64) AS n"
            "  FROM links"
            " WHERE source = 'neighbor_table' AND (is_child = 0 OR is_child IS NULL)"
            " GROUP BY reporter_eui64"
        ).fetchall()
    return {r["reporter_eui64"]: int(r["n"]) for r in rows if r["reporter_eui64"]}


def _build_router_peers(s: SQLiteStore) -> dict[str, list[str]]:
    """Return each reporter's distinct router peer EUIs (is_child=0).

    Returns ``{reporter_eui: [peer_eui, ...]}``, sorted by peer EUI for
    determinism.
    """
    with s._lock:  # noqa: SLF001
        rows = s._conn.execute(  # noqa: SLF001
            "SELECT DISTINCT reporter_eui64, neighbor_eui64 FROM links"
            " WHERE source = 'neighbor_table'"
            "   AND (is_child = 0 OR is_child IS NULL)"
            "   AND neighbor_eui64 IS NOT NULL"
            " ORDER BY reporter_eui64, neighbor_eui64"
        ).fetchall()
    out: dict[str, list[str]] = {}
    for r in rows:
        rep = r["reporter_eui64"]
        nei = r["neighbor_eui64"]
        if rep and nei:
            out.setdefault(rep, []).append(nei)
    return out


def _build_next_hop_to_otbr(
    s: SQLiteStore,
    nodes: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Compute each router's next-hop on the path toward the OTBR.

    For every router in the OTBR's partition we look at its route_table row
    pointing at the OTBR's EUI. That row has:
      - ``path_cost``: total cost to reach the OTBR
      - ``next_hop_router_id``: the directly-meshed peer to forward through

    Returns ``{reporter_eui: {"eui64", "name", "router_id", "path_cost",
    "is_direct"}}``. Direct-neighbor cases (next-hop == OTBR itself) are
    flagged with ``is_direct=True``.

    Missing prerequisites (no OTBR known, no router_id mapping) yield an
    empty dict — caller treats that as "next-hop view not available yet".
    """
    # Locate the OTBR.
    otbr = next(
        (n for n in nodes
         if n.get("role") == "border_router" and n.get("eui64") and n.get("partition_id") is not None),
        None,
    )
    if not otbr:
        return {}
    otbr_eui = otbr["eui64"]
    otbr_partition = otbr["partition_id"]
    otbr_router_id = otbr.get("router_id")

    # Build router_id -> (eui64, friendly_name) for this partition.
    router_by_id: dict[int, tuple[str, str | None]] = {}
    for n in nodes:
        if n.get("partition_id") != otbr_partition:
            continue
        rid = n.get("router_id")
        eui = n.get("eui64")
        if rid is None or not eui:
            continue
        router_by_id[int(rid)] = (eui, n.get("friendly_name") or get_node_display_name(n))

    # Pull the route_table rows pointing at the OTBR.
    with s._lock:  # noqa: SLF001
        rows = s._conn.execute(  # noqa: SLF001
            "SELECT reporter_eui64, path_cost, next_hop_router_id FROM links"
            " WHERE source = 'route_table' AND neighbor_eui64 = ?",
            (otbr_eui,),
        ).fetchall()

    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        reporter = r["reporter_eui64"]
        if not reporter or reporter == otbr_eui:
            continue
        next_hop_rid = r["next_hop_router_id"]
        path_cost = r["path_cost"]
        # Per Thread spec, NextHop == own RouterId means the destination is a
        # direct neighbor (no forwarding needed). NextHop == 63 (0x3F) means
        # "no route". Both cases collapse to "direct to OTBR" or "unknown".
        direct = False
        target_eui: str | None = None
        target_name: str | None = None
        target_rid: int | None = None
        if next_hop_rid is None or next_hop_rid == 63:
            # No NextHop recorded — treat as direct to OTBR if the row exists
            # at all (reporter has OTBR in its route table).
            direct = True
            target_eui = otbr_eui
            target_name = otbr.get("friendly_name") or otbr.get("display_name")
            target_rid = otbr_router_id if otbr_router_id is not None else None
        else:
            # If NextHop equals OTBR's RouterId, this reporter is a direct peer.
            if otbr_router_id is not None and int(next_hop_rid) == int(otbr_router_id):
                direct = True
                target_eui = otbr_eui
                target_name = otbr.get("friendly_name") or otbr.get("display_name")
                target_rid = int(otbr_router_id)
            else:
                resolved = router_by_id.get(int(next_hop_rid))
                if resolved:
                    target_eui, target_name = resolved
                    target_rid = int(next_hop_rid)
                else:
                    # Unknown next-hop router (we haven't seen its router_id yet).
                    target_rid = int(next_hop_rid)
        out[reporter] = {
            "eui64": target_eui,
            "name": target_name,
            "router_id": target_rid,
            "path_cost": int(path_cost) if path_cost is not None else None,
            "is_direct": direct,
        }
    return out


def list_nodes_enriched(
    store: SQLiteStore | None = None,
    include_signal_strength: bool = False,
    include_phantoms: bool = False,
) -> list[dict[str, Any]]:
    """Return all nodes with enrichment (status, display_name, device kind,
    parent router for sleepy/end devices, partition + peer count for routers).
    """
    s = store or get_store()
    nodes = s.list_nodes()
    parent_map = _build_parent_map(s)
    peer_counts = _build_router_peer_counts(s)
    peer_map = _build_router_peers(s)
    name_by_eui = {n["eui64"]: (n.get("friendly_name") or get_node_display_name(n))
                   for n in nodes if n.get("eui64")}
    # Map partition_id -> leader EUI by scanning nodes for routing_role == 'leader'
    leader_by_partition: dict[Any, str] = {}
    for n in nodes:
        if n.get("routing_role") == "leader" and n.get("partition_id") is not None and n.get("eui64"):
            leader_by_partition[n["partition_id"]] = n["eui64"]
    # Next-hop to OTBR per router. Empty dict if OTBR isn't ingested yet.
    next_hop_map = _build_next_hop_to_otbr(s, nodes)
    out: list[dict[str, Any]] = []
    for node in nodes:
        if not include_phantoms and node.get("status") == "phantom":
            continue
        eui = node.get("eui64")
        routing_role = node.get("routing_role")
        parent_eui = parent_map.get(eui) if eui else None
        partition_id = node.get("partition_id")
        leader_eui = leader_by_partition.get(partition_id) if partition_id is not None else None
        summary: dict[str, Any] = {
            "eui64": eui,
            "friendly_name": node.get("friendly_name"),
            "display_name": get_node_display_name(node),
            "role": node.get("role"),
            "routing_role": routing_role,
            "device_kind": classify_device_kind(routing_role),
            "area": node.get("area_name") or node.get("area"),
            "area_id": node.get("area_id"),
            "area_name": node.get("area_name"),
            "manufacturer": node.get("manufacturer"),
            "model": node.get("model"),
            "sw_version": node.get("sw_version"),
            "hw_version": node.get("hw_version"),
            "ha_device_path": node.get("ha_device_path"),
            "device_id": node.get("device_id"),
            "first_seen": node.get("first_seen"),
            "last_seen": node.get("last_seen"),
            "status": node.get("status") or infer_node_status(node),
            "status_changed_at": node.get("status_changed_at"),
            "last_referenced_at": node.get("last_referenced_at"),
            "available": (
                None if node.get("available") is None
                else bool(node.get("available"))
            ),
            "availability_source": node.get("availability_source"),
            "availability_checked_at": node.get("availability_checked_at"),
            "partition_id": partition_id,
            "partition_leader_eui64": leader_eui,
            "partition_leader_name": name_by_eui.get(leader_eui) if leader_eui else None,
            "router_id": node.get("router_id"),
            "router_peer_count": peer_counts.get(eui, 0) if eui else 0,
            "router_peers": [
                {"eui64": p, "name": name_by_eui.get(p)}
                for p in (peer_map.get(eui) or [])
            ] if eui else [],
            "parent_eui64": parent_eui,
            "parent_name": name_by_eui.get(parent_eui) if parent_eui else None,
            "next_hop_to_otbr": next_hop_map.get(eui) if eui else None,
        }
        if include_signal_strength and eui:
            summary["signal_strength"] = get_latest_signal_strength(eui, store=s)
        out.append(summary)
    return out
