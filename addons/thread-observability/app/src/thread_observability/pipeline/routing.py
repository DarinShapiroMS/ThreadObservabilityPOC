"""Server-side Thread mesh routing primitives.

Responsibilities that used to be reconstructed in the dashboard JS but
belong on the server (so AI / MCP / external consumers get them too):

* ``find_otbr`` — locate the Thread Border Router EUI.
* ``walk_route_to_otbr`` — walk the next-hop chain from any router to the
  OTBR, with cycle detection, link-quality per hop, and an ``issues`` list.
* ``list_neighbors_enriched`` — return a router's NeighborTable rows with
  friendly names resolved and "is this neighbor still meshed" derived
  from the partition stamp on the link row.

These functions are pure reads from the SQLite store. They make no
network calls and can be invoked freely from API handlers.
"""

from __future__ import annotations

from typing import Any

from ..storage.sqlite_store import SQLiteStore, get_store
from .nodes import get_node_display_name


# NextHopRouterId sentinel meaning "no route allocated" per Thread spec.
NO_ROUTE_RID = 63

# Cycle-detection safety bound (Thread has a hard cap of 32 routers per
# partition; any chain exceeding this is malformed).
MAX_HOPS = 32


def find_otbr(store: SQLiteStore | None = None) -> dict[str, Any] | None:
    """Return the OTBR node row (as a dict) or ``None`` if not yet ingested.

    Preference order:
      1. A node with ``role == 'border_router'`` and a known ``partition_id``.
      2. A node with ``routing_role == 'leader'`` and no ``next_hop`` rows
         pointing away (fallback for cold boots before OTBR REST ingest).
    """
    s = store or get_store()
    nodes = s.list_nodes()
    for n in nodes:
        if n.get("role") == "border_router" and n.get("partition_id") is not None:
            return n
    # Fallback: the partition leader is usually the OTBR in single-OTBR meshes.
    for n in nodes:
        if n.get("routing_role") == "leader" and n.get("partition_id") is not None:
            return n
    return None


def _router_id_index(nodes: list[dict[str, Any]], partition_id: Any) -> dict[int, dict[str, Any]]:
    """Build ``{router_id: node_row}`` for the given partition."""
    out: dict[int, dict[str, Any]] = {}
    for n in nodes:
        if n.get("partition_id") != partition_id:
            continue
        rid = n.get("router_id")
        if rid is None:
            continue
        try:
            out[int(rid)] = n
        except (TypeError, ValueError):
            continue
    return out


def walk_route_to_otbr(
    source_eui: str,
    *,
    store: SQLiteStore | None = None,
) -> dict[str, Any]:
    """Walk the multi-hop forwarding path from ``source_eui`` to the OTBR.

    Returns a structured chain so AI / external consumers don't have to
    re-implement next-hop resolution against the raw ``links`` table::

        {
            "source_eui64": str,
            "otbr_eui64":   str | None,
            "complete":     bool,
            "hop_count":    int,
            "hops": [
                {
                    "eui64":       str,
                    "name":        str | None,
                    "router_id":   int | None,
                    "path_cost":   int | None,   # cost reported by previous hop
                    "lqi_in":      int | None,
                    "lqi_out":     int | None,
                    "link_established": bool | None,
                    "is_otbr":     bool,
                },
                ...
            ],
            "issues": [
                {"code": "no_otbr" | "no_route_to_otbr" | "loop_detected"
                       | "unknown_next_hop" | "different_partition"
                       | "self_is_otbr",
                 "detail": str},
                ...
            ],
        }

    ``complete=True`` iff the final hop is the OTBR. Partial paths (chain
    terminates early because a NextHop RouterId can't be resolved, or the
    reporter has no route_table entry for the OTBR) are still returned —
    consumers should not assume completeness.
    """
    s = store or get_store()
    nodes = s.list_nodes()
    name_by_eui = {n["eui64"]: (n.get("friendly_name") or get_node_display_name(n))
                   for n in nodes if n.get("eui64")}

    otbr = find_otbr(s)
    out: dict[str, Any] = {
        "source_eui64": source_eui,
        "otbr_eui64": otbr["eui64"] if otbr else None,
        "complete": False,
        "hop_count": 0,
        "hops": [],
        "issues": [],
    }
    if not otbr:
        out["issues"].append({"code": "no_otbr", "detail": "OTBR not yet discovered"})
        return out

    otbr_eui = otbr["eui64"]
    otbr_partition = otbr.get("partition_id")
    otbr_router_id = otbr.get("router_id")

    if source_eui == otbr_eui:
        out["issues"].append({"code": "self_is_otbr", "detail": "source is the OTBR itself"})
        out["complete"] = True
        out["hops"].append({
            "eui64": otbr_eui,
            "name": name_by_eui.get(otbr_eui),
            "router_id": otbr_router_id,
            "path_cost": 0,
            "lqi_in": None,
            "lqi_out": None,
            "link_established": None,
            "is_otbr": True,
        })
        out["hop_count"] = 1
        return out

    source_node = next((n for n in nodes if n.get("eui64") == source_eui), None)
    if source_node and source_node.get("partition_id") != otbr_partition:
        out["issues"].append({
            "code": "different_partition",
            "detail": f"source partition {source_node.get('partition_id')} "
                      f"!= OTBR partition {otbr_partition}",
        })
        # Don't try to walk — partition-scoped router IDs aren't comparable.
        return out

    router_by_id = _router_id_index(nodes, otbr_partition)

    # Seed the chain with the source.
    out["hops"].append({
        "eui64": source_eui,
        "name": name_by_eui.get(source_eui),
        "router_id": source_node.get("router_id") if source_node else None,
        "path_cost": None,
        "lqi_in": None,
        "lqi_out": None,
        "link_established": None,
        "is_otbr": False,
    })

    seen: set[str] = {source_eui}
    current_eui = source_eui

    for _ in range(MAX_HOPS):
        # Fetch the route_table row from current → OTBR.
        with s._lock:  # noqa: SLF001
            row = s._conn.execute(  # noqa: SLF001
                "SELECT path_cost, next_hop_router_id, lqi_in, lqi_out,"
                "       link_established"
                "  FROM links"
                " WHERE source = 'route_table'"
                "   AND reporter_eui64 = ?"
                "   AND neighbor_eui64 = ?",
                (current_eui, otbr_eui),
            ).fetchone()
        if row is None:
            out["issues"].append({
                "code": "no_route_to_otbr",
                "detail": f"{current_eui} has no route_table entry for OTBR",
            })
            return out
        nh_rid = row["next_hop_router_id"]
        path_cost = row["path_cost"]
        lqi_in = row["lqi_in"]
        lqi_out = row["lqi_out"]
        link_est = row["link_established"]
        link_est_b: bool | None = bool(link_est) if link_est is not None else None

        # Direct-neighbor short-circuit. OpenThread fills NextHopRouterId on
        # RouteTable rows even when the reporter has a direct MLE link to
        # the destination — the field names the route-advertisement relay
        # that last gossiped this route, not the actual forwarding next
        # hop. ``PathCost == 1`` together with ``LinkEstablished == 1`` is
        # the authoritative "direct link in use" signal; otherwise we'd
        # render phantom A→B→A loops between any two routers that both
        # reach the OTBR directly.
        is_direct = (
            path_cost is not None
            and int(path_cost) == 1
            and link_est_b is True
        )
        if is_direct:
            nh_rid = None

        # Resolve the next hop's EUI.
        next_eui: str
        next_rid: int | None
        if nh_rid is None or nh_rid == NO_ROUTE_RID:
            # NextHop unset → destination is a direct neighbor.
            next_eui = otbr_eui
            next_rid = otbr_router_id
        elif otbr_router_id is not None and int(nh_rid) == int(otbr_router_id):
            next_eui = otbr_eui
            next_rid = int(nh_rid)
        else:
            resolved = router_by_id.get(int(nh_rid))
            if not resolved or not resolved.get("eui64"):
                out["issues"].append({
                    "code": "unknown_next_hop",
                    "detail": f"router_id {nh_rid} not in partition router index",
                })
                # Append a placeholder hop so AI can see the dangling rid.
                out["hops"].append({
                    "eui64": None,
                    "name": None,
                    "router_id": int(nh_rid),
                    "path_cost": int(path_cost) if path_cost is not None else None,
                    "lqi_in": lqi_in,
                    "lqi_out": lqi_out,
                    "link_established": link_est_b,
                    "is_otbr": False,
                })
                out["hop_count"] = len(out["hops"])
                return out
            next_eui = resolved["eui64"]
            next_rid = int(nh_rid)

        if next_eui in seen:
            out["issues"].append({
                "code": "loop_detected",
                "detail": f"next-hop {next_eui} already in path",
            })
            out["hop_count"] = len(out["hops"])
            return out
        seen.add(next_eui)

        is_otbr = next_eui == otbr_eui
        out["hops"].append({
            "eui64": next_eui,
            "name": name_by_eui.get(next_eui),
            "router_id": next_rid,
            "path_cost": int(path_cost) if path_cost is not None else None,
            "lqi_in": lqi_in,
            "lqi_out": lqi_out,
            "link_established": link_est_b,
            "is_otbr": is_otbr,
        })

        if is_otbr:
            out["complete"] = True
            out["hop_count"] = len(out["hops"])
            return out

        current_eui = next_eui

    out["issues"].append({
        "code": "max_hops_exceeded",
        "detail": f"path did not terminate within {MAX_HOPS} hops",
    })
    out["hop_count"] = len(out["hops"])
    return out


def list_neighbors_enriched(
    reporter_eui: str,
    *,
    store: SQLiteStore | None = None,
) -> dict[str, Any]:
    """Return a router's NeighborTable + RouteTable rows with names resolved.

    Shape::

        {
            "reporter_eui64": str,
            "reporter_name":  str | None,
            "neighbor_count": int,
            "route_count":    int,
            "neighbors": [ {neighbor_eui64, name, lqi_in, rssi_avg,
                            rx_on_when_idle, full_thread_device, is_child,
                            age_seconds, frame_error_rate, message_error_rate,
                            link_frame_counter, mle_frame_counter}, ... ],
            "routes":    [ {neighbor_eui64, name, router_id, next_hop_router_id,
                            next_hop_eui64, next_hop_name, path_cost,
                            lqi_in, lqi_out, link_established, allocated,
                            age_seconds}, ... ],
        }
    """
    s = store or get_store()
    nodes = s.list_nodes()
    name_by_eui = {n["eui64"]: (n.get("friendly_name") or get_node_display_name(n))
                   for n in nodes if n.get("eui64")}
    reporter_node = next((n for n in nodes if n.get("eui64") == reporter_eui), None)
    partition_id = reporter_node.get("partition_id") if reporter_node else None
    router_by_id = _router_id_index(nodes, partition_id)

    with s._lock:  # noqa: SLF001
        rows = s._conn.execute(  # noqa: SLF001
            "SELECT * FROM links WHERE reporter_eui64 = ? ORDER BY source, neighbor_eui64",
            (reporter_eui,),
        ).fetchall()

    neighbors: list[dict[str, Any]] = []
    routes: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        nei = d.get("neighbor_eui64")
        name = name_by_eui.get(nei) if nei else None
        if d.get("source") == "neighbor_table":
            neighbors.append({
                "neighbor_eui64": nei,
                "name": name,
                "rssi_avg": d.get("rssi_avg"),
                "rssi_last": d.get("rssi_last"),
                "lqi_in": d.get("lqi_in"),
                "is_child": d.get("is_child"),
                "rx_on_when_idle": d.get("rx_on_when_idle"),
                "full_thread_device": d.get("full_thread_device"),
                "full_network_data": d.get("full_network_data"),
                "age_seconds": d.get("age_seconds"),
                "frame_error_rate": d.get("frame_error_rate"),
                "message_error_rate": d.get("message_error_rate"),
                "link_frame_counter": d.get("link_frame_counter"),
                "mle_frame_counter": d.get("mle_frame_counter"),
            })
        else:  # route_table
            nh_rid = d.get("next_hop_router_id")
            nh_eui: str | None = None
            if nh_rid is not None and nh_rid != NO_ROUTE_RID:
                resolved = router_by_id.get(int(nh_rid))
                nh_eui = resolved.get("eui64") if resolved else None
            # Direct-neighbor short-circuit (same rule as walk_route_to_otbr):
            # PathCost=1 + LinkEstablished=1 means the row's destination is
            # the actual forwarding next hop; NextHopRouterId is just the
            # route-advertisement relay. Surface the derived field so UI
            # and AI consumers don't have to re-implement the rule. Raw
            # next_hop_* fields are preserved for diagnostics.
            path_cost_val = d.get("path_cost")
            link_est_val = d.get("link_established")
            is_direct = (
                path_cost_val is not None
                and int(path_cost_val) == 1
                and link_est_val == 1
            )
            effective_nh_eui = nei if is_direct else nh_eui
            routes.append({
                "neighbor_eui64": nei,
                "name": name,
                "router_id": None,  # not separately stored on link; nei is dest
                "next_hop_router_id": nh_rid,
                "next_hop_eui64": nh_eui,
                "next_hop_name": name_by_eui.get(nh_eui) if nh_eui else None,
                "effective_next_hop_eui64": effective_nh_eui,
                "effective_next_hop_name": (
                    name if is_direct else name_by_eui.get(nh_eui) if nh_eui else None
                ),
                "is_direct_link": is_direct,
                "path_cost": d.get("path_cost"),
                "lqi_in": d.get("lqi_in"),
                "lqi_out": d.get("lqi_out"),
                "link_established": d.get("link_established"),
                "allocated": d.get("allocated"),
                "age_seconds": d.get("age_seconds"),
            })

    return {
        "reporter_eui64": reporter_eui,
        "reporter_name": name_by_eui.get(reporter_eui),
        "partition_id": partition_id,
        "neighbor_count": len(neighbors),
        "route_count": len(routes),
        "neighbors": neighbors,
        "routes": routes,
    }


# Thread spec: a router can host at most 16 children; the standard practical
# cap most implementations advertise is 10. Used only as a "headroom" hint —
# the actual limit varies by stack.
_THREAD_CHILD_CAP_HINT = 10


def list_children_enriched(
    parent_eui: str,
    *,
    store: SQLiteStore | None = None,
) -> dict[str, Any]:
    """Return the child-attachment roster as seen from a parent router.

    Sleepy / minimal end devices only show up in their *parent's*
    NeighborTable with ``IsChild=1`` — they don't broadcast their own
    diagnostics, so this is the only place their link quality is visible.

    Shape::

        {
            "parent_eui64":     str,
            "parent_name":      str | None,
            "partition_id":     int | None,
            "child_count":      int,
            "capacity_hint":    int,   # spec-practical cap; informational
            "is_at_capacity":   bool,  # child_count >= capacity_hint
            "children": [
                {
                    "eui64": str,
                    "name":  str | None,
                    "registered": bool,     # True if neighbor_known=1
                    "rssi_avg": int|None,
                    "rssi_last": int|None,
                    "lqi_in": int|None,
                    "rx_on_when_idle": int|None,  # 0 = sleepy
                    "full_thread_device": int|None,
                    "age_seconds": int|None,
                    "frame_error_rate": int|None,
                    "message_error_rate": int|None,
                    "link_frame_counter": int|None,
                    "mle_frame_counter": int|None,
                },
                ...
            ],
        }

    The rows come from ``links`` where ``reporter_eui64=parent_eui``,
    ``source='neighbor_table'``, ``is_child=1``. Name resolution mirrors
    :func:`list_neighbors_enriched`.
    """
    s = store or get_store()
    nodes = s.list_nodes()
    name_by_eui = {n["eui64"]: (n.get("friendly_name") or get_node_display_name(n))
                   for n in nodes if n.get("eui64")}
    parent_node = next((n for n in nodes if n.get("eui64") == parent_eui), None)
    partition_id = parent_node.get("partition_id") if parent_node else None

    with s._lock:  # noqa: SLF001
        rows = s._conn.execute(  # noqa: SLF001
            "SELECT * FROM links WHERE reporter_eui64 = ? AND source = 'neighbor_table'"
            " AND is_child = 1 ORDER BY neighbor_eui64",
            (parent_eui,),
        ).fetchall()

    children: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        nei = d.get("neighbor_eui64")
        children.append({
            "eui64": nei,
            "name": name_by_eui.get(nei) if nei else None,
            "registered": bool(d.get("neighbor_known", 1)),
            "rssi_avg": d.get("rssi_avg"),
            "rssi_last": d.get("rssi_last"),
            "lqi_in": d.get("lqi_in"),
            "rx_on_when_idle": d.get("rx_on_when_idle"),
            "full_thread_device": d.get("full_thread_device"),
            "age_seconds": d.get("age_seconds"),
            "frame_error_rate": d.get("frame_error_rate"),
            "message_error_rate": d.get("message_error_rate"),
            "link_frame_counter": d.get("link_frame_counter"),
            "mle_frame_counter": d.get("mle_frame_counter"),
        })

    return {
        "parent_eui64": parent_eui,
        "parent_name": name_by_eui.get(parent_eui),
        "partition_id": partition_id,
        "child_count": len(children),
        "capacity_hint": _THREAD_CHILD_CAP_HINT,
        "is_at_capacity": len(children) >= _THREAD_CHILD_CAP_HINT,
        "children": children,
    }
