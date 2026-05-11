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
    """Return latest RSSI and LQI for a node from the event log."""
    s = store or get_store()
    events = s.query_events(eui64=node_eui64, limit=100)
    rssi_samples = [e.get("rssi") for e in events if e.get("rssi") is not None]
    lqi_samples = [e.get("lqi") for e in events if e.get("lqi") is not None]
    return {
        "rssi": rssi_samples[0] if rssi_samples else None,
        "lqi": lqi_samples[0] if lqi_samples else None,
        "rssi_avg": sum(rssi_samples) // len(rssi_samples) if rssi_samples else None,
        "lqi_avg": sum(lqi_samples) // len(lqi_samples) if lqi_samples else None,
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
        "area": node.get("area"),
        "device_id": node.get("device_id"),
        "first_seen": node.get("first_seen"),
        "last_seen": node.get("last_seen"),
        "status": infer_node_status(node),
    }

    if include_signal_strength:
        summary["signal_strength"] = get_latest_signal_strength(eui64, store=s)

    return summary


def list_nodes_enriched(
    store: SQLiteStore | None = None,
    include_signal_strength: bool = False,
) -> list[dict[str, Any]]:
    """Return all nodes with enrichment (status, display_name)."""
    s = store or get_store()
    nodes = s.list_nodes()
    out: list[dict[str, Any]] = []
    for node in nodes:
        eui = node.get("eui64")
        summary: dict[str, Any] = {
            "eui64": eui,
            "friendly_name": node.get("friendly_name"),
            "display_name": get_node_display_name(node),
            "role": node.get("role"),
            "area": node.get("area"),
            "device_id": node.get("device_id"),
            "first_seen": node.get("first_seen"),
            "last_seen": node.get("last_seen"),
            "status": infer_node_status(node),
        }
        if include_signal_strength and eui:
            summary["signal_strength"] = get_latest_signal_strength(eui, store=s)
        out.append(summary)
    return out
