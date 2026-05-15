"""Historical link-signal queries derived from retained per-link samples."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from ..storage.sqlite_store import get_store
from ..utils.datetime import parse_iso_datetime

DEFAULT_LOOKBACK_HOURS = 24


def _resolve_window(since: str | None, until: str | None) -> tuple[str, str]:
    now = datetime.now(tz=UTC)
    until_dt = parse_iso_datetime(until) or now
    if since:
        since_dt = parse_iso_datetime(since) or (until_dt - timedelta(hours=DEFAULT_LOOKBACK_HOURS))
    else:
        since_dt = until_dt - timedelta(hours=DEFAULT_LOOKBACK_HOURS)
    return since_dt.isoformat(), until_dt.isoformat()


def _metric_summary(series: list[dict[str, Any]], key: str) -> dict[str, Any]:
    values = [float(row[key]) for row in series if isinstance(row.get(key), (int, float))]
    if not values:
        return {}
    first = values[0]
    last = values[-1]
    return {
        "first": first,
        "last": last,
        "delta": round(last - first, 3),
        "min": min(values),
        "max": max(values),
        "avg": round(sum(values) / len(values), 3),
    }


def get_node_link_signal_history(
    *,
    eui64: str,
    since: str | None = None,
    until: str | None = None,
    peer_eui64: str | None = None,
    source: str | None = None,
    limit: int = 5000,
) -> dict[str, Any]:
    """Return retained historical link-signal samples for one node."""
    if not eui64:
        return {"error": "eui64 is required", "links": []}
    since_iso, until_iso = _resolve_window(since, until)
    rows = get_store().list_link_signal_samples(
        eui64=eui64,
        source=source,
        since=since_iso,
        until=until_iso,
        limit=limit,
    )
    if peer_eui64:
        rows = [
            row for row in rows
            if row.get("reporter_eui64") == peer_eui64 or row.get("neighbor_eui64") == peer_eui64
        ]

    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            str(row.get("reporter_eui64") or ""),
            str(row.get("neighbor_eui64") or ""),
            str(row.get("source") or ""),
        )
        grouped.setdefault(key, []).append(
            {
                "observed_at": row.get("observed_at"),
                "present": bool(row.get("present")),
                "change_reason": row.get("change_reason"),
                "partition_id": row.get("partition_id"),
                "rssi_avg": row.get("rssi_avg"),
                "rssi_last": row.get("rssi_last"),
                "lqi_in": row.get("lqi_in"),
                "lqi_out": row.get("lqi_out"),
                "is_child": row.get("is_child"),
                "path_cost": row.get("path_cost"),
                "frame_error_rate": row.get("frame_error_rate"),
                "message_error_rate": row.get("message_error_rate"),
            }
        )

    links: list[dict[str, Any]] = []
    for (reporter, neighbor, link_source), series in grouped.items():
        peer = neighbor if reporter == eui64 else reporter
        links.append(
            {
                "reporter_eui64": reporter,
                "neighbor_eui64": neighbor,
                "peer_eui64": peer,
                "source": link_source,
                "series": series,
                "metrics": {
                    "rssi_avg": _metric_summary(series, "rssi_avg"),
                    "rssi_last": _metric_summary(series, "rssi_last"),
                    "lqi_in": _metric_summary(series, "lqi_in"),
                    "lqi_out": _metric_summary(series, "lqi_out"),
                },
            }
        )
    links.sort(key=lambda row: (row["peer_eui64"], row["source"]))
    return {
        "eui64": eui64,
        "since": since_iso,
        "until": until_iso,
        "peer_eui64": peer_eui64,
        "source": source,
        "links": links,
        "link_count": len(links),
        "sample_count": sum(len(row["series"]) for row in links),
    }