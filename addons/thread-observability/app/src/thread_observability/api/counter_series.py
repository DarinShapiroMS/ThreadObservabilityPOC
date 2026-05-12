"""Phase 4 counter time-series tools.

Reads ``node_counter_samples`` rows produced by the pipeline and exposes them
as a per-node series with computed deltas. Two read tools:

* :func:`get_counter_series` — one node, one or many counter names, [since, until].
* :func:`compare_node_counters` — two nodes side-by-side over the same window.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from ..storage.sqlite_store import get_store

DEFAULT_LOOKBACK_HOURS = 6
MAX_LOOKBACK_HOURS = 24 * 14  # capped by sampled_archive_days default of 14


def _resolve_window(since: str | None, until: str | None) -> tuple[str, str]:
    """Return an ISO window, defaulting to the last DEFAULT_LOOKBACK_HOURS."""
    now = datetime.now(tz=UTC)
    until_dt = _parse_iso(until) or now
    if since:
        since_dt = _parse_iso(since) or (until_dt - timedelta(hours=DEFAULT_LOOKBACK_HOURS))
    else:
        since_dt = until_dt - timedelta(hours=DEFAULT_LOOKBACK_HOURS)
    return since_dt.isoformat(), until_dt.isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _filter_counters(sample: dict[str, Any], counter_names: list[str] | None) -> dict[str, Any]:
    if not counter_names:
        return dict(sample)
    return {k: sample[k] for k in counter_names if k in sample}


def _bucket_5min(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Downsample to 5-minute buckets by averaging each numeric counter.

    Input rows are oldest-first with `observed_at` ISO strings and `counters`
    dicts. Output preserves that shape, keyed by the bucket start.
    """
    buckets: dict[str, list[dict[str, Any]]] = {}
    bucket_order: list[str] = []
    for row in samples:
        ts = row.get("observed_at")
        try:
            dt = datetime.fromisoformat(ts)
        except (TypeError, ValueError):
            continue
        bm = (dt.minute // 5) * 5
        b_dt = dt.replace(minute=bm, second=0, microsecond=0)
        key = b_dt.isoformat()
        if key not in buckets:
            buckets[key] = []
            bucket_order.append(key)
        buckets[key].append(row.get("counters") or {})

    out: list[dict[str, Any]] = []
    for key in bucket_order:
        members = buckets[key]
        sums: dict[str, float] = {}
        counts: dict[str, int] = {}
        for m in members:
            for k, v in m.items():
                if isinstance(v, (int, float)):
                    sums[k] = sums.get(k, 0.0) + float(v)
                    counts[k] = counts.get(k, 0) + 1
        averaged = {k: round(sums[k] / counts[k], 3) for k in sums}
        out.append({"observed_at": key, "counters": averaged, "sample_count": len(members)})
    return out


def _compute_deltas(series: list[dict[str, Any]]) -> dict[str, Any]:
    """Return per-counter (last - first) deltas, ignoring resets."""
    if len(series) < 2:
        return {}
    first = series[0].get("counters") or {}
    last = series[-1].get("counters") or {}
    deltas: dict[str, Any] = {}
    for key in set(first) | set(last):
        a = first.get(key)
        b = last.get(key)
        if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
            continue
        diff = b - a
        if diff < 0:
            # Counter reset (re-attach, OTBR restart). Report explicitly.
            deltas[key] = {"delta": None, "reset_detected": True, "first": a, "last": b}
        else:
            deltas[key] = {"delta": diff, "reset_detected": False, "first": a, "last": b}
    return deltas


def get_counter_series(
    *,
    eui64: str,
    counter_names: list[str] | None = None,
    since: str | None = None,
    until: str | None = None,
    resolution: str = "raw",
) -> dict[str, Any]:
    """Return per-node counter time-series.

    Returns:
      {eui64, since, until, resolution, series: [{observed_at, counters}, ...],
       deltas: {<counter>: {delta, reset_detected, first, last}, ...}}
    """
    if not eui64:
        return {"error": "eui64 is required", "series": [], "deltas": {}}
    res = resolution if resolution in ("raw", "5min") else "raw"
    since_iso, until_iso = _resolve_window(since, until)
    store = get_store()
    rows = store.get_counter_samples(eui64=eui64, since=since_iso, until=until_iso)
    if counter_names:
        rows = [
            {**r, "counters": _filter_counters(r.get("counters") or {}, counter_names)}
            for r in rows
        ]
    series: list[dict[str, Any]]
    if res == "5min":
        series = _bucket_5min(rows)
    else:
        series = [
            {"observed_at": r["observed_at"], "counters": r.get("counters") or {}}
            for r in rows
        ]
    deltas = _compute_deltas(series)
    return {
        "eui64": eui64,
        "since": since_iso,
        "until": until_iso,
        "resolution": res,
        "counter_names": counter_names or [],
        "series": series,
        "deltas": deltas,
        "sample_count": len(series),
    }


def compare_node_counters(
    *,
    eui64_a: str,
    eui64_b: str,
    counter_names: list[str] | None = None,
    since: str | None = None,
    until: str | None = None,
    resolution: str = "raw",
) -> dict[str, Any]:
    """Side-by-side comparison of two nodes' counter series.

    Returns ``{a, b, peer_summary}`` where each side has the same structure
    as :func:`get_counter_series`, and ``peer_summary`` highlights any counter
    whose delta on A is >=2x the delta on B (or vice versa).
    """
    if not eui64_a or not eui64_b:
        return {"error": "eui64_a and eui64_b are required"}
    a = get_counter_series(
        eui64=eui64_a, counter_names=counter_names,
        since=since, until=until, resolution=resolution,
    )
    b = get_counter_series(
        eui64=eui64_b, counter_names=counter_names,
        since=since, until=until, resolution=resolution,
    )
    peer = _peer_summary(a.get("deltas") or {}, b.get("deltas") or {})
    return {"a": a, "b": b, "peer_summary": peer}


def _peer_summary(da: dict[str, Any], db: dict[str, Any]) -> dict[str, Any]:
    """Flag counters where one side's delta is at least 2x the other."""
    flagged: list[dict[str, Any]] = []
    for key in sorted(set(da) | set(db)):
        ea = da.get(key) or {}
        eb = db.get(key) or {}
        a_delta = ea.get("delta") if isinstance(ea.get("delta"), (int, float)) else None
        b_delta = eb.get("delta") if isinstance(eb.get("delta"), (int, float)) else None
        if a_delta is None or b_delta is None:
            continue
        if a_delta == 0 and b_delta == 0:
            continue
        # Avoid divide-by-zero. Use the larger side's ratio against max(other,1).
        a_ratio = a_delta / max(b_delta, 1)
        b_ratio = b_delta / max(a_delta, 1)
        if a_ratio >= 2 or b_ratio >= 2:
            flagged.append({
                "counter": key,
                "a_delta": a_delta,
                "b_delta": b_delta,
                "ratio": round(max(a_ratio, b_ratio), 2),
            })
    return {"flagged": flagged, "flagged_count": len(flagged)}
