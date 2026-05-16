"""Tier 4: unified chronological timeline.

Synthesizes a single newest-first stream from three existing sources so
an AI consultant can correlate Thread / Matter / observer-side activity
without paying for multiple round-trips:

* canonical events from the ``events`` table (attach, parent_change,
  status_change, link_acquired, link_lost, rloc16_change, …)
* issue lifecycle synthesized from the ``issues`` table (one
  ``issue.opened`` row at ``opened_at`` and, if applicable, one
  ``issue.closed`` row at ``closed_at``)
* outage/start windows from the ``observer_events`` table

This is read-only — no new migration. Each timeline row is normalized
to ``{ts, source, kind, eui64, severity?, details, ref_id}`` so a model
can reason over them uniformly.
"""

from __future__ import annotations

from typing import Any, Iterable

from ..storage.sqlite_store import SQLiteStore
from ..utils.datetime import utc_now_iso


# Canonical sources callers can ask for; "all" means union of everything.
SOURCES = ("events", "issues", "observer_events")


def _matches_kind(kind: str, allowed: Iterable[str] | None) -> bool:
    if not allowed:
        return True
    return kind in allowed


def _matches_source(source: str, allowed: Iterable[str] | None) -> bool:
    if not allowed:
        return True
    return source in allowed


def query_timeline(
    store: SQLiteStore,
    *,
    since: str,
    until: str | None = None,
    eui64: str | None = None,
    kinds: Iterable[str] | None = None,
    sources: Iterable[str] | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    """Return a unified newest-first timeline across the three sources.

    ``kinds`` filters by the row's normalized ``kind`` (e.g. ``attach``,
    ``issue.opened``, ``outage``). ``sources`` restricts which source
    tables are read at all. ``limit`` caps the final merged list.
    """
    limit = max(1, min(int(limit), 5000))
    upper = until or utc_now_iso()
    kind_set = set(kinds) if kinds else None
    source_set = set(sources) if sources else None

    rows: list[dict[str, Any]] = []

    # --- events --------------------------------------------------------
    if _matches_source("events", source_set):
        evs = store.query_events(
            eui64=eui64,
            since=since,
            limit=limit,
        )
        for e in evs:
            ts = e.get("ts")
            if not ts or ts > upper:
                continue
            kind = str(e.get("type") or "")
            if not _matches_kind(kind, kind_set):
                continue
            rows.append(
                {
                    "ts": ts,
                    "source": "events",
                    "kind": kind,
                    "eui64": e.get("eui64"),
                    "severity": None,
                    "details": {
                        k: v
                        for k, v in e.items()
                        if k not in ("ts", "type", "eui64", "id")
                    },
                    "ref_id": e.get("id"),
                }
            )

    # --- issues lifecycle ---------------------------------------------
    if _matches_source("issues", source_set):
        issues = store.list_issues_in_window(
            since=since, until=upper, eui64=eui64
        )
        for iss in issues:
            opened_at = iss.get("opened_at")
            closed_at = iss.get("closed_at")
            iid = iss.get("id")
            base = {
                "eui64": iss.get("eui64"),
                "severity": iss.get("severity"),
                "details": {
                    "issue_kind": iss.get("kind"),
                    "evidence": iss.get("evidence"),
                },
                "ref_id": iid,
            }
            if opened_at and since <= opened_at <= upper:
                kind = "issue.opened"
                if _matches_kind(kind, kind_set):
                    rows.append(
                        {"ts": opened_at, "source": "issues", "kind": kind, **base}
                    )
            if closed_at and since <= closed_at <= upper:
                kind = "issue.closed"
                if _matches_kind(kind, kind_set):
                    rows.append(
                        {"ts": closed_at, "source": "issues", "kind": kind, **base}
                    )

    # --- observer events ----------------------------------------------
    if _matches_source("observer_events", source_set):
        obs = store.list_observer_events_in_window(since=since, until=upper)
        for ev in obs:
            started_at = ev.get("started_at")
            ended_at = ev.get("ended_at")
            kind = str(ev.get("kind") or "")
            base = {
                "eui64": None,
                "severity": None,
                "details": {
                    "observer_source": ev.get("source"),
                    "observer_kind": kind,
                    "ended_at": ended_at,
                    **(ev.get("details") or {}),
                },
                "ref_id": ev.get("id"),
            }
            if started_at and since <= started_at <= upper:
                emit_kind = f"observer.{kind}"
                if _matches_kind(emit_kind, kind_set):
                    rows.append(
                        {
                            "ts": started_at,
                            "source": "observer_events",
                            "kind": emit_kind,
                            **base,
                        }
                    )
            if ended_at and ended_at != started_at and since <= ended_at <= upper:
                emit_kind = f"observer.{kind}.ended"
                if _matches_kind(emit_kind, kind_set):
                    rows.append(
                        {
                            "ts": ended_at,
                            "source": "observer_events",
                            "kind": emit_kind,
                            **base,
                        }
                    )

    # Newest-first merge then cap.
    rows.sort(key=lambda r: (r["ts"], r.get("ref_id") or 0), reverse=True)
    if len(rows) > limit:
        rows = rows[:limit]

    return {
        "since": since,
        "until": upper,
        "count": len(rows),
        "rows": rows,
    }
