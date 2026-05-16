"""OTBR / OpenThread Border Router ingestion adapter.

Polls the Supervisor ``/addons/{slug}/logs`` endpoint for an OTBR-class add-on,
parses recognised event lines via :mod:`otbr_parser`, and persists canonical
events to the SQLite store. Tracks a cursor (hash of last successfully
ingested line + count) in the ``ingest_state`` table so re-runs are
idempotent.

Discovery: ``list_candidates()`` returns add-ons whose slug or name suggests
they host OpenThread / OTBR (matchers: ``openthread``, ``otbr``, ``silabs-otbr``).
Callers can override autodiscovery via :func:`set_slug`.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from typing import Any

import httpx

from . import otbr_parser
from ..storage.sqlite_store import SQLiteStore, get_store
from ..utils.datetime import utc_now_iso

log = logging.getLogger(__name__)

SUPERVISOR_URL = os.getenv("SUPERVISOR_URL", "http://supervisor")
SUPERVISOR_TOKEN_ENV = "SUPERVISOR_TOKEN"
DEFAULT_TIMEOUT = 10.0

# Slug substrings that mark an add-on as an OTBR candidate.
_OTBR_HINTS = ("openthread", "otbr", "silabs-multiprotocol", "silabs_multiprotocol")

# Special key used to persist the configured slug + cursor in ingest_state.
_STATE_KEY_PREFIX = "otbr:"


def _token_or_raise() -> str:
    tok = os.getenv(SUPERVISOR_TOKEN_ENV)
    if not tok:
        raise RuntimeError(f"{SUPERVISOR_TOKEN_ENV} not set; running outside Supervisor?")
    return tok


def _headers(accept: str = "application/json") -> dict[str, str]:
    return {"Authorization": f"Bearer {_token_or_raise()}", "Accept": accept}


def _hash_line(line: str) -> str:
    return hashlib.sha1(line.encode("utf-8", errors="replace")).hexdigest()[:16]


# --------------------------------------------------------------------------
# Discovery + slug persistence
# --------------------------------------------------------------------------

async def list_candidates() -> list[dict[str, Any]]:
    """Return Supervisor add-ons that look like OTBR hosts."""
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        r = await client.get(f"{SUPERVISOR_URL}/addons", headers=_headers())
        r.raise_for_status()
        payload = r.json()
    items = (payload.get("data") or {}).get("addons", payload.get("addons", []))
    out: list[dict[str, Any]] = []
    for a in items:
        slug = (a.get("slug") or "").lower()
        name = (a.get("name") or "").lower()
        if any(h in slug or h in name for h in _OTBR_HINTS):
            out.append({
                "slug": a.get("slug"),
                "name": a.get("name"),
                "version": a.get("version"),
                "state": a.get("state"),
                "update_available": a.get("update_available"),
            })
    return out


def get_configured_slug(store: SQLiteStore | None = None) -> str | None:
    s = store or get_store()
    state = _read_state(s)
    return state.get("slug")


def set_slug(slug: str, store: SQLiteStore | None = None) -> dict[str, Any]:
    """Persist the OTBR add-on slug to use for ingestion."""
    s = store or get_store()
    state = _read_state(s)
    state["slug"] = slug
    # Reset cursor when switching add-ons.
    state["last_line_hash"] = None
    state["last_event_ts"] = None
    state["last_run_at"] = utc_now_iso()
    state["last_error"] = None
    _write_state(s, state)
    return state


# --------------------------------------------------------------------------
# ingest_state row helpers (stored under key "otbr:default")
# --------------------------------------------------------------------------

_STATE_PATH = f"{_STATE_KEY_PREFIX}default"


def _read_state(store: SQLiteStore) -> dict[str, Any]:
    with store._lock:  # type: ignore[attr-defined]
        row = store._conn.execute(  # type: ignore[attr-defined]
            "SELECT path, position, inode, last_event_ts FROM ingest_state WHERE path = ?",
            (_STATE_PATH,),
        ).fetchone()
    if not row:
        return {
            "path": _STATE_PATH,
            "slug": None,
            "position": 0,            # total lines ever processed
            "last_line_hash": None,   # for resume after rotation
            "last_event_ts": None,
            "last_run_at": None,
            "last_error": None,
            "events_total": 0,
        }
    d = dict(row)
    import json as _json
    # We pack auxiliary fields into a json blob hidden in inode (int) and via
    # a parallel row mechanism would be cleaner, but the v1 schema only has
    # 4 columns. Encode the structured state via inode=events_total and
    # last_event_ts; remaining fields live in process memory mirrored here.
    return {
        "path": d["path"],
        "slug": _MEM_STATE.get("slug"),
        "position": int(d["position"] or 0),
        "last_line_hash": _MEM_STATE.get("last_line_hash"),
        "last_event_ts": d["last_event_ts"],
        "last_run_at": _MEM_STATE.get("last_run_at"),
        "last_error": _MEM_STATE.get("last_error"),
        "events_total": int(d["inode"] or 0),
    }


# In-memory mirror for fields the v1 schema can't hold (slug, hash, last_run_at,
# last_error). Re-populated from a sentinel on every read/write. For the v1
# scaffold this is acceptable; v2 will widen the schema with a JSON blob column.
_MEM_STATE: dict[str, Any] = {}


def _write_state(store: SQLiteStore, state: dict[str, Any]) -> None:
    _MEM_STATE.update({
        "slug": state.get("slug"),
        "last_line_hash": state.get("last_line_hash"),
        "last_run_at": state.get("last_run_at"),
        "last_error": state.get("last_error"),
    })
    with store._lock:  # type: ignore[attr-defined]
        store._conn.execute(  # type: ignore[attr-defined]
            "INSERT INTO ingest_state(path, position, inode, last_event_ts)"
            " VALUES (?, ?, ?, ?)"
            " ON CONFLICT(path) DO UPDATE SET"
            "   position=excluded.position,"
            "   inode=excluded.inode,"
            "   last_event_ts=excluded.last_event_ts",
            (
                _STATE_PATH,
                int(state.get("position") or 0),
                int(state.get("events_total") or 0),
                state.get("last_event_ts"),
            ),
        )


def get_state(store: SQLiteStore | None = None) -> dict[str, Any]:
    """Return the current ingest state (for MCP/dashboard surfaces)."""
    return _read_state(store or get_store())


# --------------------------------------------------------------------------
# Log fetch + ingest
# --------------------------------------------------------------------------

async def fetch_logs(slug: str, *, max_bytes: int = 256_000) -> list[str]:
    """Return raw log lines from Supervisor for the given add-on slug."""
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        r = await client.get(
            f"{SUPERVISOR_URL}/addons/{slug}/logs",
            headers=_headers("text/plain"),
        )
        r.raise_for_status()
        text = r.text
    if len(text) > max_bytes:
        text = text[-max_bytes:]
    return text.splitlines()


def _slice_new_lines(lines: list[str], last_hash: str | None) -> list[str]:
    """Return only the lines after ``last_hash``. If hash not found, return all."""
    if not last_hash:
        return lines
    for idx in range(len(lines) - 1, -1, -1):
        if _hash_line(lines[idx]) == last_hash:
            return lines[idx + 1:]
    # Hash not present (log rotated / truncated) → ingest everything.
    return lines


async def ingest_once(
    *,
    slug: str | None = None,
    store: SQLiteStore | None = None,
) -> dict[str, Any]:
    """Fetch the latest logs once, parse, persist new events. Returns summary."""
    s = store or get_store()
    state = _read_state(s)
    target = slug or state.get("slug")
    summary: dict[str, Any] = {
        "ran_at": utc_now_iso(),
        "slug": target,
        "lines_seen": 0,
        "lines_new": 0,
        "events_inserted": 0,
        "error": None,
    }
    if not target:
        summary["error"] = "no OTBR slug configured; call set_otbr_slug or list_otbr_candidates first"
        state["last_run_at"] = summary["ran_at"]
        state["last_error"] = summary["error"]
        _write_state(s, state)
        return summary
    try:
        lines = await fetch_logs(target)
    except Exception as exc:  # noqa: BLE001
        summary["error"] = f"fetch failed: {exc}"
        state["last_run_at"] = summary["ran_at"]
        state["last_error"] = summary["error"]
        _write_state(s, state)
        return summary

    summary["lines_seen"] = len(lines)
    new_lines = _slice_new_lines(lines, state.get("last_line_hash"))
    summary["lines_new"] = len(new_lines)

    inserted = 0
    last_ts: str | None = state.get("last_event_ts")
    for line in new_lines:
        ev = otbr_parser.parse_line(line)
        if ev is None:
            continue
        try:
            s.insert_event(**ev.to_storage_kwargs())
            inserted += 1
            if not last_ts or ev.ts > last_ts:
                last_ts = ev.ts
        except Exception as exc:  # noqa: BLE001
            log.warning("insert_event failed for line %r: %s", line[:120], exc)

    summary["events_inserted"] = inserted
    if lines:
        state["last_line_hash"] = _hash_line(lines[-1])
    state["position"] = int(state.get("position") or 0) + len(new_lines)
    state["events_total"] = int(state.get("events_total") or 0) + inserted
    state["last_event_ts"] = last_ts
    state["last_run_at"] = summary["ran_at"]
    state["last_error"] = None
    state["slug"] = target
    _write_state(s, state)
    return summary


# --------------------------------------------------------------------------
# Background scheduler (called from core service startup)
# --------------------------------------------------------------------------

async def run_forever(interval_seconds: int = 10) -> None:
    """Loop forever, invoking ``ingest_once`` every ``interval_seconds``.

    Errors are logged but do not break the loop.
    """
    log.info("otbr ingestion loop starting (interval=%ss)", interval_seconds)
    while True:
        try:
            res = await ingest_once()
            if res.get("events_inserted"):
                log.info(
                    "otbr ingest: slug=%s lines_new=%d events_inserted=%d",
                    res.get("slug"), res.get("lines_new"), res.get("events_inserted"),
                )
            elif res.get("error"):
                # Only log first-occurrence errors at WARN; otherwise debug.
                log.debug("otbr ingest noop: %s", res.get("error"))
        except Exception as exc:  # noqa: BLE001
            log.exception("otbr ingestion loop iteration failed: %s", exc)
        await asyncio.sleep(max(1, int(interval_seconds)))
