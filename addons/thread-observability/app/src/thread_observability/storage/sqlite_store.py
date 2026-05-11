"""SQLite identity / event / issue store for Thread Observability.

Schema is migration-versioned via a single ``schema_version`` table; each
migration is idempotent and applied in order. The store is intentionally
synchronous (sqlite3 is fast enough at our scale) and serialised through
a module-level lock so it can be safely called from FastAPI async handlers
via ``run_in_executor`` or directly from short request paths.

Tables (v1):

* ``nodes`` (eui64 PK, friendly_name, area, device_id, role, first_seen, last_seen)
* ``events`` (id PK, ts, eui64, type, parent_eui64, rssi, lqi, payload_json)
* ``issues`` (id PK, opened_at, closed_at, severity, kind, eui64, evidence_json)
* ``metadata_cache`` (key PK, value_json, fetched_at, ttl_seconds)
* ``ingest_state`` (path PK, position, inode, last_event_ts)
* ``schema_version`` (version PK, applied_at)
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

DEFAULT_DB_PATH = Path(
    os.getenv("THREAD_OBS_DB_PATH", "/data/thread-observability/state.db")
)

# Each entry is applied in order; ``version`` matches its index (1-based).
_MIGRATIONS: list[str] = [
    # v1: initial schema
    """
    CREATE TABLE IF NOT EXISTS schema_version (
        version    INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS nodes (
        eui64         TEXT PRIMARY KEY,
        friendly_name TEXT,
        area          TEXT,
        device_id     TEXT,
        role          TEXT,
        first_seen    TEXT NOT NULL,
        last_seen     TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS events (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        ts            TEXT NOT NULL,
        eui64         TEXT NOT NULL,
        type          TEXT NOT NULL,
        parent_eui64  TEXT,
        rssi          INTEGER,
        lqi           INTEGER,
        payload_json  TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_events_eui64_ts ON events(eui64, ts DESC);
    CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts DESC);
    CREATE INDEX IF NOT EXISTS idx_events_type_ts ON events(type, ts DESC);

    CREATE TABLE IF NOT EXISTS issues (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        opened_at     TEXT NOT NULL,
        closed_at     TEXT,
        severity      TEXT NOT NULL,
        kind          TEXT NOT NULL,
        eui64         TEXT,
        evidence_json TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_issues_open ON issues(closed_at, severity);
    CREATE INDEX IF NOT EXISTS idx_issues_eui64 ON issues(eui64);

    CREATE TABLE IF NOT EXISTS metadata_cache (
        key          TEXT PRIMARY KEY,
        value_json   TEXT NOT NULL,
        fetched_at   TEXT NOT NULL,
        ttl_seconds  INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS ingest_state (
        path           TEXT PRIMARY KEY,
        position       INTEGER NOT NULL,
        inode          INTEGER,
        last_event_ts  TEXT
    );
    """,
]


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


class SQLiteStore:
    """Thin wrapper around the on-disk SQLite database."""

    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self._lock = threading.RLock()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            isolation_level=None,  # autocommit; we manage transactions explicitly
            timeout=10.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(
            "PRAGMA journal_mode=WAL;"
            "PRAGMA synchronous=NORMAL;"
            "PRAGMA foreign_keys=ON;"
            "PRAGMA temp_store=MEMORY;"
        )
        self._migrate()

    def _migrate(self) -> None:
        with self._lock:
            self._conn.executescript(_MIGRATIONS[0])
            cur = self._conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version")
            current = int(cur.fetchone()[0])
            for idx, sql in enumerate(_MIGRATIONS, start=1):
                if idx <= current:
                    continue
                self._conn.executescript(sql)
                self._conn.execute(
                    "INSERT OR REPLACE INTO schema_version(version, applied_at) VALUES (?, ?)",
                    (idx, _utc_now()),
                )

    @property
    def schema_version(self) -> int:
        with self._lock:
            cur = self._conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version")
            return int(cur.fetchone()[0])

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # -- events --------------------------------------------------------

    def insert_event(
        self,
        *,
        eui64: str,
        type: str,
        ts: str | None = None,
        parent_eui64: str | None = None,
        rssi: int | None = None,
        lqi: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> int:
        ts = ts or _utc_now()
        payload_json = json.dumps(payload) if payload is not None else None
        with self._tx() as conn:
            cur = conn.execute(
                "INSERT INTO events(ts, eui64, type, parent_eui64, rssi, lqi, payload_json)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ts, eui64, type, parent_eui64, rssi, lqi, payload_json),
            )
            conn.execute(
                "INSERT INTO nodes(eui64, first_seen, last_seen) VALUES (?, ?, ?)"
                " ON CONFLICT(eui64) DO UPDATE SET last_seen=excluded.last_seen",
                (eui64, ts, ts),
            )
            return int(cur.lastrowid or 0)

    def insert_events(self, batch: Iterable[dict[str, Any]]) -> int:
        count = 0
        for ev in batch:
            self.insert_event(**ev)
            count += 1
        return count

    def query_events(
        self,
        *,
        eui64: str | None = None,
        event_type: str | None = None,
        since: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 1000))
        clauses: list[str] = []
        params: list[Any] = []
        if eui64:
            clauses.append("eui64 = ?")
            params.append(eui64)
        if event_type:
            clauses.append("type = ?")
            params.append(event_type)
        if since:
            clauses.append("ts >= ?")
            params.append(since)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT * FROM events{where} ORDER BY ts DESC, id DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_event(r) for r in rows]

    # -- nodes ---------------------------------------------------------

    def upsert_node_metadata(
        self,
        *,
        eui64: str,
        friendly_name: str | None = None,
        area: str | None = None,
        device_id: str | None = None,
        role: str | None = None,
    ) -> None:
        now = _utc_now()
        with self._tx() as conn:
            conn.execute(
                """
                INSERT INTO nodes(eui64, friendly_name, area, device_id, role,
                                  first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(eui64) DO UPDATE SET
                    friendly_name = COALESCE(excluded.friendly_name, nodes.friendly_name),
                    area          = COALESCE(excluded.area,          nodes.area),
                    device_id     = COALESCE(excluded.device_id,     nodes.device_id),
                    role          = COALESCE(excluded.role,          nodes.role),
                    last_seen     = excluded.last_seen
                """,
                (eui64, friendly_name, area, device_id, role, now, now),
            )

    def get_node(self, eui64: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM nodes WHERE eui64 = ?", (eui64,)
            ).fetchone()
        return dict(row) if row else None

    def list_nodes(self) -> list[dict[str, Any]]:
        """Return all nodes ordered by last_seen DESC."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM nodes ORDER BY last_seen DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_node_by_friendly_name(self, name: str) -> dict[str, Any] | None:
        """Lookup a node by its friendly_name (case-insensitive)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM nodes WHERE LOWER(friendly_name) = LOWER(?)",
                (name,),
            ).fetchone()
        return dict(row) if row else None

    def set_node_friendly_name(self, eui64: str, friendly_name: str) -> bool:
        """Set or update the friendly_name for a node. Returns True if updated."""
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE nodes SET friendly_name = ? WHERE eui64 = ?",
                (friendly_name, eui64),
            )
            return cur.rowcount > 0

    # -- stats ---------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        with self._lock:
            counts = {
                t: int(self._conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
                for t in ("nodes", "events", "issues", "metadata_cache", "ingest_state")
            }
            oldest = self._conn.execute("SELECT MIN(ts) FROM events").fetchone()[0]
            newest = self._conn.execute("SELECT MAX(ts) FROM events").fetchone()[0]
        try:
            size_bytes = self.db_path.stat().st_size
        except OSError:
            size_bytes = 0
        return {
            "db_path": str(self.db_path),
            "schema_version": self.schema_version,
            "size_bytes": size_bytes,
            "row_counts": counts,
            "events_oldest": oldest,
            "events_newest": newest,
        }

    def vacuum(self) -> None:
        with self._lock:
            self._conn.execute("VACUUM")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- issues --------------------------------------------------------

    def open_issue(
        self,
        *,
        kind: str,
        severity: str,
        eui64: str | None = None,
        evidence: dict[str, Any] | None = None,
        dedupe: bool = True,
    ) -> int:
        """Open an issue, returning its id.

        If ``dedupe`` is true and an open issue with the same ``kind`` and
        ``eui64`` already exists, that issue's id is returned and the
        evidence is merged via REPLACE (last-write-wins).
        """
        now = _utc_now()
        evidence_json = json.dumps(evidence) if evidence is not None else None
        with self._tx() as conn:
            if dedupe:
                row = conn.execute(
                    "SELECT id FROM issues"
                    " WHERE closed_at IS NULL AND kind = ?"
                    " AND ((eui64 IS NULL AND ? IS NULL) OR eui64 = ?)",
                    (kind, eui64, eui64),
                ).fetchone()
                if row:
                    existing_id = int(row[0])
                    if evidence_json is not None:
                        conn.execute(
                            "UPDATE issues SET evidence_json = ?, severity = ?"
                            " WHERE id = ?",
                            (evidence_json, severity, existing_id),
                        )
                    return existing_id
            cur = conn.execute(
                "INSERT INTO issues(opened_at, severity, kind, eui64, evidence_json)"
                " VALUES (?, ?, ?, ?, ?)",
                (now, severity, kind, eui64, evidence_json),
            )
            return int(cur.lastrowid or 0)

    def close_issue(self, issue_id: int) -> bool:
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE issues SET closed_at = ? WHERE id = ? AND closed_at IS NULL",
                (_utc_now(), issue_id),
            )
            return cur.rowcount > 0

    def list_active_issues(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM issues WHERE closed_at IS NULL"
                " ORDER BY opened_at DESC, id DESC"
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            evj = d.pop("evidence_json", None)
            if evj:
                try:
                    d["evidence"] = json.loads(evj)
                except Exception:  # noqa: BLE001
                    d["evidence"] = {"_raw": evj}
            out.append(d)
        return out


def _row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    payload = d.pop("payload_json", None)
    if payload:
        try:
            d["payload"] = json.loads(payload)
        except Exception:  # noqa: BLE001
            d["payload"] = {"_raw": payload}
    return d


_store: SQLiteStore | None = None
_singleton_lock = threading.Lock()


def get_store() -> SQLiteStore:
    """Return the process-wide store, constructing it on first use."""
    global _store
    if _store is None:
        with _singleton_lock:
            if _store is None:
                _store = SQLiteStore()
    return _store


def reset_store_for_tests(store: SQLiteStore | None = None) -> None:
    """Replace (or clear) the process-wide store. Test-only helper."""
    global _store
    with _singleton_lock:
        if _store is not None and _store is not store:
            try:
                _store.close()
            except Exception:  # noqa: BLE001
                pass
        _store = store

