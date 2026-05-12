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
from datetime import UTC, datetime, timedelta
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
    # v2: thread mesh links from Matter cluster 53 NeighborTable / RouteTable
    """
    CREATE TABLE IF NOT EXISTS links (
        reporter_eui64  TEXT NOT NULL,
        neighbor_eui64  TEXT NOT NULL,
        source          TEXT NOT NULL,   -- 'neighbor_table' | 'route_table'
        rssi_avg        INTEGER,
        rssi_last       INTEGER,
        lqi_in          INTEGER,
        lqi_out         INTEGER,
        is_child        INTEGER,         -- 0/1
        age_seconds     INTEGER,
        frame_error_rate INTEGER,
        message_error_rate INTEGER,
        path_cost       INTEGER,
        observed_at     TEXT NOT NULL,
        PRIMARY KEY (reporter_eui64, neighbor_eui64, source)
    );
    CREATE INDEX IF NOT EXISTS idx_links_reporter ON links(reporter_eui64);
    CREATE INDEX IF NOT EXISTS idx_links_neighbor ON links(neighbor_eui64);
    CREATE INDEX IF NOT EXISTS idx_links_observed ON links(observed_at DESC);
    """,
    # v3: per-node Thread diagnostics scalars (partition, role, leader)
    """
    ALTER TABLE nodes ADD COLUMN partition_id     INTEGER;
    ALTER TABLE nodes ADD COLUMN leader_router_id INTEGER;
    ALTER TABLE nodes ADD COLUMN routing_role     TEXT;
    ALTER TABLE nodes ADD COLUMN active_routers   INTEGER;
    ALTER TABLE nodes ADD COLUMN channel          INTEGER;
    ALTER TABLE nodes ADD COLUMN weighting        INTEGER;
    ALTER TABLE nodes ADD COLUMN diag_updated_at  TEXT;
    """,
    # v4: phantom / liveness tracking. `last_referenced_at` is bumped every
    # time the EUI is observed as a reporter or as a neighbor in any router's
    # table this cycle. `is_phantom` is the derived flag (1 = no recent
    # reference within the configured threshold).
    """
    ALTER TABLE nodes ADD COLUMN last_referenced_at TEXT;
    ALTER TABLE nodes ADD COLUMN is_phantom         INTEGER NOT NULL DEFAULT 0;
    CREATE INDEX IF NOT EXISTS idx_nodes_phantom ON nodes(is_phantom);
    CREATE INDEX IF NOT EXISTS idx_nodes_referenced ON nodes(last_referenced_at DESC);
    """,
    # v5: per-node Thread router_id (within partition) and per-link
    # next_hop_router_id from RouteTable so we can resolve forwarding paths
    # (e.g. "next hop toward OTBR").
    """
    ALTER TABLE nodes ADD COLUMN router_id          INTEGER;
    ALTER TABLE links ADD COLUMN next_hop_router_id INTEGER;
    CREATE INDEX IF NOT EXISTS idx_nodes_router_id ON nodes(router_id);
    """,
    # v6: richer HA-registry metadata. Existing `area` column stays for
    # backwards compatibility (we now mirror `area_name` into it); the new
    # columns let the UI render area/manufacturer/model and link out to
    # the HA device page directly.
    """
    ALTER TABLE nodes ADD COLUMN area_id        TEXT;
    ALTER TABLE nodes ADD COLUMN area_name      TEXT;
    ALTER TABLE nodes ADD COLUMN manufacturer   TEXT;
    ALTER TABLE nodes ADD COLUMN model          TEXT;
    ALTER TABLE nodes ADD COLUMN sw_version     TEXT;
    ALTER TABLE nodes ADD COLUMN hw_version     TEXT;
    ALTER TABLE nodes ADD COLUMN ha_device_path TEXT;
    CREATE INDEX IF NOT EXISTS idx_nodes_area_id ON nodes(area_id);
    """,
    # v7: explicit node status enum, replacing the binary is_phantom flag
    # as the primary lifecycle signal. Values:
    #   'online'       — referenced in current discovery window
    #   'offline'      — not referenced recently but still within retention
    #   'unregistered' — observed via mesh, no HA device_id
    #   'phantom'      — eligible for purge (long-stale, not HA-registered)
    # `is_phantom` stays for now (mirrored from status='phantom') so any
    # external consumer keeps working; remove in a later major.
    """
    ALTER TABLE nodes ADD COLUMN status            TEXT NOT NULL DEFAULT 'online';
    ALTER TABLE nodes ADD COLUMN status_changed_at TEXT;
    CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
    """,
    # v8: enrich `links` with the Matter NeighborTable + RouteTable fields
    # that were previously dropped. These let consumers reason about routing
    # mode (rx-on vs sleepy), full-thread-device vs MTD neighbors, route
    # allocation/establishment state, and partition scoping. Also adds
    # `partition_id` so a link row is always interpretable in the context
    # of the partition it was observed in (link rows referencing a different
    # partition are stale and should be ignored by the route walker).
    """
    ALTER TABLE links ADD COLUMN rx_on_when_idle    INTEGER;
    ALTER TABLE links ADD COLUMN full_thread_device INTEGER;
    ALTER TABLE links ADD COLUMN full_network_data  INTEGER;
    ALTER TABLE links ADD COLUMN link_frame_counter INTEGER;
    ALTER TABLE links ADD COLUMN mle_frame_counter  INTEGER;
    ALTER TABLE links ADD COLUMN link_established   INTEGER;
    ALTER TABLE links ADD COLUMN allocated          INTEGER;
    ALTER TABLE links ADD COLUMN partition_id       INTEGER;
    CREATE INDEX IF NOT EXISTS idx_links_partition ON links(partition_id);
    """,
    # v9: registry-first node model. The `nodes` table is now authoritative
    # for "what devices exist", sourced exclusively from the HA device
    # registry (plus the OTBR). Stray EUIs seen in NeighborTable / RouteTable
    # rows of other routers no longer create node rows — they become a
    # `neighbor_known = 0` flag on the link row instead. This eliminates the
    # phantom-node problem at its root: dead links can't masquerade as
    # devices, and online/offline state belongs to HA, not our heuristics.
    #
    # `is_thread` distinguishes Thread Matter devices from WiFi Matter
    # devices in the same registry (we only care about Thread ones).
    """
    ALTER TABLE nodes ADD COLUMN is_thread INTEGER;
    ALTER TABLE links ADD COLUMN neighbor_known INTEGER NOT NULL DEFAULT 1;
    CREATE INDEX IF NOT EXISTS idx_nodes_is_thread ON nodes(is_thread);
    CREATE INDEX IF NOT EXISTS idx_links_stale ON links(neighbor_known) WHERE neighbor_known = 0;
    """,
    # v10: stability telemetry + Thread Network Data.
    #
    # `nodes` gains the cluster 53 RoleCount counters (0x000E–0x0011) plus
    # AttachAttemptCount (0x0012) and ParentChangeCount (0x0015). These are
    # monotonically-increasing device-side counters that survive across our
    # snapshots — a child cycling between attached/detached every hour is
    # visible as DetachedRoleCount climbing fast, regardless of whether
    # our discovery cycle happened to catch the event itself.
    #
    # `network_data` is a new table: one row per partition_id holding the
    # OTBR-side Thread Network Data (PAN ID, channel, on-mesh prefixes,
    # external routes, BR servers, SRP services). Partition-wide facts
    # don't belong on the OTBR's node row; they're the network's identity,
    # not the device's. Stored as the partition's most-recent active
    # dataset; older partitions (from a split that healed) age out.
    """
    ALTER TABLE nodes ADD COLUMN detached_role_count   INTEGER;
    ALTER TABLE nodes ADD COLUMN router_role_count     INTEGER;
    ALTER TABLE nodes ADD COLUMN leader_role_count     INTEGER;
    ALTER TABLE nodes ADD COLUMN attach_attempt_count  INTEGER;
    ALTER TABLE nodes ADD COLUMN parent_change_count   INTEGER;

    CREATE TABLE IF NOT EXISTS network_data (
        partition_id      INTEGER PRIMARY KEY,
        otbr_eui64        TEXT,
        pan_id            TEXT,
        extended_pan_id   TEXT,
        network_name      TEXT,
        channel           INTEGER,
        channel_mask      TEXT,
        mesh_local_prefix TEXT,
        on_mesh_prefixes  TEXT,
        external_routes   TEXT,
        services          TEXT,
        br_servers        TEXT,
        active_timestamp  TEXT,
        observed_at       TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_network_data_observed ON network_data(observed_at DESC);
    """,
    # v11: HA-entity-availability-based online signal. Until v11 the
    # ``status`` column was derived from ``last_referenced_at`` recency
    # (i.e. "did we see this EUI in any Matter cluster sweep recently").
    # That definition diverges from the user's mental model — what the
    # user actually cares about is "can HA control this device right
    # now?". v11 introduces a first-class availability signal sourced
    # from HA's entity states; ``last_referenced_at`` is preserved as a
    # separate diagnostic field so the UI can surface mesh-vs-HA
    # disagreement (mesh sees it but HA can't reach it = integration bug;
    # HA sees it but mesh doesn't = sleepy child / stale registry).
    #
    # Columns:
    #   available              — 1 (HA can reach at least one entity),
    #                            0 (all entities unavailable/unknown),
    #                            NULL (no availability lookup yet).
    #   availability_source    — 'ha_entity' | 'otbr_rest' | 'unknown'.
    #   availability_checked_at — ISO timestamp of the last lookup.
    """
    ALTER TABLE nodes ADD COLUMN available              INTEGER;
    ALTER TABLE nodes ADD COLUMN availability_source    TEXT;
    ALTER TABLE nodes ADD COLUMN availability_checked_at TEXT;
    CREATE INDEX IF NOT EXISTS idx_nodes_available ON nodes(available);
    """,
    # v12: drop the legacy ``is_phantom`` column + its index.
    # ``status = 'phantom'`` has been the authoritative lifecycle signal
    # since v7; ``is_phantom`` was kept only as a transitional mirror for
    # external consumers and is no longer written by
    # ``recompute_node_statuses``. SQLite >= 3.35 supports ALTER TABLE
    # DROP COLUMN; the addon ships Python 3.12 with a newer sqlite3
    # module so this is safe.
    """
    DROP INDEX IF EXISTS idx_nodes_phantom;
    ALTER TABLE nodes DROP COLUMN is_phantom;
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
            # Registry-first (v9): only bump last_seen if the node is
            # already known. Unknown EUIs don't get phantom rows from events.
            conn.execute(
                "UPDATE nodes SET last_seen = ? WHERE eui64 = ?",
                (ts, eui64),
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
        area_id: str | None = None,
        area_name: str | None = None,
        manufacturer: str | None = None,
        model: str | None = None,
        sw_version: str | None = None,
        hw_version: str | None = None,
        ha_device_path: str | None = None,
        is_thread: bool | None = None,
    ) -> None:
        now = _utc_now()
        # Keep legacy `area` column populated with the resolved name so older
        # readers continue to work.
        legacy_area = area if area is not None else area_name
        is_thread_val: int | None = (
            None if is_thread is None else (1 if is_thread else 0)
        )
        with self._tx() as conn:
            conn.execute(
                """
                INSERT INTO nodes(eui64, friendly_name, area, device_id, role,
                                  area_id, area_name, manufacturer, model,
                                  sw_version, hw_version, ha_device_path,
                                  is_thread,
                                  first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(eui64) DO UPDATE SET
                    friendly_name  = COALESCE(excluded.friendly_name,  nodes.friendly_name),
                    area           = COALESCE(excluded.area,           nodes.area),
                    device_id      = COALESCE(excluded.device_id,      nodes.device_id),
                    role           = COALESCE(excluded.role,           nodes.role),
                    area_id        = COALESCE(excluded.area_id,        nodes.area_id),
                    area_name      = COALESCE(excluded.area_name,      nodes.area_name),
                    manufacturer   = COALESCE(excluded.manufacturer,   nodes.manufacturer),
                    model          = COALESCE(excluded.model,          nodes.model),
                    sw_version     = COALESCE(excluded.sw_version,     nodes.sw_version),
                    hw_version     = COALESCE(excluded.hw_version,     nodes.hw_version),
                    ha_device_path = COALESCE(excluded.ha_device_path, nodes.ha_device_path),
                    is_thread      = COALESCE(excluded.is_thread,      nodes.is_thread),
                    last_seen      = excluded.last_seen
                """,
                (
                    eui64, friendly_name, legacy_area, device_id, role,
                    area_id, area_name, manufacturer, model,
                    sw_version, hw_version, ha_device_path,
                    is_thread_val,
                    now, now,
                ),
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

    def set_node_metadata(
        self,
        eui64: str,
        friendly_name: str | None = None,
        device_id: str | None = None,
        area: str | None = None,
        role: str | None = None,
    ) -> bool:
        """Update multiple metadata fields for a node. Returns True if any field changed."""
        updates: list[tuple[str, Any]] = []
        params: list[Any] = []
        
        if friendly_name is not None:
            updates.append("friendly_name = ?")
            params.append(friendly_name)
        if device_id is not None:
            updates.append("device_id = ?")
            params.append(device_id)
        if area is not None:
            updates.append("area = ?")
            params.append(area)
        if role is not None:
            updates.append("role = ?")
            params.append(role)
        
        if not updates:
            return False
        
        params.append(eui64)
        query = f"UPDATE nodes SET {', '.join(updates)} WHERE eui64 = ?"
        
        with self._tx() as conn:
            cur = conn.execute(query, params)
            return cur.rowcount > 0

    def set_node_diagnostics(
        self,
        eui64: str,
        *,
        partition_id: int | None = None,
        leader_router_id: int | None = None,
        routing_role: str | None = None,
        active_routers: int | None = None,
        channel: int | None = None,
        weighting: int | None = None,
        detached_role_count: int | None = None,
        router_role_count: int | None = None,
        leader_role_count: int | None = None,
        attach_attempt_count: int | None = None,
        parent_change_count: int | None = None,
    ) -> bool:
        """Update Thread diagnostic scalars for a node. Returns True if row updated.

        All scalars are sourced from the same matter-server poll, so a NULL
        in any field means "device did not report it this cycle" — overwrite
        is the correct behaviour (no COALESCE).

        v10 additions: role-count counters (Matter cluster 53 attrs
        0x000E–0x0011, 0x0012, 0x0015). These are monotonically increasing
        on the device; a sustained climb in ``detached_role_count`` or
        ``parent_change_count`` is the textbook signal of an unstable
        sleepy device, surfaced without us having to catch the events live.
        """
        with self._tx() as conn:
            cur = conn.execute(
                """
                UPDATE nodes SET
                    partition_id         = ?,
                    leader_router_id     = ?,
                    routing_role         = ?,
                    active_routers       = ?,
                    channel              = ?,
                    weighting            = ?,
                    detached_role_count  = ?,
                    router_role_count    = ?,
                    leader_role_count    = ?,
                    attach_attempt_count = ?,
                    parent_change_count  = ?,
                    diag_updated_at      = ?
                WHERE eui64 = ?
                """,
                (
                    partition_id, leader_router_id, routing_role,
                    active_routers, channel, weighting,
                    detached_role_count,
                    router_role_count, leader_role_count,
                    attach_attempt_count, parent_change_count,
                    _utc_now(), eui64,
                ),
            )
            return cur.rowcount > 0

    def set_node_router_id(self, eui64: str, router_id: int | None) -> bool:
        """Persist a node's Thread Router ID (6-bit value within its partition).

        Used to resolve next-hop RouterId references in RouteTable entries
        back to a named node. Pass ``None`` to clear.
        """
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE nodes SET router_id = ? WHERE eui64 = ?",
                (router_id, eui64),
            )
            return cur.rowcount > 0

    # -- network data (v10) -------------------------------------------

    def upsert_network_data(
        self,
        *,
        partition_id: int,
        otbr_eui64: str | None = None,
        pan_id: str | None = None,
        extended_pan_id: str | None = None,
        network_name: str | None = None,
        channel: int | None = None,
        channel_mask: str | None = None,
        mesh_local_prefix: str | None = None,
        on_mesh_prefixes: list[dict[str, Any]] | None = None,
        external_routes: list[dict[str, Any]] | None = None,
        services: list[dict[str, Any]] | None = None,
        br_servers: list[dict[str, Any]] | None = None,
        active_timestamp: str | None = None,
    ) -> None:
        """Upsert the OTBR-sourced Network Data for a partition.

        These are partition-wide facts (PAN ID, channel, on-mesh prefixes,
        BR Server entries, SRP services) — the network's identity, not any
        single device's. Keyed on ``partition_id`` so a partition split is
        visible as two rows side-by-side.
        """
        def _j(v: Any) -> str | None:
            return None if v is None else json.dumps(v)

        with self._tx() as conn:
            conn.execute(
                """
                INSERT INTO network_data(
                    partition_id, otbr_eui64, pan_id, extended_pan_id,
                    network_name, channel, channel_mask, mesh_local_prefix,
                    on_mesh_prefixes, external_routes, services, br_servers,
                    active_timestamp, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(partition_id) DO UPDATE SET
                    otbr_eui64        = COALESCE(excluded.otbr_eui64,        network_data.otbr_eui64),
                    pan_id            = COALESCE(excluded.pan_id,            network_data.pan_id),
                    extended_pan_id   = COALESCE(excluded.extended_pan_id,   network_data.extended_pan_id),
                    network_name      = COALESCE(excluded.network_name,      network_data.network_name),
                    channel           = COALESCE(excluded.channel,           network_data.channel),
                    channel_mask      = COALESCE(excluded.channel_mask,      network_data.channel_mask),
                    mesh_local_prefix = COALESCE(excluded.mesh_local_prefix, network_data.mesh_local_prefix),
                    on_mesh_prefixes  = COALESCE(excluded.on_mesh_prefixes,  network_data.on_mesh_prefixes),
                    external_routes   = COALESCE(excluded.external_routes,   network_data.external_routes),
                    services          = COALESCE(excluded.services,          network_data.services),
                    br_servers        = COALESCE(excluded.br_servers,        network_data.br_servers),
                    active_timestamp  = COALESCE(excluded.active_timestamp,  network_data.active_timestamp),
                    observed_at       = excluded.observed_at
                """,
                (
                    partition_id, otbr_eui64, pan_id, extended_pan_id,
                    network_name, channel, channel_mask, mesh_local_prefix,
                    _j(on_mesh_prefixes), _j(external_routes),
                    _j(services), _j(br_servers),
                    active_timestamp, _utc_now(),
                ),
            )

    def list_network_data(self) -> list[dict[str, Any]]:
        """Return all known partition Network Data rows, freshest first."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM network_data ORDER BY observed_at DESC"
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            for k in ("on_mesh_prefixes", "external_routes", "services", "br_servers"):
                if d.get(k):
                    try:
                        d[k] = json.loads(d[k])
                    except Exception:  # noqa: BLE001
                        pass
            out.append(d)
        return out

    def get_network_data(self, partition_id: int) -> dict[str, Any] | None:
        """Return one partition's Network Data row, or None."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM network_data WHERE partition_id = ?",
                (partition_id,),
            ).fetchone()
        if not row:
            return None
        d = dict(row)
        for k in ("on_mesh_prefixes", "external_routes", "services", "br_servers"):
            if d.get(k):
                try:
                    d[k] = json.loads(d[k])
                except Exception:  # noqa: BLE001
                    pass
        return d

    # -- links ---------------------------------------------------------

    def bump_last_referenced(self, euis: Iterable[str]) -> int:
        """Update ``last_referenced_at`` for every known EUI in the batch.

        **Registry-first contract (v9):** this method is UPDATE-only. EUIs
        that aren't already in the ``nodes`` table are silently skipped —
        they belong to stale neighbor/route references and have no business
        in our authoritative node set. Use :meth:`upsert_node_metadata`
        (driven by the HA device registry sync) to add real nodes.

        Returns the number of rows that were actually touched.
        """
        ts = _utc_now()
        n = 0
        with self._tx() as conn:
            for eui in euis:
                if not eui:
                    continue
                cur = conn.execute(
                    "UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?",
                    (ts, eui),
                )
                n += int(cur.rowcount or 0)
        return n

    def apply_availability(
        self,
        updates: Iterable[tuple[str, bool | None, str]],
    ) -> dict[str, int]:
        """Stamp the ``available`` / ``availability_source`` columns.

        ``updates`` is an iterable of ``(eui64, available, source)`` tuples,
        where ``available`` is ``True``/``False``/``None`` and ``source`` is
        one of ``'ha_entity'``, ``'otbr_rest'``, ``'unknown'``. EUIs not
        already in ``nodes`` are silently skipped (same UPDATE-only contract
        as :meth:`bump_last_referenced`).

        Returns ``{applied, skipped}``.
        """
        ts = _utc_now()
        applied = 0
        skipped = 0
        with self._tx() as conn:
            for eui, avail, source in updates:
                if not eui:
                    continue
                val = None if avail is None else (1 if avail else 0)
                cur = conn.execute(
                    "UPDATE nodes"
                    "    SET available = ?,"
                    "        availability_source = ?,"
                    "        availability_checked_at = ?"
                    "  WHERE eui64 = ?",
                    (val, source, ts, eui),
                )
                if cur.rowcount:
                    applied += int(cur.rowcount)
                else:
                    skipped += 1
        return {"applied": applied, "skipped": skipped}

    def list_phantom_nodes(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM nodes WHERE status = 'phantom'"
                " ORDER BY last_referenced_at IS NULL DESC, last_referenced_at ASC"
            ).fetchall()
        return [dict(r) for r in rows]

    def purge_phantom_nodes(self) -> dict[str, Any]:
        """Delete phantom nodes and any links referencing them. Returns counts."""
        with self._tx() as conn:
            rows = conn.execute(
                "SELECT eui64 FROM nodes WHERE status = 'phantom'"
            ).fetchall()
            euis = [r[0] for r in rows]
            if not euis:
                return {"deleted_nodes": 0, "deleted_links": 0, "euis": []}
            placeholders = ",".join("?" for _ in euis)
            cur_links = conn.execute(
                f"DELETE FROM links WHERE reporter_eui64 IN ({placeholders})"
                f"    OR neighbor_eui64 IN ({placeholders})",
                (*euis, *euis),
            )
            cur_nodes = conn.execute(
                f"DELETE FROM nodes WHERE eui64 IN ({placeholders})",
                euis,
            )
            return {
                "deleted_nodes": int(cur_nodes.rowcount or 0),
                "deleted_links": int(cur_links.rowcount or 0),
                "euis": euis,
            }

    def recompute_node_statuses(
        self,
        *,
        offline_seconds: int,
        phantom_seconds: int,
    ) -> dict[str, int]:
        """Recompute the ``status`` column for every node.

        **v11 contract — availability-first.** "Online" now means "Home
        Assistant can reach at least one entity backed by this device right
        now", i.e. ``available = 1``. Mesh-side observation
        (``last_referenced_at``) is preserved as an independent diagnostic
        field but no longer drives the primary status — the two signals
        intentionally disagree when something is wrong (mesh-visible but
        HA-unreachable = Matter integration bug; HA-reachable but
        mesh-invisible = sleepy child or bridged device).

        State machine (evaluated atomically against the current timestamp):

        * ``online``       — ``available = 1`` (HA can talk to it).
        * ``offline``      — ``available = 0`` AND HA-registered
          (``device_id`` not null), OR ``available IS NULL`` but the row
          has a ``device_id`` (registry-known, availability not yet
          probed). HA-registered nodes never auto-purge.
        * ``unregistered`` — no ``device_id`` AND never referenced.
        * ``phantom``      — last referenced longer than ``phantom_seconds``
          ago AND not HA-registered. Eligible for ``purge_expired_nodes``.

        The ``offline_seconds`` parameter is retained for backwards
        compatibility but is now only consulted as a fallback when
        availability has never been probed.

        ``status_changed_at`` is bumped only when the value actually changes.

        Returns ``{state: count}`` summary plus ``changed`` (number of rows
        whose status flipped this call).
        """
        now = _utc_now()
        offline_cutoff = (
            datetime.now(tz=UTC) - timedelta(seconds=offline_seconds)
        ).isoformat()
        phantom_cutoff = (
            datetime.now(tz=UTC) - timedelta(seconds=phantom_seconds)
        ).isoformat()
        with self._tx() as conn:
            cur = conn.execute(
                """
                UPDATE nodes
                   SET status_changed_at = CASE
                           WHEN status <> new_status THEN ?
                           ELSE status_changed_at
                       END,
                       status     = new_status
                  FROM (
                      SELECT eui64,
                             CASE
                                 -- Primary signal: HA entity availability.
                                 WHEN available = 1 THEN 'online'
                                 WHEN available = 0 AND device_id IS NOT NULL
                                     THEN 'offline'
                                 -- Availability not yet probed: fall back to
                                 -- last_referenced recency for registered
                                 -- nodes; otherwise treat as unregistered.
                                 WHEN available IS NULL
                                  AND device_id IS NOT NULL
                                  AND last_referenced_at IS NOT NULL
                                  AND last_referenced_at >= ?
                                     THEN 'online'
                                 WHEN device_id IS NOT NULL THEN 'offline'
                                 -- No device_id: aged out or never seen.
                                 WHEN last_referenced_at IS NULL THEN 'unregistered'
                                 WHEN last_referenced_at < ? THEN 'phantom'
                                 ELSE 'unregistered'
                             END AS new_status
                        FROM nodes
                  ) AS calc
                 WHERE nodes.eui64 = calc.eui64
                """,
                (now, offline_cutoff, phantom_cutoff),
            )
            changed = int(cur.rowcount or 0)
            counts = {
                row["status"]: int(row["n"])
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS n FROM nodes GROUP BY status"
                ).fetchall()
            }
        out = {
            "online": counts.get("online", 0),
            "offline": counts.get("offline", 0),
            "unregistered": counts.get("unregistered", 0),
            "phantom": counts.get("phantom", 0),
            "changed": changed,
        }
        return out

    def purge_expired_nodes(self, *, max_offline_seconds: int) -> dict[str, Any]:
        """Delete nodes in 'phantom' state OR 'offline' beyond the retention
        window, provided they are NOT HA-registered.

        HA-registered nodes (``device_id`` not null) are preserved indefinitely
        — they represent something the user owns and we should respect.

        Also removes any link rows touching the deleted EUIs.
        """
        cutoff = (
            datetime.now(tz=UTC) - timedelta(seconds=max_offline_seconds)
        ).isoformat()
        with self._tx() as conn:
            rows = conn.execute(
                """
                SELECT eui64 FROM nodes
                 WHERE device_id IS NULL
                   AND (
                        status = 'phantom'
                     OR (status = 'offline' AND (status_changed_at IS NULL OR status_changed_at < ?))
                   )
                """,
                (cutoff,),
            ).fetchall()
            euis = [r[0] for r in rows]
            if not euis:
                return {"deleted_nodes": 0, "deleted_links": 0, "euis": []}
            placeholders = ",".join("?" for _ in euis)
            cur_links = conn.execute(
                f"DELETE FROM links WHERE reporter_eui64 IN ({placeholders})"
                f"    OR neighbor_eui64 IN ({placeholders})",
                (*euis, *euis),
            )
            cur_nodes = conn.execute(
                f"DELETE FROM nodes WHERE eui64 IN ({placeholders})",
                euis,
            )
            return {
                "deleted_nodes": int(cur_nodes.rowcount or 0),
                "deleted_links": int(cur_links.rowcount or 0),
                "euis": euis,
            }

    def replace_links_for_reporter(
        self,
        reporter_eui64: str,
        source: str,
        links: list[dict[str, Any]],
        *,
        partition_id: int | None = None,
    ) -> int:
        """Replace all links for a given (reporter, source) tuple atomically.

        Each link dict may include: neighbor_eui64 (required), rssi_avg,
        rssi_last, lqi_in, lqi_out, is_child, age_seconds, frame_error_rate,
        message_error_rate, path_cost, next_hop_router_id, rx_on_when_idle,
        full_thread_device, full_network_data, link_frame_counter,
        mle_frame_counter, link_established, allocated.

        ``partition_id`` (if known) is stamped onto every row so stale rows
        from a previous partition can be detected without re-scanning the
        whole table.

        Returns the number of link rows inserted.
        """
        now = _utc_now()
        inserted = 0
        with self._tx() as conn:
            conn.execute(
                "DELETE FROM links WHERE reporter_eui64 = ? AND source = ?",
                (reporter_eui64, source),
            )
            # Build the "known EUI" set once per call. Cheaper than a
            # subquery per row and we'll need this for every link anyway.
            known_euis = {
                r[0] for r in conn.execute("SELECT eui64 FROM nodes").fetchall()
            }
            for link in links:
                neighbor = link.get("neighbor_eui64")
                if not neighbor:
                    continue

                def _b(v: Any) -> int | None:
                    if v is None:
                        return None
                    return 1 if v else 0

                # Registry-first (v9): mark the row as a stale reference if
                # the neighbor isn't in the (registry-driven) nodes table.
                neighbor_known = 1 if neighbor in known_euis else 0

                conn.execute(
                    """
                    INSERT INTO links(
                        reporter_eui64, neighbor_eui64, source,
                        rssi_avg, rssi_last, lqi_in, lqi_out,
                        is_child, age_seconds,
                        frame_error_rate, message_error_rate, path_cost,
                        next_hop_router_id,
                        rx_on_when_idle, full_thread_device, full_network_data,
                        link_frame_counter, mle_frame_counter,
                        link_established, allocated, partition_id,
                        neighbor_known,
                        observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        reporter_eui64,
                        neighbor,
                        source,
                        link.get("rssi_avg"),
                        link.get("rssi_last"),
                        link.get("lqi_in"),
                        link.get("lqi_out"),
                        _b(link.get("is_child")),
                        link.get("age_seconds"),
                        link.get("frame_error_rate"),
                        link.get("message_error_rate"),
                        link.get("path_cost"),
                        link.get("next_hop_router_id"),
                        _b(link.get("rx_on_when_idle")),
                        _b(link.get("full_thread_device")),
                        _b(link.get("full_network_data")),
                        link.get("link_frame_counter"),
                        link.get("mle_frame_counter"),
                        _b(link.get("link_established")),
                        _b(link.get("allocated")),
                        partition_id,
                        neighbor_known,
                        now,
                    ),
                )
                inserted += 1
        return inserted

    def list_links(self, source: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM links"
        params: list[Any] = []
        if source:
            sql += " WHERE source = ?"
            params.append(source)
        sql += " ORDER BY reporter_eui64, neighbor_eui64"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def list_stale_links(self) -> list[dict[str, Any]]:
        """Return every link row whose ``neighbor_eui64`` is not in the nodes
        table — i.e., neighbor/route references to EUIs HA has never heard of.

        These are the troubleshooting bait: a router is forwarding to (or
        seeing as a neighbor) an EUI that no Matter device is registered
        under. Usually a recommissioned device that left a stale router
        cache, or a device whose Matter pairing failed but Thread retained
        a frame counter.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM links WHERE neighbor_known = 0"
                " ORDER BY reporter_eui64, neighbor_eui64"
            ).fetchall()
        return [dict(r) for r in rows]

    def refresh_neighbor_known(self) -> dict[str, int]:
        """Recompute ``neighbor_known`` for every link against the current
        nodes table. Call this after the HA registry sync stage adds or
        removes nodes so existing link rows reflect the new node set
        without waiting for the reporter to be re-polled.

        Returns ``{marked_known, marked_stale}``.
        """
        with self._tx() as conn:
            cur1 = conn.execute(
                "UPDATE links SET neighbor_known = 1"
                " WHERE neighbor_known = 0"
                "   AND neighbor_eui64 IN (SELECT eui64 FROM nodes)"
            )
            cur2 = conn.execute(
                "UPDATE links SET neighbor_known = 0"
                " WHERE neighbor_known = 1"
                "   AND neighbor_eui64 NOT IN (SELECT eui64 FROM nodes)"
            )
            return {
                "marked_known": int(cur1.rowcount or 0),
                "marked_stale": int(cur2.rowcount or 0),
            }

    def sweep_stale_links(self, ttl_seconds: int) -> int:
        """Delete link rows whose ``observed_at`` is older than the TTL.

        ``replace_links_for_reporter`` only overwrites rows for the
        (reporter, source) tuples that report this cycle. If a reporter
        goes silent (powered off, removed from fabric, falls off-mesh)
        its rows never refresh and persist forever, causing dead nodes
        to keep appearing as "peer of X" long after they are gone.

        Calling this each discovery cycle with a TTL of roughly
        3\u00d7 the discovery interval purges those stale rows without
        risking false-positives during a single missed poll.

        Returns the number of rows deleted.
        """
        cutoff = (datetime.now(tz=UTC) - timedelta(seconds=ttl_seconds)).isoformat()
        with self._tx() as conn:
            cur = conn.execute(
                "DELETE FROM links WHERE observed_at < ?", (cutoff,),
            )
            return int(cur.rowcount or 0)

    # -- stats ---------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        with self._lock:
            counts = {
                t: int(self._conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
                for t in ("nodes", "events", "issues", "metadata_cache", "ingest_state", "links", "network_data")
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

    def reset_data(self) -> int:
        """Truncate all observed-state tables, preserving schema.

        The DB is a live cache of what the Thread fabric currently reports;
        anything we keep across restarts that does not come back in the next
        poll cycle is by definition stale. Wiping on every boot makes the
        DB authoritative-by-construction: if a node/link reappears, it is
        real; if it does not, it was zombie data.

        Preserves: ``schema_version`` (migrations stay applied).
        Wipes: ``nodes``, ``links``, ``events``, ``issues``,
        ``metadata_cache``, ``ingest_state``.

        Returns the total number of rows deleted across all tables.
        """
        tables = (
            "links",
            "events",
            "issues",
            "nodes",
            "network_data",
            "metadata_cache",
            "ingest_state",
        )
        total = 0
        with self._tx() as conn:
            for t in tables:
                # Use a guarded delete so missing tables (older schema) are skipped.
                exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (t,),
                ).fetchone()
                if not exists:
                    continue
                cur = conn.execute(f"DELETE FROM {t}")
                total += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        # Reclaim space outside the transaction.
        try:
            self._conn.execute("VACUUM")
        except sqlite3.OperationalError:
            pass
        return total

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

