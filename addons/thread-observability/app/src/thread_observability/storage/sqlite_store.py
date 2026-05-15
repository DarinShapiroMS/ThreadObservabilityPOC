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

from ..utils.datetime import utc_now_iso

DEFAULT_DB_PATH = Path(
    os.getenv("THREAD_OBS_DB_PATH", "/data/thread-observability/state.db")
)

# A sleepy end device can look unavailable to HA while still being freshly
# claimed by its parent router in NeighborTable. Keep this window aligned
# with the topology/UI SED mesh-alive view.
_SED_MESH_ALIVE_WINDOW_SECONDS = 300

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
    # v13: cluster 53 Tx/Rx error counters + partition-change counters.
    #
    # The role counters captured in v10 told us about MLE state churn
    # (router/leader/detached cycles, parent changes). They do not say
    # anything about radio-layer health: a router that is silently
    # dropping 30% of its transmissions because of a failing antenna or
    # an RF-noisy neighbour looks identical in v10 to a perfectly healthy
    # one. v13 adds the per-node MAC counters that expose this directly.
    #
    # Selected attributes (Matter ThreadNetworkDiagnostics cluster 0x0035):
    #   0x0013 PartitionIdChangeCount                — how often THIS node
    #                                                  has changed partition
    #   0x0014 BetterPartitionAttachAttemptCount     — actively trying to
    #                                                  merge into a stronger
    #                                                  partition (split heal)
    #   0x0016 TxTotalCount                          — denominator for tx rates
    #   0x0021 TxRetryCount                          — MAC retransmits
    #   0x0024 TxErrCcaCount                         — channel-busy failures
    #                                                  (interference)
    #   0x0025 TxErrAbortCount                       — driver aborts
    #   0x0026 TxErrBusyChannelCount                 — CSMA backoff failures
    #   0x0027 RxTotalCount                          — denominator for rx rates
    #   0x0031 RxDuplicatedCount                     — acks lost or peer
    #                                                  retransmitting
    #   0x0032 RxErrNoFrameCount                     — corrupted preamble
    #   0x0035 RxErrSecCount                         — failed MIC/auth
    #                                                  (key roll? attacker?)
    #   0x0036 RxErrFcsCount                         — bad FCS (RF noise,
    #                                                  weak signal)
    #
    # All counters are monotonically increasing on the device; the addon
    # snapshots them once per discovery cycle and computes per-tick deltas
    # at read time (no new index needed — these are looked up by eui64).
    """
    ALTER TABLE nodes ADD COLUMN partition_id_change_count               INTEGER;
    ALTER TABLE nodes ADD COLUMN better_partition_attach_attempt_count   INTEGER;
    ALTER TABLE nodes ADD COLUMN tx_total_count                          INTEGER;
    ALTER TABLE nodes ADD COLUMN tx_retry_count                          INTEGER;
    ALTER TABLE nodes ADD COLUMN tx_err_cca_count                        INTEGER;
    ALTER TABLE nodes ADD COLUMN tx_err_abort_count                      INTEGER;
    ALTER TABLE nodes ADD COLUMN tx_err_busy_channel_count               INTEGER;
    ALTER TABLE nodes ADD COLUMN rx_total_count                          INTEGER;
    ALTER TABLE nodes ADD COLUMN rx_duplicated_count                     INTEGER;
    ALTER TABLE nodes ADD COLUMN rx_err_no_frame_count                   INTEGER;
    ALTER TABLE nodes ADD COLUMN rx_err_sec_count                        INTEGER;
    ALTER TABLE nodes ADD COLUMN rx_err_fcs_count                        INTEGER;
    """,
    # v14: rloc16 column + observed-at timestamps for OTBR-side cross-check.
    #
    # The 16-bit RLOC (mesh-local short address) is derived from a router's
    # 6-bit RouterId as ``router_id << 10``. Tracking it explicitly serves
    # two goals:
    #   1. ``rloc16_change`` event emission when a router gets re-assigned
    #      a new RouterId within the same partition (router ID churn —
    #      typically signals a leader-side reassignment after a brief
    #      drop). Storing the previous value is the only way to detect
    #      this; an in-memory cache wouldn't survive addon restarts.
    #   2. Future OTBR ``/diagnostics`` cross-checks key targets by RLOC16
    #      rather than by EUI64 (the MGMT_DIAG_GET destination is an
    #      RLOC). Having the mapping pre-computed avoids a router-table
    #      scan on every diagnostic poll.
    #
    # ``otbr_diagnostics`` is the v14 landing table for the OTBR-side
    # second-witness counters. We key by target EUI + observed_at so an
    # operator can replay history; aggregations roll up at query time.
    """
    ALTER TABLE nodes ADD COLUMN rloc16 INTEGER;
    CREATE INDEX IF NOT EXISTS idx_nodes_rloc16 ON nodes(rloc16);

    CREATE TABLE IF NOT EXISTS otbr_diagnostics (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        target_eui64      TEXT NOT NULL,
        target_rloc16     INTEGER,
        observed_at       TEXT NOT NULL,
        partition_id      INTEGER,
        -- Selected MGMT_DIAG_GET TLVs (raw integer counts; serialized
        -- larger TLVs as JSON in ``extra_json`` to keep the schema thin).
        mac_tx_total      INTEGER,
        mac_tx_retry      INTEGER,
        mac_tx_err        INTEGER,
        mac_rx_total      INTEGER,
        mac_rx_err        INTEGER,
        mac_rx_dup        INTEGER,
        mle_counters_json TEXT,
        child_table_json  TEXT,
        extra_json        TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_otbr_diag_target_ts
        ON otbr_diagnostics(target_eui64, observed_at DESC);
    """,
    # v15: observer_events — track restart / outage windows of the
    # ingestion-side software stack so the reasoner can annotate
    # (and downgrade) issues that fire while WE were temporarily blind.
    #
    # ``source`` identifies the component (e.g. ``addon:self``,
    # ``addon:core_openthread_border_router``, ``addon:core_matter_server``).
    # ``kind`` is one of ``start``, ``stop``, ``restart``, ``outage``.
    # ``started_at`` always set; ``ended_at`` set when the event is a
    # bounded window (a restart whose recovery we observed) and NULL
    # while the gap is still open. The reasoner treats an event as
    # "currently suppressing" until ``ended_at + suppression_grace``.
    """
    CREATE TABLE IF NOT EXISTS observer_events (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        source       TEXT NOT NULL,
        kind         TEXT NOT NULL,
        started_at   TEXT NOT NULL,
        ended_at     TEXT,
        details_json TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_observer_events_source_ts
        ON observer_events(source, started_at DESC);
    CREATE INDEX IF NOT EXISTS idx_observer_events_window
        ON observer_events(started_at, ended_at);
    """,
    # v16: topology_snapshots — periodic JSON-blob captures of the
    # full topology so the reasoner / consultant tools can diff "what
    # changed in the last hour" without reconstructing the past from
    # raw events. ``snapshot_hash`` is a stable fingerprint of the
    # snapshot content so the capture stage can skip writing duplicate
    # rows. ``partition_id`` and node/link counts are denormalized
    # summary columns for fast listing.
    """
    CREATE TABLE IF NOT EXISTS topology_snapshots (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        captured_at   TEXT NOT NULL,
        snapshot_hash TEXT NOT NULL,
        partition_id  INTEGER,
        node_count    INTEGER NOT NULL,
        link_count    INTEGER NOT NULL,
        snapshot_json TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_topology_snapshots_ts
        ON topology_snapshots(captured_at DESC);
    CREATE INDEX IF NOT EXISTS idx_topology_snapshots_hash
        ON topology_snapshots(snapshot_hash);
    """,
    # v17 (0.9.46): per-node Thread network identity (cluster 53
    # NetworkName attr 0x0002, ExtendedPanId attr 0x0004) +
    # device-registry physical-identity fields used to detect
    # duplicate commissioning of the same physical hardware.
    #
    # Why on ``nodes`` and not ``network_data``: the per-node copy
    # is the *source of truth for whether this device is on the
    # mesh we expect*. A device whose ``extended_pan_id`` doesn't
    # match the modal value across the mesh is on a different
    # Thread network — even if its ``partition_id`` happens to
    # collide. ``network_data`` continues to hold partition-wide
    # facts (leader-discovered active dataset blob etc.); the new
    # columns hold what each node *itself reports*.
    #
    # ``vendor_id`` / ``product_id`` / ``serial_number`` come from
    # the HA device registry (Basic Information cluster, surfaced
    # via the existing ``discover_thread_devices`` lookup). When
    # two ``nodes`` rows share the same triple they are the same
    # physical hardware re-commissioned — duplicate-identity
    # detection key off this.
    """
    ALTER TABLE nodes ADD COLUMN network_name      TEXT;
    ALTER TABLE nodes ADD COLUMN extended_pan_id   TEXT;
    ALTER TABLE nodes ADD COLUMN vendor_id         INTEGER;
    ALTER TABLE nodes ADD COLUMN product_id        INTEGER;
    ALTER TABLE nodes ADD COLUMN serial_number     TEXT;
    CREATE INDEX IF NOT EXISTS idx_nodes_extended_pan_id
        ON nodes(extended_pan_id);
    CREATE INDEX IF NOT EXISTS idx_nodes_physical_identity
        ON nodes(vendor_id, product_id, serial_number);
    """,
    # v18 (0.9.54 / Phase 1 temporal honesty): pipeline_ticks — one
    # row per unified pipeline tick, recording when it ran, how long
    # each stage took, and which stages succeeded vs. failed. Drives
    # the ``meta.pipeline_tick`` block on every read-tool response so
    # callers can see exactly how stale the SQLite-cached data is and
    # which ingestion stage produced it (or which one failed).
    """
    CREATE TABLE IF NOT EXISTS pipeline_ticks (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at    TEXT NOT NULL,
        completed_at  TEXT NOT NULL,
        duration_s    REAL NOT NULL,
        ok_count      INTEGER NOT NULL,
        fail_count    INTEGER NOT NULL,
        stages_json   TEXT NOT NULL,
        error         TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_pipeline_ticks_completed_at
        ON pipeline_ticks(completed_at DESC);
    """,
    # v19 (0.10.0 / Phase 4 time-series): node_counter_samples — one
    # row per (eui64, observation) recording the live MAC/MLE counter
    # values at that tick. Identity stays scalar on ``nodes``; volatile
    # counters move here so deltas can be computed correctly even when
    # a node resets its counters (a re-attach makes the scalar go
    # *down*, which the reasoner would misread as a huge negative spike).
    """
    CREATE TABLE IF NOT EXISTS node_counter_samples (
        eui64         TEXT NOT NULL,
        observed_at   TEXT NOT NULL,
        tick_id       INTEGER,
        counters_json TEXT NOT NULL,
        PRIMARY KEY (eui64, observed_at)
    );
    CREATE INDEX IF NOT EXISTS idx_node_counter_samples_eui_ts
        ON node_counter_samples(eui64, observed_at DESC);
    CREATE INDEX IF NOT EXISTS idx_node_counter_samples_ts
        ON node_counter_samples(observed_at DESC);
    """,
    # v20 (0.11.0 / Phase 4 Background Diagnostics — #18):
    # assessment_schedule — single-row state machine for the adaptive
    # AI assessment scheduler. Persisted across addon updates so a
    # network that has been "steady" for a week doesn't reset to
    # probation cadence on every release. ``state`` ∈ {probation,
    # relaxing, steady, heightened, engaged, disabled}. ``budget_*``
    # tracks the daily LLM-call budget; ``daily_window_start_at`` is
    # rolled over once per UTC day.
    """
    CREATE TABLE IF NOT EXISTS assessment_schedule (
        id                      INTEGER PRIMARY KEY CHECK (id = 1),
        state                   TEXT NOT NULL DEFAULT 'probation',
        state_since             TEXT NOT NULL,
        last_assessment_at      TEXT,
        next_assessment_at      TEXT,
        consecutive_ok          INTEGER NOT NULL DEFAULT 0,
        consecutive_concern     INTEGER NOT NULL DEFAULT 0,
        current_interval_seconds INTEGER NOT NULL,
        budget_calls_used       INTEGER NOT NULL DEFAULT 0,
        budget_window_start_at  TEXT NOT NULL,
        reason                  TEXT,
        updated_at              TEXT NOT NULL
    );
    """,
    # v21 (0.11.0 / Phase 4 Background Diagnostics — #19):
    # assessment_findings — verdict envelopes produced by the
    # assessment engine. ``finding_key`` = sha1(eui64 || '|' || finding_type)
    # is the dedup handle: a re-occurring concern bumps last_seen_at and
    # confidence rather than creating a new row. ``state`` ∈
    # {open, cleared, dismissed}. ``cleared_by`` ∈ {assessment,
    # user_dismiss, user_resolve, user_wrong}. ``evidence_json`` holds
    # the array of {tool, key_finding} pairs the agent cited;
    # ``suggested_starter_prompt`` is the agent-authored chat opener.
    """
    CREATE TABLE IF NOT EXISTS assessment_findings (
        finding_id              TEXT PRIMARY KEY,
        finding_key             TEXT NOT NULL,
        state                   TEXT NOT NULL DEFAULT 'open',
        verdict                 TEXT NOT NULL,
        severity                TEXT NOT NULL,
        confidence              REAL NOT NULL,
        finding_type            TEXT,
        headline                TEXT NOT NULL,
        evidence_json           TEXT NOT NULL,
        suggested_starter_prompt TEXT,
        node_eui64              TEXT,
        created_at              TEXT NOT NULL,
        last_seen_at            TEXT NOT NULL,
        cleared_at              TEXT,
        cleared_by              TEXT,
        suppress_until          TEXT,
        seen_count              INTEGER NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_findings_open
        ON assessment_findings(state, last_seen_at);
    CREATE INDEX IF NOT EXISTS idx_findings_key
        ON assessment_findings(finding_key);
    CREATE INDEX IF NOT EXISTS idx_findings_eui64
        ON assessment_findings(node_eui64);
    """,
    # v22 (0.11.0 / Phase 4 Background Diagnostics — #22):
    # assessment_feedback — outcomes for findings, captured via the
    # ``mark_finding_outcome`` MCP tool or implicitly by the engine
    # (auto-clear → ignored_expired) and the dismiss flow
    # (→ ignored_dismissed). Powers precision metrics + the
    # ``noisy_signal_types`` callout in get_assessment_quality.
    """
    CREATE TABLE IF NOT EXISTS assessment_feedback (
        finding_id              TEXT PRIMARY KEY
            REFERENCES assessment_findings(finding_id),
        outcome                 TEXT NOT NULL,
        outcome_at              TEXT NOT NULL,
        finding_type            TEXT,
        notes                   TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_feedback_outcome_at
        ON assessment_feedback(outcome_at DESC);
    CREATE INDEX IF NOT EXISTS idx_feedback_finding_type
        ON assessment_feedback(finding_type);
    """,
    # v23 (0.11.1 / Phase 4 follow-up): assessment_runs — append-only
    # execution history for the Adaptive Monitoring side-panel. Unlike
    # assessment_findings (which dedups active concerns by finding_key),
    # this keeps a row per run so the UI can show recent "ok" / "watch"
    # checks and the backend can paginate history cleanly.
    """
    CREATE TABLE IF NOT EXISTS assessment_runs (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        assessed_at             TEXT NOT NULL,
        verdict                 TEXT NOT NULL,
        severity                TEXT NOT NULL,
        confidence              REAL NOT NULL,
        headline                TEXT NOT NULL,
        finding_key             TEXT,
        finding_id              TEXT,
        finding_type            TEXT,
        node_eui64              TEXT,
        parse_attempts          INTEGER NOT NULL DEFAULT 0,
        duration_seconds        REAL NOT NULL DEFAULT 0,
        suppressed              INTEGER NOT NULL DEFAULT 0,
        dedup_hit               INTEGER NOT NULL DEFAULT 0,
        cleared_count           INTEGER NOT NULL DEFAULT 0,
        model_name              TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_assessment_runs_assessed_at
        ON assessment_runs(assessed_at DESC);
    CREATE INDEX IF NOT EXISTS idx_assessment_runs_verdict
        ON assessment_runs(verdict, assessed_at DESC);
    """,
    # v24 (0.11.14): persisted chat session memory. Unlike the live mesh
    # cache tables, this preserves short-term investigation context across
    # add-on restarts so follow-up chat turns can keep structured facts,
    # hypotheses, and pending questions without replaying full history.
    """
    CREATE TABLE IF NOT EXISTS chat_session_memory (
        conversation_id  TEXT PRIMARY KEY,
        created_at       TEXT NOT NULL,
        updated_at       TEXT NOT NULL,
        expires_at       TEXT,
        payload_json     TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_chat_session_memory_updated_at
        ON chat_session_memory(updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_chat_session_memory_expires_at
        ON chat_session_memory(expires_at);
    """,
    # v25 (0.11.20): aggregate-safe chat telemetry. Stores one row per turn
    # with latency / tool-use / outcome metadata only; never raw message text.
    """
    CREATE TABLE IF NOT EXISTS chat_turn_stats (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        conversation_id     TEXT,
        recorded_at         TEXT NOT NULL,
        backend             TEXT NOT NULL,
        agent_id            TEXT,
        model_name          TEXT,
        status              TEXT NOT NULL,
        error_kind          TEXT,
        duration_ms         INTEGER NOT NULL DEFAULT 0,
        tool_call_count     INTEGER NOT NULL DEFAULT 0,
        had_page_context    INTEGER NOT NULL DEFAULT 0,
        selected_node_eui64 TEXT,
        active_tab          TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_chat_turn_stats_recorded_at
        ON chat_turn_stats(recorded_at DESC);
    CREATE INDEX IF NOT EXISTS idx_chat_turn_stats_backend
        ON chat_turn_stats(backend, recorded_at DESC);
    CREATE INDEX IF NOT EXISTS idx_chat_turn_stats_status
        ON chat_turn_stats(status, recorded_at DESC);
    """,
    # v26 (0.11.28 local batch): persist SQLite file size on each pipeline
    # tick so Diagnostics can estimate recent DB growth without inventing a
    # separate history table.
    """
    ALTER TABLE pipeline_ticks ADD COLUMN db_size_bytes INTEGER;
    """,
]


def _utc_now() -> str:
    return utc_now_iso()


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
        # v17 (0.9.46) — physical-identity fields from HA device registry.
        # Used to group duplicate commissionings of the same physical
        # hardware. ``serial_number`` is the Matter Basic Information
        # cluster's SerialNumber attribute, surfaced by HA's device
        # registry alongside manufacturer/model.
        vendor_id: int | None = None,
        product_id: int | None = None,
        serial_number: str | None = None,
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
                                  vendor_id, product_id, serial_number,
                                  first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    vendor_id      = COALESCE(excluded.vendor_id,      nodes.vendor_id),
                    product_id     = COALESCE(excluded.product_id,     nodes.product_id),
                    serial_number  = COALESCE(excluded.serial_number,  nodes.serial_number),
                    last_seen      = excluded.last_seen
                """,
                (
                    eui64, friendly_name, legacy_area, device_id, role,
                    area_id, area_name, manufacturer, model,
                    sw_version, hw_version, ha_device_path,
                    is_thread_val,
                    vendor_id, product_id, serial_number,
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
        # v13: partition-stability counters
        partition_id_change_count: int | None = None,
        better_partition_attach_attempt_count: int | None = None,
        # v13: MAC-layer Tx counters
        tx_total_count: int | None = None,
        tx_retry_count: int | None = None,
        tx_err_cca_count: int | None = None,
        tx_err_abort_count: int | None = None,
        tx_err_busy_channel_count: int | None = None,
        # v13: MAC-layer Rx counters
        rx_total_count: int | None = None,
        rx_duplicated_count: int | None = None,
        rx_err_no_frame_count: int | None = None,
        rx_err_sec_count: int | None = None,
        rx_err_fcs_count: int | None = None,
        # v17 (0.9.46): per-node Thread network identity (cluster 53
        # attrs 0x0002 NetworkName, 0x0004 ExtendedPanId). These let
        # the consultant distinguish "on the right Thread network but
        # split into multiple partitions" (RF issue) from "on a
        # different Thread network entirely" (credentials mismatch).
        network_name: str | None = None,
        extended_pan_id: str | None = None,
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

        v13 additions: MAC-layer Tx/Rx counters and partition-stability
        counters. These expose radio-layer health independently of MLE
        state: a node with a fast-climbing ``tx_err_cca_count`` is seeing
        interference; a fast-climbing ``rx_err_fcs_count`` is on a noisy
        channel; ``rx_err_sec_count`` going up is a key-rotation or
        attacker signal. All are monotonic; rate is computed from
        snapshot deltas at read time.
        """
        with self._tx() as conn:
            cur = conn.execute(
                """
                UPDATE nodes SET
                    partition_id                         = ?,
                    leader_router_id                     = ?,
                    routing_role                         = ?,
                    active_routers                       = ?,
                    channel                              = ?,
                    weighting                            = ?,
                    detached_role_count                  = ?,
                    router_role_count                    = ?,
                    leader_role_count                    = ?,
                    attach_attempt_count                 = ?,
                    parent_change_count                  = ?,
                    partition_id_change_count            = ?,
                    better_partition_attach_attempt_count = ?,
                    tx_total_count                       = ?,
                    tx_retry_count                       = ?,
                    tx_err_cca_count                     = ?,
                    tx_err_abort_count                   = ?,
                    tx_err_busy_channel_count            = ?,
                    rx_total_count                       = ?,
                    rx_duplicated_count                  = ?,
                    rx_err_no_frame_count                = ?,
                    rx_err_sec_count                     = ?,
                    rx_err_fcs_count                     = ?,
                    network_name                         = ?,
                    extended_pan_id                      = ?,
                    diag_updated_at                      = ?
                WHERE eui64 = ?
                """,
                (
                    partition_id, leader_router_id, routing_role,
                    active_routers, channel, weighting,
                    detached_role_count,
                    router_role_count, leader_role_count,
                    attach_attempt_count, parent_change_count,
                    partition_id_change_count,
                    better_partition_attach_attempt_count,
                    tx_total_count, tx_retry_count,
                    tx_err_cca_count, tx_err_abort_count,
                    tx_err_busy_channel_count,
                    rx_total_count, rx_duplicated_count,
                    rx_err_no_frame_count, rx_err_sec_count,
                    rx_err_fcs_count,
                    network_name, extended_pan_id,
                    _utc_now(), eui64,
                ),
            )
            return cur.rowcount > 0

    def set_node_router_id(self, eui64: str, router_id: int | None) -> dict[str, Any]:
        """Persist a node's Thread Router ID + derived RLOC16.

        Used to resolve next-hop RouterId references in RouteTable entries
        back to a named node. Pass ``None`` to clear.

        v0.9.43: also derives RLOC16 (= ``router_id << 10``) and stamps it
        on the row. Returns a diff dict ``{updated, old_router_id,
        new_router_id, old_rloc16, new_rloc16}`` so callers can emit
        ``rloc16_change`` events when the router ID changes between two
        observations within the same partition (a leader-side
        reassignment, typically after a brief detach). The legacy bool
        return is preserved as ``updated``.
        """
        new_rloc16 = (router_id << 10) if isinstance(router_id, int) else None
        with self._tx() as conn:
            prior = conn.execute(
                "SELECT router_id, rloc16 FROM nodes WHERE eui64 = ?",
                (eui64,),
            ).fetchone()
            old_router_id = prior["router_id"] if prior else None
            old_rloc16 = prior["rloc16"] if prior else None
            cur = conn.execute(
                "UPDATE nodes SET router_id = ?, rloc16 = ? WHERE eui64 = ?",
                (router_id, new_rloc16, eui64),
            )
            updated = cur.rowcount > 0
        return {
            "updated": updated,
            "old_router_id": old_router_id,
            "new_router_id": router_id,
            "old_rloc16": old_rloc16,
            "new_rloc16": new_rloc16,
        }

    def insert_otbr_diagnostic(
        self,
        *,
        target_eui64: str,
        target_rloc16: int | None = None,
        partition_id: int | None = None,
        mac_tx_total: int | None = None,
        mac_tx_retry: int | None = None,
        mac_tx_err: int | None = None,
        mac_rx_total: int | None = None,
        mac_rx_err: int | None = None,
        mac_rx_dup: int | None = None,
        mle_counters: dict[str, Any] | None = None,
        child_table: list[dict[str, Any]] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> int:
        """Persist one OTBR ``MGMT_DIAG_GET`` snapshot for a target router.

        Each call is a new row (history retained); the caller is
        responsible for rate-limiting and for pruning. Returns the new
        row's id, or 0 on insert failure.
        """
        with self._tx() as conn:
            cur = conn.execute(
                """
                INSERT INTO otbr_diagnostics(
                    target_eui64, target_rloc16, observed_at, partition_id,
                    mac_tx_total, mac_tx_retry, mac_tx_err,
                    mac_rx_total, mac_rx_err, mac_rx_dup,
                    mle_counters_json, child_table_json, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    target_eui64, target_rloc16, _utc_now(), partition_id,
                    mac_tx_total, mac_tx_retry, mac_tx_err,
                    mac_rx_total, mac_rx_err, mac_rx_dup,
                    json.dumps(mle_counters) if mle_counters is not None else None,
                    json.dumps(child_table) if child_table is not None else None,
                    json.dumps(extra) if extra is not None else None,
                ),
            )
            return int(cur.lastrowid or 0)

    def get_latest_otbr_diagnostic(
        self, target_eui64: str
    ) -> dict[str, Any] | None:
        """Return the most-recent OTBR diagnostic snapshot for a target."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM otbr_diagnostics"
                " WHERE target_eui64 = ?"
                " ORDER BY observed_at DESC, id DESC LIMIT 1",
                (target_eui64,),
            ).fetchone()
        if not row:
            return None
        d = dict(row)
        for field in ("mle_counters_json", "child_table_json", "extra_json"):
            raw = d.pop(field, None)
            key = field.removesuffix("_json")
            if raw:
                try:
                    d[key] = json.loads(raw)
                except Exception:  # noqa: BLE001
                    d[key] = None
            else:
                d[key] = None
        return d

    # -- observer events (v15) ----------------------------------------

    def insert_observer_event(
        self,
        *,
        source: str,
        kind: str,
        started_at: str | None = None,
        ended_at: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> int:
        """Record one observer-side disruption window.

        ``source`` is free-form (e.g. ``addon:self``); ``kind`` is one of
        ``start``, ``stop``, ``restart``, ``outage``. ``started_at``
        defaults to ``now()``. Returns the new row id.
        """
        ts = started_at or _utc_now()
        with self._tx() as conn:
            cur = conn.execute(
                """
                INSERT INTO observer_events(
                    source, kind, started_at, ended_at, details_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    source, kind, ts, ended_at,
                    json.dumps(details) if details is not None else None,
                ),
            )
            return int(cur.lastrowid or 0)

    def close_observer_event(
        self, event_id: int, *, ended_at: str | None = None
    ) -> bool:
        """Stamp ``ended_at`` on an open event window."""
        ts = ended_at or _utc_now()
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE observer_events SET ended_at = ?"
                " WHERE id = ? AND ended_at IS NULL",
                (ts, event_id),
            )
            return cur.rowcount > 0

    def get_latest_observer_event(self, source: str) -> dict[str, Any] | None:
        """Return the most-recent observer event for ``source``."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM observer_events"
                " WHERE source = ?"
                " ORDER BY started_at DESC, id DESC LIMIT 1",
                (source,),
            ).fetchone()
        if not row:
            return None
        return self._row_to_observer_event(row)

    def list_observer_events_in_window(
        self, *, since: str, until: str | None = None
    ) -> list[dict[str, Any]]:
        """Return observer events that overlap ``[since, until]``.

        An event overlaps if its ``started_at < until`` AND
        (``ended_at IS NULL`` OR ``ended_at >= since``). Used by the
        reasoner to find blackouts that overlap an issue's trigger
        window.
        """
        upper = until or _utc_now()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM observer_events
                 WHERE started_at <= ?
                   AND (ended_at IS NULL OR ended_at >= ?)
                 ORDER BY started_at DESC, id DESC
                """,
                (upper, since),
            ).fetchall()
        return [self._row_to_observer_event(r) for r in rows]

    @staticmethod
    def _row_to_observer_event(row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        raw = d.pop("details_json", None)
        if raw:
            try:
                d["details"] = json.loads(raw)
            except Exception:  # noqa: BLE001
                d["details"] = {"_raw": raw}
        else:
            d["details"] = None
        return d

    # -- topology snapshots (v16) -------------------------------------

    def insert_topology_snapshot(
        self,
        *,
        snapshot: dict[str, Any],
        snapshot_hash: str,
        captured_at: str | None = None,
    ) -> int:
        """Insert a topology snapshot row. Returns the new row id.

        The caller computes ``snapshot_hash`` over the canonical
        normalized snapshot content so the capture stage can skip
        writing rows when nothing has changed.
        """
        ts = captured_at or _utc_now()
        # Pull denormalized summary columns from the snapshot dict so
        # ``list_topology_snapshots`` doesn't have to parse JSON.
        node_count = int(snapshot.get("node_count") or len(snapshot.get("nodes") or []))
        link_count = int(snapshot.get("link_count") or len(snapshot.get("links") or []))
        pid: int | None = None
        partitions = snapshot.get("partitions") or []
        if len(partitions) == 1:
            try:
                pid = int(partitions[0].get("partition_id"))
            except (TypeError, ValueError):
                pid = None
        with self._tx() as conn:
            cur = conn.execute(
                """
                INSERT INTO topology_snapshots(
                    captured_at, snapshot_hash, partition_id,
                    node_count, link_count, snapshot_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (ts, snapshot_hash, pid, node_count, link_count, json.dumps(snapshot)),
            )
            return int(cur.lastrowid or 0)

    def get_topology_snapshot(self, snapshot_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM topology_snapshots WHERE id = ?",
                (snapshot_id,),
            ).fetchone()
        if not row:
            return None
        return self._row_to_topology_snapshot(row)

    def get_latest_topology_snapshot(
        self, *, at: str | None = None
    ) -> dict[str, Any] | None:
        """Return the most-recent snapshot whose ``captured_at <= at``.

        If ``at`` is None, returns the newest snapshot overall.
        """
        with self._lock:
            if at:
                row = self._conn.execute(
                    "SELECT * FROM topology_snapshots"
                    " WHERE captured_at <= ?"
                    " ORDER BY captured_at DESC, id DESC LIMIT 1",
                    (at,),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT * FROM topology_snapshots"
                    " ORDER BY captured_at DESC, id DESC LIMIT 1"
                ).fetchone()
        if not row:
            return None
        return self._row_to_topology_snapshot(row)

    def list_topology_snapshots(
        self,
        *,
        since: str | None = None,
        until: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List snapshot summaries (no JSON body) for fast browsing."""
        limit = max(1, min(int(limit), 1000))
        clauses: list[str] = []
        params: list[Any] = []
        if since:
            clauses.append("captured_at >= ?")
            params.append(since)
        if until:
            clauses.append("captured_at <= ?")
            params.append(until)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            "SELECT id, captured_at, snapshot_hash, partition_id,"
            " node_count, link_count"
            f" FROM topology_snapshots{where}"
            " ORDER BY captured_at DESC, id DESC LIMIT ?"
        )
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _row_to_topology_snapshot(row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        raw = d.pop("snapshot_json", None)
        if raw:
            try:
                d["snapshot"] = json.loads(raw)
            except Exception:  # noqa: BLE001
                d["snapshot"] = {"_raw": raw}
        else:
            d["snapshot"] = None
        return d

    # -- pipeline_ticks (v18, Phase 1 temporal honesty) ---------------

    def record_pipeline_tick(self, tick: dict[str, Any]) -> int:
        """Persist a finished pipeline tick. Returns the new row id.

        ``tick`` is the dict produced by ``pipeline.runner.run_tick`` /
        ``get_runner_state``. Only the fields we care about are extracted;
        unknown keys are ignored. The full per-stage dict is JSON-encoded
        in ``stages_json`` so we can replay timings later without forcing
        a schema migration every time we add a stage.
        """
        stages = tick.get("stages") or {}
        ok_count = sum(1 for s in stages.values() if isinstance(s, dict) and s.get("ok"))
        fail_count = sum(1 for s in stages.values() if isinstance(s, dict) and not s.get("ok"))
        started_at = tick.get("started_at")
        finished_at = tick.get("finished_at")

        def _iso(v: Any) -> str:
            if isinstance(v, (int, float)):
                return datetime.fromtimestamp(v, tz=UTC).isoformat()
            if isinstance(v, str):
                return v
            return _utc_now()

        try:
            db_size_bytes = int(self.db_path.stat().st_size)
        except OSError:
            db_size_bytes = 0

        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO pipeline_ticks("
                "started_at, completed_at, duration_s, ok_count, fail_count,"
                " stages_json, error, db_size_bytes"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    _iso(started_at),
                    _iso(finished_at),
                    float(tick.get("duration_seconds") or 0.0),
                    ok_count,
                    fail_count,
                    json.dumps(stages, default=str),
                    tick.get("error"),
                    db_size_bytes,
                ),
            )
            return int(cur.lastrowid or 0)

    def get_recent_pipeline_ticks(self, limit: int = 20) -> list[dict[str, Any]]:
        """Return the most recent N pipeline ticks (newest first)."""
        limit = max(1, min(int(limit), 1000))
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, started_at, completed_at, duration_s,"
                " ok_count, fail_count, stages_json, error, db_size_bytes"
                " FROM pipeline_ticks ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            raw = d.pop("stages_json", None)
            try:
                d["stages"] = json.loads(raw) if raw else {}
            except Exception:  # noqa: BLE001
                d["stages"] = {"_raw": raw}
            out.append(d)
        return out

    # -- node counter samples (v19, Phase 4) --------------------------

    def record_counter_sample(
        self,
        *,
        eui64: str,
        counters: dict[str, Any],
        tick_id: int | None = None,
        observed_at: str | None = None,
    ) -> bool:
        """Insert a counter sample row, returning True on success.

        Drops keys whose value is None so the JSON stays compact. Existing
        rows with the same ``(eui64, observed_at)`` are kept (INSERT OR
        IGNORE) — a single tick should record exactly one sample per node.
        """
        if not eui64:
            return False
        cleaned = {k: v for k, v in counters.items() if v is not None}
        if not cleaned:
            return False
        ts = observed_at or _utc_now()
        payload = json.dumps(cleaned, separators=(",", ":"), sort_keys=True)
        with self._tx() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO node_counter_samples"
                "(eui64, observed_at, tick_id, counters_json)"
                " VALUES (?, ?, ?, ?)",
                (eui64, ts, tick_id, payload),
            )
            return bool(cur.rowcount)

    def get_counter_samples(
        self,
        *,
        eui64: str,
        since: str | None = None,
        until: str | None = None,
        limit: int = 2000,
    ) -> list[dict[str, Any]]:
        """Return raw samples for one node within [since, until], oldest first.

        ``since`` and ``until`` are ISO-8601 strings; if absent, no bound.
        """
        clauses: list[str] = ["eui64 = ?"]
        params: list[Any] = [eui64]
        if since:
            clauses.append("observed_at >= ?")
            params.append(since)
        if until:
            clauses.append("observed_at <= ?")
            params.append(until)
        params.append(max(1, min(int(limit), 100_000)))
        sql = (
            "SELECT eui64, observed_at, tick_id, counters_json"
            " FROM node_counter_samples WHERE "
            + " AND ".join(clauses)
            + " ORDER BY observed_at ASC LIMIT ?"
        )
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            raw = d.pop("counters_json", None)
            try:
                d["counters"] = json.loads(raw) if raw else {}
            except Exception:  # noqa: BLE001
                d["counters"] = {"_raw": raw}
            out.append(d)
        return out

    def count_counter_samples(self) -> int:
        """Return total row count in ``node_counter_samples``."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM node_counter_samples"
            ).fetchone()
        return int(row[0] or 0)

    def prune_counter_samples(
        self,
        *,
        full_resolution_days: int,
        sampled_archive_days: int,
    ) -> dict[str, Any]:
        """Apply Phase 4 retention to ``node_counter_samples``.

        Rows newer than ``full_resolution_days`` are kept at full resolution.
        Rows between that cutoff and ``sampled_archive_days`` are downsampled
        to 5-minute averages per (eui64, bucket). Rows older than
        ``sampled_archive_days`` are deleted.

        Returns ``{deleted, downsampled, kept}`` counts.
        """
        now = datetime.now(tz=UTC)
        full_cutoff = (now - timedelta(days=int(full_resolution_days))).isoformat()
        archive_cutoff = (now - timedelta(days=int(sampled_archive_days))).isoformat()

        deleted_old = 0
        downsampled = 0

        with self._tx() as conn:
            # 1. Drop anything beyond the archive horizon.
            cur = conn.execute(
                "DELETE FROM node_counter_samples WHERE observed_at < ?",
                (archive_cutoff,),
            )
            deleted_old = cur.rowcount or 0

            # 2. Downsample rows between archive_cutoff and full_cutoff.
            # SQLite has no nice 5-min bucket function; do it in Python.
            rows = conn.execute(
                "SELECT eui64, observed_at, counters_json"
                " FROM node_counter_samples"
                " WHERE observed_at >= ? AND observed_at < ?"
                " ORDER BY eui64, observed_at",
                (archive_cutoff, full_cutoff),
            ).fetchall()

            buckets: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for r in rows:
                ts = r["observed_at"]
                try:
                    dt = datetime.fromisoformat(ts)
                except ValueError:
                    continue
                bucket_minute = (dt.minute // 5) * 5
                bucket_dt = dt.replace(minute=bucket_minute, second=0, microsecond=0)
                key = (r["eui64"], bucket_dt.isoformat())
                try:
                    cnt = json.loads(r["counters_json"]) if r["counters_json"] else {}
                except Exception:  # noqa: BLE001
                    cnt = {}
                if isinstance(cnt, dict):
                    buckets.setdefault(key, []).append(cnt)

            for (eui, bucket_ts), samples in buckets.items():
                if len(samples) <= 1:
                    # Nothing to compress; skip.
                    continue
                # Average across samples in this bucket.
                avg: dict[str, float] = {}
                for s in samples:
                    for k, v in s.items():
                        if isinstance(v, (int, float)):
                            avg[k] = avg.get(k, 0.0) + float(v)
                if not avg:
                    continue
                n = float(len(samples))
                avg = {k: round(v / n, 3) for k, v in avg.items()}
                payload = json.dumps(avg, separators=(",", ":"), sort_keys=True)

                # Delete the originals in this bucket window.
                # Bucket spans [bucket_ts, bucket_ts + 5min).
                bucket_dt = datetime.fromisoformat(bucket_ts)
                end_ts = (bucket_dt + timedelta(minutes=5)).isoformat()
                conn.execute(
                    "DELETE FROM node_counter_samples"
                    " WHERE eui64 = ? AND observed_at >= ? AND observed_at < ?",
                    (eui, bucket_ts, end_ts),
                )
                conn.execute(
                    "INSERT OR REPLACE INTO node_counter_samples"
                    "(eui64, observed_at, tick_id, counters_json)"
                    " VALUES (?, ?, NULL, ?)",
                    (eui, bucket_ts, payload),
                )
                downsampled += len(samples)

            kept_row = conn.execute(
                "SELECT COUNT(*) FROM node_counter_samples"
            ).fetchone()

        return {
            "deleted": int(deleted_old),
            "downsampled": int(downsampled),
            "kept": int(kept_row[0] or 0),
        }

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
                * ``sleeping``     — HA-registered sleepy end device with
                    ``available = 0`` (or still-unprobed stale availability) while a
                    recent NeighborTable sighting still places it on the mesh within
                    the last five minutes, including the live shape where the sleepy
                    device reports its parent as an outgoing peer instead of being
                    claimed by an incoming ``is_child`` row.
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

        Every status transition is persisted as a ``status_change`` row in
        the ``events`` table (payload: ``{"from": old, "to": new}``) so the
        flap history can be reconstructed after the fact via
        ``query_events`` or ``get_node_flap_history``.

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
        sed_alive_cutoff = (
            datetime.now(tz=UTC) - timedelta(seconds=_SED_MESH_ALIVE_WINDOW_SECONDS)
        ).isoformat()
        # Compute the new status per node in one common-table-expression so
        # we can both (a) emit a status_change event for each transition and
        # (b) update the row, all inside a single transaction.
        calc_sql = """
            SELECT eui64, status AS old_status,
                   CASE
                       -- Primary signal: HA entity availability.
                       WHEN available = 1 THEN 'online'
                       WHEN available = 0
                        AND device_id IS NOT NULL
                        AND routing_role = 'sleepy_end_device'
                                                AND EXISTS (
                                                        SELECT 1
                                                            FROM links
                                                         WHERE links.source = 'neighbor_table'
                                                             AND links.observed_at IS NOT NULL
                                                             AND links.observed_at >= ?
                                                             AND (
                                                                     links.neighbor_eui64 = nodes.eui64
                                                                     OR links.reporter_eui64 = nodes.eui64
                                                             )
                                                )
                           THEN 'sleeping'
                       WHEN available = 0 AND device_id IS NOT NULL
                           THEN 'offline'
                       -- Availability not yet probed: fall back to
                       -- last_referenced recency for registered nodes;
                       -- otherwise treat as unregistered.
                       WHEN available IS NULL
                        AND device_id IS NOT NULL
                        AND last_referenced_at IS NOT NULL
                        AND last_referenced_at >= ?
                           THEN 'online'
                                             WHEN available IS NULL
                                                AND device_id IS NOT NULL
                                                AND routing_role = 'sleepy_end_device'
                                                AND EXISTS (
                                                        SELECT 1
                                                            FROM links
                                                         WHERE links.source = 'neighbor_table'
                                                             AND links.observed_at IS NOT NULL
                                                             AND links.observed_at >= ?
                                                             AND (
                                                                     links.neighbor_eui64 = nodes.eui64
                                                                     OR links.reporter_eui64 = nodes.eui64
                                                             )
                                                )
                                                     THEN 'sleeping'
                       WHEN device_id IS NOT NULL THEN 'offline'
                       -- No device_id: aged out or never seen.
                       WHEN last_referenced_at IS NULL THEN 'unregistered'
                       WHEN last_referenced_at < ? THEN 'phantom'
                       ELSE 'unregistered'
                   END AS new_status
              FROM nodes
        """
        with self._tx() as conn:
            transitions = [
                (row["eui64"], row["old_status"], row["new_status"])
                for row in conn.execute(
                    calc_sql, (sed_alive_cutoff, offline_cutoff, sed_alive_cutoff, phantom_cutoff)
                ).fetchall()
                if (row["old_status"] or "") != (row["new_status"] or "")
            ]
            if transitions:
                conn.executemany(
                    "INSERT INTO events(ts, eui64, type, payload_json)"
                    " VALUES (?, ?, 'status_change', ?)",
                    [
                        (
                            now,
                            eui64,
                            json.dumps({"from": old, "to": new}),
                        )
                        for eui64, old, new in transitions
                    ],
                )
            cur = conn.execute(
                f"""
                UPDATE nodes
                   SET status_changed_at = CASE
                           WHEN status <> new_status THEN ?
                           ELSE status_changed_at
                       END,
                       status     = new_status
                  FROM ({calc_sql}) AS calc
                 WHERE nodes.eui64 = calc.eui64
                """,
                (now, sed_alive_cutoff, offline_cutoff, sed_alive_cutoff, phantom_cutoff),
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
            "sleeping": counts.get("sleeping", 0),
            "offline": counts.get("offline", 0),
            "unregistered": counts.get("unregistered", 0),
            "phantom": counts.get("phantom", 0),
            "changed": changed,
            "transitions": len(transitions),
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
    ) -> dict[str, Any]:
        """Replace all links for a given (reporter, source) tuple atomically.

        Each link dict may include: neighbor_eui64 (required), rssi_avg,
        rssi_last, lqi_in, lqi_out, is_child, age_seconds, frame_error_rate,
        message_error_rate, path_cost, next_hop_router_id, rx_on_when_idle,
        full_thread_device, full_network_data, link_frame_counter,
        mle_frame_counter, link_established, allocated.

        ``partition_id`` (if known) is stamped onto every row so stale rows
        from a previous partition can be detected without re-scanning the
        whole table.

        Returns a dict with:
          ``inserted``: int       — total rows inserted this call
          ``added``: list[str]    — neighbor EUI64s that are new for this
                                    (reporter, source) since the previous
                                    snapshot
          ``removed``: list[str]  — neighbor EUI64s that were present in
                                    the previous snapshot but are absent
                                    now
          ``prior_frame_counters``: dict[str, dict[str, int | None]]
                                  — for each neighbor present in the
                                    PRIOR snapshot, the previously-stored
                                    ``{link_frame_counter, mle_frame_counter}``.
                                    Frame counters are monotonic on a
                                    given Matter session: a drop between
                                    two consecutive observations means
                                    the device re-attached (new session
                                    keys). Caller uses this to emit
                                    ``re_attached_node`` events.

        v13: the added/removed diffs are what ``link_acquired`` /
        ``link_lost`` events are derived from. Computing the diff inside
        the same transaction that swaps the rows guarantees we never miss
        a transient flap (e.g. a child that detaches and re-attaches
        within a single tick would show up as removed+added on the parent
        but the new row would still be present after the call).

        v0.9.43: ``prior_frame_counters`` was added for the
        ``re_attached_node`` rule. Snapshotting the counters inside the
        same transaction as the swap is the only way to guarantee no
        race — a second concurrent sweep on the same reporter would
        otherwise destroy the evidence before we read it.
        """
        now = _utc_now()
        inserted = 0
        with self._tx() as conn:
            # Snapshot the previous neighbour set + frame counters BEFORE
            # deleting so we can diff against the incoming set. Cheap:
            # indexed lookup on (reporter_eui64, source).
            prior_rows = conn.execute(
                "SELECT neighbor_eui64, link_frame_counter, mle_frame_counter"
                "  FROM links"
                " WHERE reporter_eui64 = ? AND source = ?",
                (reporter_eui64, source),
            ).fetchall()
            prior_neighbors: set[str] = {r[0] for r in prior_rows}
            prior_frame_counters: dict[str, dict[str, int | None]] = {
                r[0]: {
                    "link_frame_counter": r[1],
                    "mle_frame_counter": r[2],
                }
                for r in prior_rows
                if r[0]
            }
            conn.execute(
                "DELETE FROM links WHERE reporter_eui64 = ? AND source = ?",
                (reporter_eui64, source),
            )
            # Build the "known EUI" set once per call. Cheaper than a
            # subquery per row and we'll need this for every link anyway.
            known_euis = {
                r[0] for r in conn.execute("SELECT eui64 FROM nodes").fetchall()
            }
            new_neighbors: set[str] = set()
            for link in links:
                neighbor = link.get("neighbor_eui64")
                if not neighbor:
                    continue
                new_neighbors.add(neighbor)

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
            added = sorted(new_neighbors - prior_neighbors)
            removed = sorted(prior_neighbors - new_neighbors)
        return {
            "inserted": inserted,
            "added": added,
            "removed": removed,
            "prior_frame_counters": prior_frame_counters,
        }

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
                for t in (
                    "nodes",
                    "events",
                    "issues",
                    "metadata_cache",
                    "ingest_state",
                    "links",
                    "network_data",
                    "topology_snapshots",
                    "chat_session_memory",
                    "chat_turn_stats",
                )
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

        Preserves: ``schema_version`` and ``chat_session_memory``.
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

    def upsert_chat_session_memory(
        self,
        *,
        conversation_id: str,
        created_at: str,
        updated_at: str,
        expires_at: str | None,
        payload: dict[str, Any],
    ) -> None:
        payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
        with self._tx() as conn:
            conn.execute(
                """
                INSERT INTO chat_session_memory (
                    conversation_id, created_at, updated_at, expires_at, payload_json
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    expires_at = excluded.expires_at,
                    payload_json = excluded.payload_json
                """,
                (conversation_id, created_at, updated_at, expires_at, payload_json),
            )

    def get_chat_session_memory(self, conversation_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM chat_session_memory WHERE conversation_id = ?",
                (conversation_id,),
            ).fetchone()
        if row is None:
            return None
        data = dict(row)
        payload_json = data.pop("payload_json", None)
        if payload_json:
            try:
                data["payload"] = json.loads(payload_json)
            except Exception:  # noqa: BLE001
                data["payload"] = {"_raw": payload_json}
        else:
            data["payload"] = {}
        return data

    def prune_chat_session_memory(self, *, stale_before: str) -> int:
        with self._tx() as conn:
            cur = conn.execute(
                "DELETE FROM chat_session_memory WHERE (expires_at IS NULL AND updated_at < ?) OR (expires_at IS NOT NULL AND expires_at < ?)",
                (stale_before, stale_before),
            )
            return int(cur.rowcount or 0)

    def record_chat_turn_stat(
        self,
        *,
        conversation_id: str | None,
        recorded_at: str,
        backend: str,
        agent_id: str | None,
        model_name: str | None,
        status: str,
        error_kind: str | None,
        duration_ms: int,
        tool_call_count: int,
        had_page_context: bool,
        selected_node_eui64: str | None,
        active_tab: str | None,
    ) -> dict[str, Any]:
        with self._tx() as conn:
            cur = conn.execute(
                """
                INSERT INTO chat_turn_stats (
                    conversation_id, recorded_at, backend, agent_id, model_name,
                    status, error_kind, duration_ms, tool_call_count,
                    had_page_context, selected_node_eui64, active_tab
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    conversation_id,
                    recorded_at,
                    backend,
                    agent_id,
                    model_name,
                    status,
                    error_kind,
                    max(0, int(duration_ms)),
                    max(0, int(tool_call_count)),
                    1 if had_page_context else 0,
                    selected_node_eui64,
                    active_tab,
                ),
            )
            row_id = int(cur.lastrowid or 0)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM chat_turn_stats WHERE id = ?",
                (row_id,),
            ).fetchone()
        return dict(row) if row is not None else {}

    def get_chat_turn_stats(self, *, since: str | None = None) -> dict[str, Any]:
        where = ""
        params: list[Any] = []
        if since:
            where = " WHERE recorded_at >= ?"
            params.append(since)
        with self._lock:
            total_turns = int(self._conn.execute(
                f"SELECT COUNT(*) FROM chat_turn_stats{where}",
                params,
            ).fetchone()[0])
            status_rows = self._conn.execute(
                f"SELECT status, COUNT(*) AS count FROM chat_turn_stats{where} GROUP BY status",
                params,
            ).fetchall()
            backend_rows = self._conn.execute(
                f"SELECT backend, COUNT(*) AS count FROM chat_turn_stats{where} GROUP BY backend",
                params,
            ).fetchall()
            error_rows = self._conn.execute(
                f"SELECT error_kind, COUNT(*) AS count FROM chat_turn_stats{where} WHERE error_kind IS NOT NULL GROUP BY error_kind",
                params,
            ).fetchall()
            agg = self._conn.execute(
                f"SELECT AVG(duration_ms), AVG(tool_call_count), MAX(recorded_at), MIN(recorded_at), SUM(had_page_context) FROM chat_turn_stats{where}",
                params,
            ).fetchone()
            recent_rows = self._conn.execute(
                f"SELECT recorded_at, backend, agent_id, model_name, status, error_kind, duration_ms, tool_call_count, had_page_context, selected_node_eui64, active_tab FROM chat_turn_stats{where} ORDER BY recorded_at DESC LIMIT 10",
                params,
            ).fetchall()
        avg_duration_ms = float(agg[0]) if agg and agg[0] is not None else 0.0
        avg_tool_calls = float(agg[1]) if agg and agg[1] is not None else 0.0
        return {
            "since": since,
            "total_turns": total_turns,
            "avg_duration_ms": avg_duration_ms,
            "avg_tool_calls": avg_tool_calls,
            "page_context_turns": int(agg[4] or 0) if agg else 0,
            "window": {
                "oldest": agg[3] if agg else None,
                "newest": agg[2] if agg else None,
            },
            "by_status": {str(row[0]): int(row[1]) for row in status_rows if row[0] is not None},
            "by_backend": {str(row[0]): int(row[1]) for row in backend_rows if row[0] is not None},
            "by_error_kind": {str(row[0]): int(row[1]) for row in error_rows if row[0] is not None},
            "recent_turns": [dict(row) for row in recent_rows],
        }

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

    def get_node_flap_history(
        self,
        *,
        eui64: str | None = None,
        since: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        """Return ``status_change`` events plus per-EUI flap counts.

        Backed by the ``events`` table populated by
        ``recompute_node_statuses`` (since v0.9.41). Returns the most-recent
        ``limit`` transitions (newest first) and an aggregate
        ``flap_counts`` map ``{eui64: {total, by_transition}}`` covering
        the same window.
        """
        limit = max(1, min(int(limit), 5000))
        clauses = ["type = 'status_change'"]
        params: list[Any] = []
        if eui64:
            clauses.append("eui64 = ?")
            params.append(eui64)
        if since:
            clauses.append("ts >= ?")
            params.append(since)
        where = " WHERE " + " AND ".join(clauses)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM events{where}"
                f" ORDER BY ts DESC, id DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
        transitions: list[dict[str, Any]] = []
        flap_counts: dict[str, dict[str, Any]] = {}
        for r in rows:
            ev = _row_to_event(r)
            payload = ev.get("payload") or {}
            from_state = payload.get("from")
            to_state = payload.get("to")
            transitions.append(
                {
                    "id": ev.get("id"),
                    "ts": ev.get("ts"),
                    "eui64": ev.get("eui64"),
                    "from": from_state,
                    "to": to_state,
                }
            )
            bucket = flap_counts.setdefault(
                ev.get("eui64") or "",
                {"total": 0, "by_transition": {}},
            )
            bucket["total"] += 1
            key = f"{from_state}->{to_state}"
            bucket["by_transition"][key] = bucket["by_transition"].get(key, 0) + 1
        return {
            "transitions": transitions,
            "count": len(transitions),
            "flap_counts": flap_counts,
        }

    def get_link_flap_history(
        self,
        *,
        reporter_eui64: str | None = None,
        neighbor_eui64: str | None = None,
        source: str | None = None,
        since: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        """Return ``link_acquired`` / ``link_lost`` events plus pair flap counts.

        Backed by the ``events`` table populated by
        ``_persist_matter_diagnostics`` (since v0.9.42). A "pair" here is
        the unordered tuple of (reporter, neighbor); both directions of an
        edge count toward the same bucket so a flapping link surfaces
        regardless of which side reported it.

        Filters:
          ``reporter_eui64`` — events where this EUI was the reporter
          ``neighbor_eui64`` — events where this EUI was the neighbor
          ``source``        — ``"neighbor_table"`` or ``"route_table"``;
                              when omitted both are returned
        """
        limit = max(1, min(int(limit), 5000))
        clauses = ["type IN ('link_acquired', 'link_lost')"]
        params: list[Any] = []
        if reporter_eui64:
            # Match against either the eui64 column (stamped to reporter)
            # or the payload — payload-based filtering needs LIKE since we
            # don't have a JSON1 dependency yet.
            clauses.append("eui64 = ?")
            params.append(reporter_eui64)
        if since:
            clauses.append("ts >= ?")
            params.append(since)
        where = " WHERE " + " AND ".join(clauses)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM events{where}"
                f" ORDER BY ts DESC, id DESC LIMIT ?",
                (*params, limit * 4),  # over-fetch; we filter on payload below
            ).fetchall()
        transitions: list[dict[str, Any]] = []
        flap_counts: dict[str, dict[str, Any]] = {}
        for r in rows:
            ev = _row_to_event(r)
            payload = ev.get("payload") or {}
            ev_source = payload.get("source")
            ev_reporter = payload.get("reporter_eui64") or ev.get("eui64")
            ev_neighbor = payload.get("neighbor_eui64")
            if source and ev_source != source:
                continue
            if neighbor_eui64 and ev_neighbor != neighbor_eui64:
                continue
            transitions.append(
                {
                    "id": ev.get("id"),
                    "ts": ev.get("ts"),
                    "type": ev.get("type"),
                    "reporter_eui64": ev_reporter,
                    "neighbor_eui64": ev_neighbor,
                    "source": ev_source,
                    "partition_id": payload.get("partition_id"),
                }
            )
            if len(transitions) >= limit:
                break
            # Pair key is order-independent so both directions land in the
            # same bucket. A symmetric flap (parent <-> child) on a child
            # leaving and re-attaching shows up once.
            pair = "|".join(sorted([ev_reporter or "", ev_neighbor or ""]))
            bucket = flap_counts.setdefault(
                pair,
                {
                    "reporter_eui64": ev_reporter,
                    "neighbor_eui64": ev_neighbor,
                    "total": 0,
                    "acquired": 0,
                    "lost": 0,
                },
            )
            bucket["total"] += 1
            if ev.get("type") == "link_acquired":
                bucket["acquired"] += 1
            elif ev.get("type") == "link_lost":
                bucket["lost"] += 1
        return {
            "transitions": transitions,
            "count": len(transitions),
            "flap_counts": flap_counts,
        }

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

    def list_issues_in_window(
        self,
        *,
        since: str,
        until: str | None = None,
        eui64: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return issues whose lifecycle overlaps ``[since, until]``.

        An issue overlaps if it opened on/before ``until`` AND either is
        still open or closed on/after ``since``. Used by the Tier 4
        unified timeline to synthesize open/close events for a node or
        for the whole mesh.
        """
        upper = until or _utc_now()
        clauses = ["opened_at <= ?", "(closed_at IS NULL OR closed_at >= ?)"]
        params: list[Any] = [upper, since]
        if eui64:
            clauses.append("eui64 = ?")
            params.append(eui64)
        sql = (
            "SELECT * FROM issues WHERE "
            + " AND ".join(clauses)
            + " ORDER BY opened_at DESC, id DESC"
        )
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
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

    # -- assessment scheduler / findings / feedback (Phase 4) ----------

    def get_assessment_schedule(self) -> dict[str, Any] | None:
        """Return the current scheduler row, or ``None`` before first init."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM assessment_schedule WHERE id = 1"
            ).fetchone()
        return dict(row) if row else None

    def upsert_assessment_schedule(self, fields: dict[str, Any]) -> dict[str, Any]:
        """Insert or update the single scheduler row.

        ``fields`` is merged into the existing row (if any). ``updated_at``
        is always set to now. Returns the resulting row.
        """
        now = _utc_now()
        merged = {
            "state": "probation",
            "state_since": now,
            "last_assessment_at": None,
            "next_assessment_at": None,
            "consecutive_ok": 0,
            "consecutive_concern": 0,
            "current_interval_seconds": 900,
            "budget_calls_used": 0,
            "budget_window_start_at": now,
            "reason": None,
        }
        existing = self.get_assessment_schedule() or {}
        merged.update({k: v for k, v in existing.items() if k != "updated_at"})
        merged.update(fields)
        merged["updated_at"] = now
        with self._tx() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO assessment_schedule (
                    id, state, state_since, last_assessment_at, next_assessment_at,
                    consecutive_ok, consecutive_concern, current_interval_seconds,
                    budget_calls_used, budget_window_start_at, reason, updated_at
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    merged["state"],
                    merged["state_since"],
                    merged.get("last_assessment_at"),
                    merged.get("next_assessment_at"),
                    int(merged.get("consecutive_ok") or 0),
                    int(merged.get("consecutive_concern") or 0),
                    int(merged.get("current_interval_seconds") or 900),
                    int(merged.get("budget_calls_used") or 0),
                    merged["budget_window_start_at"],
                    merged.get("reason"),
                    merged["updated_at"],
                ),
            )
        out = self.get_assessment_schedule()
        assert out is not None
        return out

    def upsert_assessment_finding(
        self,
        *,
        finding_id: str,
        finding_key: str,
        verdict: str,
        severity: str,
        confidence: float,
        headline: str,
        evidence: list[dict[str, Any]] | None = None,
        suggested_starter_prompt: str | None = None,
        node_eui64: str | None = None,
        finding_type: str | None = None,
    ) -> dict[str, Any]:
        """Insert a new finding or bump an existing open row with the same key.

        Dedup rule: if a row with ``finding_key`` exists and is ``state='open'``,
        bump ``last_seen_at`` + ``seen_count`` and take the max confidence.
        Otherwise insert a new row.
        """
        now = _utc_now()
        ev_json = json.dumps(evidence or [])
        with self._tx() as conn:
            existing = conn.execute(
                "SELECT * FROM assessment_findings"
                " WHERE finding_key = ? AND state = 'open'"
                " ORDER BY created_at DESC LIMIT 1",
                (finding_key,),
            ).fetchone()
            if existing:
                new_conf = max(float(existing["confidence"] or 0.0), confidence)
                conn.execute(
                    "UPDATE assessment_findings"
                    "   SET last_seen_at = ?,"
                    "       seen_count = seen_count + 1,"
                    "       confidence = ?,"
                    "       severity = ?,"
                    "       headline = ?,"
                    "       evidence_json = ?,"
                    "       suggested_starter_prompt = COALESCE(?, suggested_starter_prompt)"
                    " WHERE finding_id = ?",
                    (
                        now,
                        new_conf,
                        severity,
                        headline,
                        ev_json,
                        suggested_starter_prompt,
                        existing["finding_id"],
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM assessment_findings WHERE finding_id = ?",
                    (existing["finding_id"],),
                ).fetchone()
            else:
                conn.execute(
                    """
                    INSERT INTO assessment_findings (
                        finding_id, finding_key, state, verdict, severity,
                        confidence, finding_type, headline, evidence_json,
                        suggested_starter_prompt, node_eui64,
                        created_at, last_seen_at, seen_count
                    ) VALUES (?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        finding_id,
                        finding_key,
                        verdict,
                        severity,
                        confidence,
                        finding_type,
                        headline,
                        ev_json,
                        suggested_starter_prompt,
                        node_eui64,
                        now,
                        now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM assessment_findings WHERE finding_id = ?",
                    (finding_id,),
                ).fetchone()
        return _row_to_finding(row)

    def list_assessment_findings(
        self,
        *,
        state: str | None = "open",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List findings filtered by ``state`` (default: only open)."""
        clauses: list[str] = []
        params: list[Any] = []
        if state:
            clauses.append("state = ?")
            params.append(state)
        sql = "SELECT * FROM assessment_findings"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY last_seen_at DESC LIMIT ?"
        params.append(int(limit))
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_finding(r) for r in rows]

    def get_assessment_finding(self, finding_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM assessment_findings WHERE finding_id = ?",
                (finding_id,),
            ).fetchone()
        return _row_to_finding(row) if row else None

    def clear_assessment_findings_by_key(
        self,
        finding_key: str,
        *,
        cleared_by: str = "assessment",
    ) -> int:
        """Mark all open rows with this key as cleared. Returns count affected."""
        now = _utc_now()
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE assessment_findings"
                "   SET state = 'cleared', cleared_at = ?, cleared_by = ?"
                " WHERE finding_key = ? AND state = 'open'",
                (now, cleared_by, finding_key),
            )
            return cur.rowcount or 0

    def dismiss_assessment_finding(
        self,
        finding_id: str,
        *,
        suppress_seconds: int = 86400,
    ) -> dict[str, Any] | None:
        """User dismissed; suppress same finding_key for the window."""
        now_dt = datetime.now(tz=UTC)
        suppress_until = (now_dt + timedelta(seconds=suppress_seconds)).isoformat()
        with self._tx() as conn:
            conn.execute(
                "UPDATE assessment_findings"
                "   SET state = 'dismissed', cleared_at = ?, cleared_by = 'user_dismiss',"
                "       suppress_until = ?"
                " WHERE finding_id = ?",
                (now_dt.isoformat(), suppress_until, finding_id),
            )
        return self.get_assessment_finding(finding_id)

    def record_assessment_run(
        self,
        *,
        verdict: str,
        severity: str,
        confidence: float,
        headline: str,
        finding_key: str | None = None,
        finding_id: str | None = None,
        finding_type: str | None = None,
        node_eui64: str | None = None,
        parse_attempts: int = 0,
        duration_seconds: float = 0.0,
        suppressed: bool = False,
        dedup_hit: bool = False,
        cleared_count: int = 0,
        model_name: str | None = None,
    ) -> dict[str, Any]:
        assessed_at = _utc_now()
        with self._tx() as conn:
            cur = conn.execute(
                """
                INSERT INTO assessment_runs (
                    assessed_at, verdict, severity, confidence, headline,
                    finding_key, finding_id, finding_type, node_eui64,
                    parse_attempts, duration_seconds, suppressed, dedup_hit,
                    cleared_count, model_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    assessed_at,
                    verdict,
                    severity,
                    confidence,
                    headline,
                    finding_key,
                    finding_id,
                    finding_type,
                    node_eui64,
                    int(parse_attempts),
                    float(duration_seconds),
                    1 if suppressed else 0,
                    1 if dedup_hit else 0,
                    int(cleared_count),
                    model_name,
                ),
            )
            row = conn.execute(
                "SELECT * FROM assessment_runs WHERE id = ?",
                (cur.lastrowid,),
            ).fetchone()
        return _row_to_assessment_run(row)

    def list_assessment_runs(
        self,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM assessment_runs ORDER BY assessed_at DESC LIMIT ? OFFSET ?",
                (int(limit), int(offset)),
            ).fetchall()
        return [_row_to_assessment_run(r) for r in rows]

    def is_finding_key_suppressed(self, finding_key: str, *, at: str | None = None) -> bool:
        """Return True if any dismissed row with the same key has suppress_until > now."""
        ts = at or _utc_now()
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM assessment_findings"
                " WHERE finding_key = ? AND state = 'dismissed'"
                "   AND suppress_until IS NOT NULL AND suppress_until > ?"
                " LIMIT 1",
                (finding_key, ts),
            ).fetchone()
        return row is not None

    def record_assessment_feedback(
        self,
        *,
        finding_id: str,
        outcome: str,
        finding_type: str | None = None,
        notes: str | None = None,
    ) -> dict[str, Any]:
        now = _utc_now()
        with self._tx() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO assessment_feedback (
                    finding_id, outcome, outcome_at, finding_type, notes
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (finding_id, outcome, now, finding_type, notes),
            )
        return {
            "finding_id": finding_id,
            "outcome": outcome,
            "outcome_at": now,
            "finding_type": finding_type,
            "notes": notes,
        }

    def assessment_feedback_summary(self, *, since: str | None = None) -> dict[str, Any]:
        """Aggregate feedback outcomes since ``since`` (ISO-8601, default 7 days)."""
        if not since:
            since = (datetime.now(tz=UTC) - timedelta(days=7)).isoformat()
        with self._lock:
            rows = self._conn.execute(
                "SELECT outcome, finding_type FROM assessment_feedback"
                " WHERE outcome_at >= ?",
                (since,),
            ).fetchall()
        by_outcome: dict[str, int] = {}
        per_type: dict[str, dict[str, int]] = {}
        for r in rows:
            by_outcome[r["outcome"]] = by_outcome.get(r["outcome"], 0) + 1
            ftype = r["finding_type"] or "unknown"
            per_type.setdefault(ftype, {"total": 0, "wrong": 0, "resolved": 0})
            per_type[ftype]["total"] += 1
            if r["outcome"] == "wrong":
                per_type[ftype]["wrong"] += 1
            elif r["outcome"] == "resolved":
                per_type[ftype]["resolved"] += 1
        total = len(rows)
        good = by_outcome.get("resolved", 0) + by_outcome.get("ignored_expired", 0)
        precision = (good / total) if total else None
        noisy: list[dict[str, Any]] = []
        for ftype, stats in per_type.items():
            if stats["total"] >= 3 and stats["wrong"] / stats["total"] > 0.25:
                noisy.append(
                    {
                        "finding_type": ftype,
                        "wrong_rate": round(stats["wrong"] / stats["total"], 3),
                        "n": stats["total"],
                    }
                )
        return {
            "since": since,
            "total_findings": total,
            "by_outcome": by_outcome,
            "precision_estimate": precision,
            "noisy_signal_types": noisy,
        }


def _row_to_finding(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    d = dict(row)
    evj = d.pop("evidence_json", None)
    if evj:
        try:
            d["evidence"] = json.loads(evj)
        except Exception:  # noqa: BLE001
            d["evidence"] = {"_raw": evj}
    else:
        d["evidence"] = []
    return d


def _row_to_assessment_run(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    d = dict(row)
    d["suppressed"] = bool(d.get("suppressed"))
    d["dedup_hit"] = bool(d.get("dedup_hit"))
    return d


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

