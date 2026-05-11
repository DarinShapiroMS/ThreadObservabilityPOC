# Changelog

## 0.9.1 — dev-loop fix: JSON body for Supervisor POST endpoints

- Fixed `_post()` in `supervisor_client.py` to send an empty JSON body `{}` by default. Supervisor's REST API (e.g., `/addons/self/rebuild`, `/store/addons/{slug}/update`) requires a JSON body even for simple POSTs; prior implementation was sending headers-only, causing 400/403 errors.
- This unblocks `ha_update_addon`, `ha_rebuild_addon`, and `ha_restart_addon` MCP tools, enabling fully automated dev loop via MCP without manual HA UI clicks.

## 0.9.0 — Phase 3: Node-friendly names and device discovery

- **Node metadata enrichment** (`pipeline/nodes.py`): compute node status (healthy/stale/offline) based on event recency, extract latest RSSI/LQI signal strength, display human-readable names alongside EUI64 hex.
- **SQLite helpers** extended: `list_nodes()`, `get_node_by_friendly_name()`, `set_node_friendly_name()` for local name management.
- **HA device registry lookup** (`supervisor_client.get_ha_device_registry()`): best-effort fetch from Home Assistant's device registry for future auto-mapping.
- **New MCP tools** (28 total, +3): `get_node_metadata`, `set_node_friendly_name`, `list_all_nodes` (all include RSSI/LQI samples and status inference).
- **New REST endpoints** (6 new, /v1/nodes/*): `GET /v1/nodes/all`, `GET /v1/nodes/{eui64}`, `POST /v1/nodes/{eui64}/friendly-name`.
- **Dashboard "Thread Nodes" card**: sortable table showing node #ID (last 4 hex digits), friendly name, role, RSSI/LQI, status badge, last-seen timestamp. Populated from `/v1/dev/status` enriched nodes.
- **Status inference**: automatic `healthy` (recent events) / `stale` (30–60 min old) / `offline` (>60 min) based on configurable freshness window.

## 0.8.0 — Phase 2.5: real OTBR log ingestion

- **OTBR log adapter** (`pipeline/otbr_adapter.py`): polls the Supervisor `/addons/{slug}/logs` endpoint, parses recognised lines into canonical Thread events, and persists them to SQLite with a resume cursor (hash of last-seen line + count) stored in `ingest_state`. Errors are non-fatal and surfaced in `/v1/ingest/state`.
- **OTBR/openthread line parser** (`pipeline/otbr_parser.py`): tolerant regex parser for `attach` / `attach_failed` / `attach_attempt` / `detach` / `parent_response` / `role_change` / `child_added` / `child_removed` / `node_seen`, plus RSSI/LQI/parent extraction.
- **Auto-discovery**: `list_otbr_candidates` enumerates Supervisor add-ons matching `openthread|otbr|silabs-multiprotocol`; `set_otbr_slug` persists the operator choice and resets the cursor.
- **Background scheduler**: FastAPI lifespan now starts an asyncio task that calls `ingest_once` every `scheduler.ingestion_interval_seconds` (default 10s).
- **New REST endpoints** on the core service: `GET /v1/ingest/state`, `GET /v1/ingest/candidates`, `POST /v1/ingest/run`, `POST /v1/ingest/slug`.
- **New MCP tools** (25 total now): `list_otbr_candidates`, `set_otbr_slug`, `ingest_now`, `get_ingest_state`.
- **Dashboard**: new “OTBR Ingestion” card showing slug, lines processed, events total, last event/run timestamps, last error; plus an “Ingest now” / “List OTBR add-ons” pair of buttons.

## 0.7.1 — dev-loop: enable admin role

- Bumped `hassio_role` from `manager` to `admin` so `ha_update_addon` can call `POST /store/addons/{slug}/update` (Supervisor returns 403 for `manager`). This unblocks fully-automated MCP deploys.
  - **One-time UI step required**: click Update once in the HA add-on UI to install 0.7.1 with the new role. After that, future versions deploy via `ha_update_addon` end-to-end.

## 0.7.0 — Phase 2 (part 1): topology engine + deterministic reasoner

- **Topology graph engine** (`pipeline/topology.py`): builds node/link snapshot from the SQLite event log; infers current parent edges from the latest `attach` / `parent_change` event per node within a configurable freshness window; surfaces last RSSI/LQI per node.
- **Deterministic reasoner** (`pipeline/reasoner.py`) with three v1 rules:
  - `parent_churn` (warn) — ≥3 `parent_change` events in 30 min
  - `attach_failures` (warn) — ≥2 `attach_failed` events in 15 min
  - `offline_node` (crit) — no events for ≥30 min since first seen
  - Auto-closes managed issues whose triggering condition no longer holds.
- **Issues table API**: `open_issue` (deduped on `kind`+`eui64`), `close_issue`, `list_active_issues`.
- **Health snapshot** (`health.py`): consolidated view (healthy/stale/offline counts, active issue counts by severity, data age, overall status).
- **Real endpoints**: `/v1/topology`, `/v1/issues/active`, `/v1/health/snapshot` now return live data. New `POST /v1/reasoner/run` and `POST /v1/dev/seed`.
- **MCP tools wired to real data**: `get_network_topology` (with `freshness_minutes`), `list_active_issues`, `get_health_snapshot`. New: `run_reasoner`, `close_issue`, `seed_demo_topology` (21 tools total).
- **Dashboard**: Thread Network card shows real node/link counts, healthy/stale/offline split, overall status pill, data age; new **Active Issues** card lists open issues with severity pills; new **Dev Actions** panel with Seed demo / Run reasoner / Refresh buttons.
- **Test suite**: 17 pytest cases covering migrations, event insertion/query, issue dedup/close, topology builder (empty, basic links, parent_change wins, stale-window cutoff), reasoner rules (no-op, churn, attach-fail open/close, offline detection), and health classification. Run with `pip install -e .[test] && pytest tests`.

## 0.6.1

- `ha_update_addon` now resolves the addon slug from `/addons/self/info` and calls `POST /store/addons/{slug}/update`. The `/addons/self/update` alias returns 404 on current Supervisor versions; this restores full dev-loop automation.

## 0.6.0 — Phase 1: storage + config foundation

- **SQLite store** (`storage/sqlite_store.py`) with migration-versioned schema:
  - `nodes`, `events`, `issues`, `metadata_cache`, `ingest_state`, `schema_version`
  - WAL mode, NORMAL sync, indexed lookups on `events(eui64, ts)` / `events(type, ts)` / `issues(closed_at, severity)`
  - DB at `/data/thread-observability/state.db`; process-wide singleton via `get_store()`
- **Time-series backend** (`storage/influx_store.py`) with automatic fallback:
  - `InfluxDBStore` writes line-protocol to InfluxDB v2 (Flux queries supported)
  - `SQLiteFallbackStore` persists numeric samples into the main SQLite DB when Influx isn’t configured / reachable
  - `get_timeseries_store()` selects automatically based on `INFLUX_TOKEN` / health probe
- **Typed config loader** (`config.py`) reading `/data/options.json` with Pydantic models:
  - `ThreadObsConfig` with `retention`, `ai`, `scheduler`, `influx` sub-models
  - Process-wide cached via `get_config()`; `reload_config()` clears the cache
- **New MCP tools:**
  - `get_storage_stats` — SQLite stats + active time-series backend
  - `query_events` — filter by `eui64`, `event_type`, `since`, `limit`
  - `insert_test_event` — dev seed for verifying end-to-end
  - `get_config` — typed config payload (influx token redacted)
  - `get_timeseries_health` — probe Influx / fallback
- **Dashboard:** new “Storage” card showing schema version, DB size, row counts, active TS backend, newest event; `/v1/dev/status` now includes `storage`, `timeseries`, `config`

## 0.5.0

- Added Supervisor-backed update lifecycle MCP tools so VS Code can drive deploys end-to-end:
  - `ha_check_for_update` — reloads the store and reports `{current, latest, update_available, auto_update, state}`. Skips Supervisor's periodic-poll wait.
  - `ha_update_addon` — equivalent to clicking "Update" in the HA UI.
  - `ha_set_auto_update` — toggle Supervisor's auto-update flag (`{enabled: bool}`).
  - `ha_reinstall_addon` — uninstall + reinstall by slug (destructive; terminates the calling process mid-flight, so a connection reset is the expected success signal).
- Underlying `supervisor_client` gains `reload_store`, `check_for_update`, `update_addon`, `set_auto_update`, `reinstall_addon`.

## 0.4.0

- Replaced the placeholder JSON root page with a live status dashboard at `/` (the Ingress entry-point)
- Dashboard auto-refreshes every 5 s and shows:
  - Supervisor's view of the add-on (state, version, latest, update flag, boot, watchdog, ingress)
  - Service health for core (this page) and MCP (probed via `127.0.0.1:8100/health`)
  - Thread network counters (nodes/links/issues/data age) — scaffold values until ingestion lands
  - Tail of the rotating add-on log (`/data/thread-observability/addon.log`)
  - Quick links to JSON endpoints
- New aggregator endpoints:
  - `GET /v1/dev/status` — single JSON payload powering the dashboard
  - `GET /v1/dev/mcp-health` — in-container probe of the MCP service
- Existing JSON `{service: core, ...}` response moved from `/` to `/api`

## 0.3.2

- Switched s6-rc.d `run` script shebangs to `#!/command/with-contenv bash` so container env vars (notably `SUPERVISOR_TOKEN`) are inherited by the supervised processes
- Fixes Supervisor-backed MCP tools (`ha_get_addon_state`, `ha_get_addon_logs`, etc.) that previously returned "SUPERVISOR_TOKEN not set"

## 0.3.1

- Publish ports 8099 (core API) and 8100 (MCP) to the HA host so VS Code's MCP client can reach them from the LAN
- Previously the ports were declared as `null` (internal only), making `http://<ha-host>:8100/mcp` unreachable from outside the container

## 0.3.0

- Added Supervisor-backed MCP tools to close the VS Code dev loop:
  - `ha_get_addon_state` — install state, current/latest version, ingress URL, raw info
  - `ha_get_addon_logs` — tail the Supervisor container log (captures s6/startup output)
  - `ha_get_supervisor_logs` — tail the Supervisor's own log (permissions, port conflicts, etc.)
  - `ha_restart_addon` — fast restart without rebuild
  - `ha_rebuild_addon` — rebuild from repo source then restart (post-push deploy)
- New `supervisor_client.py` thin async wrapper around `http://supervisor` with bearer auth via `SUPERVISOR_TOKEN`
- Made MCP tool dispatch async; JSON-RPC `tools/call` now returns JSON-serialised content
- Added `httpx` dependency

## 0.2.0

- Switched base image from `ghcr.io/home-assistant/{arch}-base:3.20` to `ghcr.io/hassio-addons/base:20.1.1`
- Fixes persistent `s6-overlay-suexec: fatal: can only run as pid 1` crash loop
- Root cause: the low-level HA base image ships a buggy legacy-services compatibility shim; the community addon base (used by ~200 official community addons) is purpose-built for s6-rc.d native services
- Removed `legacy-services` bundle override and empty `services.d/` placeholder (no longer needed)
- Removed `rm -rf /etc/cont-init.d` workaround from Dockerfile
- Dropped `bash` from apk install (provided by base image)

## 0.1.9

- Override base image's buggy legacy-services bundle with a noop s6-rc.d bundle (empty contents.d)
- Prevents HA's s6-overlay from invoking suexec on legacy-services, eliminating the PID 1 crash
- Allows native s6-rc.d core and mcp services to run cleanly without cascade restarts

## 0.1.8

- Keep empty /etc/services.d directory (only delete cont-init.d) so HA legacy-services shim finds it, scans, finds nothing, and exits cleanly
- Prevents suexec fatal crash that cascades into service restarts
- Allows s6-rc.d native services to run uninterrupted after legacy shim completes

## 0.1.7

- Added explicit `rm -rf /etc/cont-init.d /etc/services.d` in Dockerfile to eliminate Docker layer cache issues
- Forces removal of legacy HA s6-overlay compatibility layer directories that cause cascade crashes

## 0.1.6

- Added rotating file logger to /data/thread-observability/addon.log (2 MB, 2 backups)
- Both core and MCP services now log to stdout + file on startup
- get_recent_logs MCP tool now has live data to read
- Log level controlled via THREAD_OBS_LOG_LEVEL env var (default: info)

## 0.1.5

- Implemented MCP JSON-RPC 2.0 protocol endpoint at POST /mcp (VS Code MCP client compatible)
- Added get_recent_logs tool for live log access from IDE
- Added .vscode/mcp.json wired to HA instance at 192.168.68.90:8100
- Reads from /data/thread-observability/addon.log with /run/uncaught-logs/current fallback

## 0.1.4

- Removed cont-init.d entirely to eliminate legacy-cont-init and legacy-services shims
- Moved runtime directory creation to Dockerfile RUN step
- Both legacy s6-overlay shims now have nothing to process, eliminating suexec PID 1 crash

## 0.1.3

- Migrated from legacy services.d to native s6-overlay v3 s6-rc.d service format
- Eliminates s6-overlay-suexec PID 1 fatal crash on service startup

## 0.1.2

- Fixed s6-overlay v3 compatibility by replacing with-contenv shebang with plain bash in all service scripts

## 0.1.1

- Fixed container startup by ensuring s6 scripts are LF-normalized and executable
- Fixed CI build behavior for Home Assistant base image pip install restrictions
- Removed deprecated architecture and cleaned add-on metadata defaults for linting

## 0.1.0

- Initial scaffold for Home Assistant add-on structure
- Added two-process skeleton (core + MCP)
- Added configuration schema and build metadata
