# Changelog

## 0.9.24 — LQI is Matter's 0–3 LinkQuality, color-coded

- **Corrected LQI interpretation.** Matter's Thread Network Diagnostics cluster reports `LinkQuality` as a 4-bucket 0..3 value (the spec quantizes OpenThread's raw 0–255 LQI down to this band), not the 0–255 scale itself. Legend, column tooltip and color coding now reflect that: 3 green, 2 yellow, 1 red, 0 red.

## 0.9.23 — Role column shows partition / peers / parent for every node, LQI legend

- **Role column is now meaningful for routers too.** Thread routers are peers in a mesh, not children of a parent — so the dashboard now shows partition id, peer count and (for non-leaders) the partition leader instead of leaving the caption blank. Sleepy/end devices keep the `via <router>` caption.
- **`list_nodes_enriched` enriched** with `partition_leader_eui64`, `partition_leader_name`, `router_peer_count`.
- **LQI explainer** added under the Thread Nodes table: Link Quality Indicator, 0–255 (higher = better); ≥ 200 excellent, 100–200 workable, < 100 degraded. Column headers also carry tooltips.

## 0.9.22 — Tabbed dashboard, RSSI color coding, role/parent enrichment, network graph

- **Tabbed dashboard** (`Network` / `Graph` / `Diagnostics`). The Network tab focuses on Thread node data + partitions + active issues; Diagnostics gathers supervisor, storage, timeseries, OTBR ingestion, recent logs, raw config; Graph hosts the topology visualization
- **RSSI color coding**: green ≥ −70 dBm (solid), yellow −70 to −85 dBm (marginal), red < −85 dBm (poor). Legend rendered under the table
- **Role / parent enrichment**: `list_nodes_enriched` now exposes `routing_role` (raw Matter RoutingRole), `device_kind` (`router` / `reed` / `fed` / `sed` / `unknown`), `parent_eui64` and `parent_name`. Sleepy / end devices show a `via <router>` caption under the role badge so the user can see which router a SED last attached to
- **Network graph** (Cytoscape.js via CDN) — nodes colored by role (leader red, router blue, REED teal, FED grey, SED purple, phantom slate), edges colored by RSSI bin matching the table. `show phantoms` toggle and `Re-layout` button
- **No user-initiated actions in UI** — discovery, reasoner and OTBR ingestion all run on a background scheduler now. The lifespan task spawns `matter-discovery-loop` (every `discover_interval_seconds`, default 300) and `reasoner-loop` (every `reasoner_interval_seconds`, default 120) alongside the existing OTBR ingest loop. The corresponding POST endpoints remain for debugging via MCP / curl
- Dashboard HTML moved out of the Python file into `api/dashboard.html` (loaded once at import). Shipped through `setuptools.package-data`

## 0.9.21 — Dashboard UI for phantoms + partitions, RSSI from links, drop seed/demo

- **RSSI/LQI now populates** in the Thread Nodes table. `get_latest_signal_strength` now reads from the Matter cluster-53 `links` table (per-router NeighborTable `rssi_avg` / `lqi_in`), picking the strongest incoming edge, with event-log fallback
- **Phantom Nodes card** in the dashboard lists every phantom with friendly_name, area, last_referenced_at, last_seen, and an "Open in HA" deep-link (`/config/devices/device/<id>`) for manual deletion
- **Partitions card** shows partition_count, split flag (red `SPLIT` pill when split), leader EUIs, and member counts. Honors phantom filtering
- **Show phantoms** toggle on the Thread Nodes table — when enabled, phantom rows render dimmed with a `phantom` pill
- **Discover Matter devices** button in the Actions card (was MCP-only before); calls the new `POST /v1/discover/run` route
- New HTTP routes: `/v1/partitions`, `/v1/phantoms`, `/v1/discover/run`. `/v1/topology` and `/v1/dev/status` now accept `?include_phantoms=true`
- `ADDON_VERSION` is now read from `config.yaml` at startup so it can never drift (was hardcoded to `0.9.5` since v0.9.5 — wrong for 15 releases)
- **Removed seed/demo topology** entirely (`pipeline/seed.py`, MCP tool `seed_demo_topology`, route `/v1/dev/seed`, dashboard button). Real data flow now works; the seed was no longer providing value
- Tests: 67 passing (unchanged)

## 0.9.20 — Phantom-node detection (stale-reference filtering)

- **Schema v4** adds `nodes.last_referenced_at` and `nodes.is_phantom` plus supporting indexes
- Every `discover_and_sync` cycle now bumps `last_referenced_at` for every EUI seen as a reporter **or** as an entry in any router's NeighborTable / RouteTable (single source of truth: the Thread fabric itself)
- After bumping, a sweep flips `is_phantom=1` on rows whose `last_referenced_at` is older than `PHANTOM_THRESHOLD_HOURS` (env var, default `24`); rows that come back fresh are automatically cleared
- `build_topology` and `get_partition_state` hide phantoms by default; pass `include_phantoms=true` to see them. Phantom-only partitions no longer trigger the `partition_split` issue (fixes false-positive splits caused by re-commissioned EUIs and dead devices still in the HA registry)
- New MCP tool **`list_phantom_nodes`** returns `eui64`, `friendly_name`, `device_id`, `area`, `last_seen`, `last_referenced_at`, and a `ha_device_path` deep-link so the user can manually remove the device via *Settings → Devices & Services → Devices*
- Storage layer also gains `purge_phantom_nodes()` (not exposed via MCP in this release — manual cleanup only)
- Tests: +4 new (`bump_last_referenced`, `sweep_phantoms`, `purge_phantom_nodes`, topology phantom filtering); 67 total passing

## 0.9.19 — Thread topology from Matter cluster 53 + partition split detection

- New `links` table (schema v2) persists per-reporter neighbor/route adjacencies
- Schema v3 adds per-node diagnostic columns: `partition_id`, `leader_router_id`, `routing_role`, `active_routers`, `channel`, `weighting`, `diag_updated_at`
- Matter bridge now decodes `0/53/7` NeighborTable and `0/53/8` RouteTable struct lists per Matter spec field IDs, plus partition scalars (`0/53/0,1,9,10,13`)
- `discover_and_sync` persists neighbor + route links and Thread scalars per node, emits `partition_change` events on transitions, and opens/closes a `partition_split` issue automatically when multiple distinct `partition_id`s appear
- `build_topology` now sources real mesh edges from the links table, tags `weak_link` (RSSI < -85 dBm), `high_error` (FER/MER > 10%), and `asymmetric` (|A→B − B→A| > 10 dB), infers `parent_eui64` from `is_child=1` neighbor entries, and emits `partitions[]` + `split` bool
- New MCP tool `get_partition_state` returns current partitions, leader EUIs, split flag, and recent `partition_change` events
- Tests: +13 new (decoders, links table CRUD, diagnostics scalars, partition split/healthy/asymmetry); 63 total passing

## 0.9.18 — Un-flip U/L bit when deriving EUI64 from IPv6 IID

- OTBR log parser converted Mesh-Local IPv6 IIDs (e.g. `c6b7:...`) directly to EUI64 strings, but per RFC 4291 the modified-EUI64 IID has bit 6 of byte 0 flipped
- Now we XOR byte 0 with 0x02 to recover the real EUI64, so events join up with Matter-bridged node rows (`c4b7...` not `c6b7...`)
- Fixes empty Role / RSSI / Status columns on the Thread Nodes table

## 0.9.17 — Remove one-shot truncation

- Reverted the 0.9.16 startup wipe now that the DB has been cleaned

## 0.9.16 — ONE-SHOT: truncate data tables on startup

- Lifespan startup hook deletes all rows from `events`, `issues`, `metadata_cache`, `ingest_state`, `nodes` and runs `VACUUM`
- Removes ghost rows like `000000000000d000` accumulated by pre-0.9.14 ingestion
- **Revert this block in 0.9.17 once the DB is clean**

## 0.9.15 — Upsert bridged Matter devices into nodes table

- `discover_and_sync` now inserts a row for every Matter-bridged Thread device, not only updates existing ones
- Result: all Matter-commissioned Thread devices show up in the Thread Nodes table immediately, with friendly names, even if OTBR logs haven't observed them yet
- Response now also includes `inserted` count alongside `updated`

## 0.9.14 — Stamp EUI64 + preserve registry metadata in merged map

- `fetch_device_registry` now writes `extendedAddress` onto every value in the merged dict so `_extract_thread_devices` can key on it
- `_extract_thread_devices` preserves `device_id`, `name`, `name_by_user`, etc. when it sees `extendedAddress`, instead of stripping them
- This is the final link to actually surface friendly names in `list_all_nodes` and the UI

## 0.9.13 — Canonical Matter node_id (hex/decimal normalization)

- HA registry stores Matter node_ids as 16-char zero-padded hex strings (e.g. `0000000000000001`); matter-server returns them as decimal integers (e.g. `1`). Reduce both sides to `str(int)` so they match as dict keys
- Expect: 16 EUI64 mappings now actually merge into the discovery map

## 0.9.12 — Diagnose node_id key mismatch between registry and matter-server

- 0.9.11 successfully extracted 16/17 EUI64s from matter-server but merged 0 into the discovery map
- Add INFO logs showing the first 10 node_id keys on both sides (registry parse vs matter-server) so we can see the actual format mismatch

## 0.9.11 — Use Matter spec field IDs for NetworkInterfaces parsing

- python-matter-server keys struct fields by Matter attribute ID strings (e.g. "4" for HardwareAddress, "7" for InterfaceType). Replace name-based lookup with the spec-compliant integer keys; filter to Thread interfaces only (Type==4 or Name contains thread/ieee802154)
- Confirmed live: NetworkInterfaces[0]="ieee802154", [4]=base64 8-byte HardwareAddress

## 0.9.10 — Thread Network Diagnostics ExtAddress as primary EUI64 source

- Prefer Matter Thread Network Diagnostics ExtAddress attribute (cluster 0x35 / attribute 0x0F) as primary EUI64 source
- Scan all endpoints (not just endpoint 0); some Matter Thread devices expose Thread diagnostics on non-root endpoints
- Keep General Diagnostics NetworkInterfaces as fallback, dump one sample's payload to help confirm schema

## 0.9.9 — Matter WS bridge diagnostics

- Log Matter WS connect status, server_info banner, node count, and sample node schema at INFO level
- Loop until response with matching `message_id` to skip subscription events
- Surface error_code responses instead of silently returning empty

## 0.9.8 — Discovery diagnostics at INFO level

- Add INFO-level log lines at OTBR fetch, registry parse, Matter WS bridge, and merge stages so default `log_level: info` reveals which path is empty when `discover_thread_devices` returns matched=0

## 0.9.7 — Thread-only matching + Matter node_id WS bridge

- Restrict device registry matching to Thread connection types (`thread`, `ieee802154`); no longer match zigbee
- Capture Matter-identifier devices from HA registry and bridge `node_id` → Thread EUI64 via matter-server WebSocket API (`get_nodes`, General Diagnostics `NetworkInterfaces.HardwareAddress`)
- Degrades gracefully when matter-server is absent or unreachable
- Adds `websockets>=12.0` dependency

## 0.9.6 — OTBR-based device discovery with HA device registry merge

- Rewrote device discovery to query OTBR `/api/topology` endpoint as authoritative source for Thread nodes
- Merge OTBR topology (node role, rloc) with HA device registry (friendly names, device IDs, metadata)
- Fallback to reading `.storage/core.device_registry` JSON file if OTBR API unavailable
- Solves root issue: Thread device data not exposed via HA REST API; sourced from OTBR addon directly
- Enables automatic node labeling with friendly names for better troubleshooting

## 0.9.3 — update path compatibility fallback

- `ha_update_addon` now attempts both Supervisor update endpoints in order:
  - `/addons/self/update`
  - `/store/addons/{slug}/update`
- Added fallback logic for path/permission differences across Supervisor versions (401/403/404/405 on one path now retries the other).
- Preserved existing no-update behavior (`performed: false`, `reason: no_update_available`) and expected self-update disconnect handling (`status: accepted`).
- Added regression test coverage for endpoint fallback behavior in `tests/test_supervisor_client.py`.

## 0.9.2 — dev-loop hardening for update/rebuild/restart

- Hardened lifecycle MCP operations in `supervisor_client.py`:
  - `ha_update_addon` now returns a clean no-op success when no update is available (`performed: false`, `reason: no_update_available`) instead of surfacing a raw 403.
  - `ha_update_addon` handles transport disconnects during self-update as accepted dispatch (`status: accepted`) since the add-on may restart before the HTTP response completes.
  - `ha_rebuild_addon` and `ha_restart_addon` now treat expected self-disruptive disconnects as accepted dispatch instead of hard failures.
  - `ha_update_addon` keeps the canonical `/store/addons/{slug}/update` path, with preflight `reload_store()` and race-safe fallback for transient 403 when availability flips.
- Added focused regression tests in `tests/test_supervisor_client.py` for:
  - no-update path,
  - 403-to-no-update race mapping,
  - rebuild disconnect handling,
  - update disconnect handling.

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
