# Changelog

## 0.11.30 — Add-on schema hotfix

This patch fixes Home Assistant add-on option validation for direct-chat
and chat settings.

**Fixes:**
- `config.yaml` now accepts the runtime AI provider and chat fields already
  supported by the Python config model, including `cerebras`,
  `chat_backend`, `model`, `base_url`, `api_key`, `temperature`, chat
  settings, and `retention.chat_days`.
- This unblocks upgrades for installs that already have direct-chat
  options saved in Home Assistant.

## 0.11.29 — MCP transport, chat setup, and grounded diagnostics

This release bundles the recent MCP, chat, and diagnostics work into one
coherent add-on build.

**Highlights:**
- MCP now exposes resource catalog support plus SSE and streamable-HTTP
  transport endpoints for Home Assistant MCP Client compatibility, while
  keeping the legacy JSON-RPC route for existing clients.
- The dashboard and `/v1/chat/agents` now surface a clearer "Connect an
  AI agent" setup flow, including starter prompts and Home Assistant MCP
  Client guidance.
- Direct chat adds an isolated evaluator-guided retry loop so answers are
  more likely to stay grounded in gathered evidence instead of drifting.
- Diagnostics and graph surfaces now expose richer server-side facts,
  including graph-derived risks, capacity signals, route summaries, and
  node peer-comparison data.

## 0.10.3 — RSSI: name the strongest reporter

The Network-tab Nodes table previously showed each node's RSSI/LQI as a
naked dBm number with no indication of *who* was reporting that signal.
Because the value comes from the strongest entry in any router's Matter
NeighborTable (cluster 53), the implicit "via" is meaningful — for end
devices it's the parent, for routers it's the strongest peer, and across
the whole table it sketches the mesh shape.

**Changes:**
- `pipeline.nodes.get_latest_signal_strength` now also returns
  `best_reporter` (`{eui64, name, rssi, lqi, is_child}`) and a full
  `neighbors` list sorted strongest first. Same SQL query — we already
  scanned every link row, we just kept the reporter column.
- Nodes-table RSSI cell adds a "heard by &lt;name&gt;" sub-caption
  naming the strongest reporting router. End devices show their parent
  (matches the Role-column "via …"); routers show the strongest peer.
- Hover the RSSI cell to see every reporter with its RSSI/LQI, sorted
  best→worst — useful for routers/REEDs where multiple peers report.

## 0.10.2 — Network tab: deeper insight

The Network tab was showing data but not surfacing what to act on. This
release reshapes every panel on that tab so the most actionable signals
are visible without scrolling or hunting.

**Headline card (new):**
- One-line status banner: `degraded · 3 online / 12 offline of 15 · 2 active issues · 1 partition`.
- Red/yellow warning chips for split-brain (`distinct_thread_networks > 1`),
  more than one observed partition, duplicate physical-device groups
  (same hardware commissioned under multiple EUIs), stale nodes, and
  critical-severity issues. These conditions were previously buried in
  `health.summary` and invisible to dashboard users.

**Hot spots card (new):**
- Auto-picks up to four “worst right now” nodes: weakest RSSI online,
  most TX retries in last 1 h, most parent changes in last 24 h, oldest
  parent-link age. Clicking any chip scrolls to and flashes the matching
  row in the Nodes table.

**Active Issues card:**
- Severity-count pills (`1 critical · 3 warn`) in the header.
- Sorted by severity desc, then most-recent-first.
- Per-issue EUI64 chips are clickable and jump to the nodes table.

**Partitions card:**
- Shows leader name (not just last-4 of EUI), channel, network name,
  member count. When more than one partition is observed every row is
  flagged `split` so the condition is impossible to miss.

**Thread Nodes table:**
- **Default sort is now “worst first”** (offline → degraded RSSI/LQI →
  healthy). Alphabetical sort is one header-click away.
- Click any column header to sort; click again to reverse; third click
  returns to the default worst-first sort.
- New filter row: free-text search (name/EUI), status, role, area.
- Two new columns powered by Phase 4 counter time-series:
  - **TX retry 1h** — `Δ tx_retry_count` over the last hour. Yellow at ≥5,
    red at ≥20. `reset` rendered explicitly when the counter rolled over.
  - **Parent Δ 24h** — `Δ parent_change_count` over the last 24 hours.
    Yellow at ≥1, red at ≥3.
- `last_seen` now exposes the absolute ISO timestamp on hover.

**New endpoint backing the trend columns:**
- `GET /v1/counters/deltas` — returns per-EUI `{1h, 24h}` deltas for every
  numeric counter in one shot. Computed as `last - first` within each
  window; resets (negative diffs) report `null` so the UI can render
  `reset` instead of misleading drops. Polled by the dashboard every
  60 s (out of band from the 10 s `/v1/dev/status` poll).

No MCP, schema, or breaking changes — UI plus one new internal HTTP
endpoint.

## 0.10.1 — Graph layout: nodes always spaced out

The Network Graph tab was unusable when more than a handful of devices were
present — the built-in `cose` layout left nodes piled on top of each other,
and the Re-layout button replayed the same stuck state. Two changes fix this:

- Load `cytoscape-fcose` from CDN and use it as the default layout. fcose
  produces dramatically better separation, especially across disconnected
  components (phantom subgraphs, multiple Thread partitions). Falls back to
  tuned built-in `cose` (with `componentSpacing`, `nodeOverlap`,
  `randomize`) when the CDN is unreachable.
- Run a deterministic post-layout overlap-resolution pass on every render
  and on Re-layout, so any pairs the solver leaves too close get pushed
  apart along their connecting vector before fit.

Re-layout now always randomises so it can break out of stuck states instead
of reseating into the same overlap.

UI only — no API, schema, or tool changes.

## 0.10.0 — Phase 4: counter time-series

Per-node MAC/MLE counters are now recorded as time-series samples each pipeline
tick, so trends and resets become first-class signals instead of single-point
snapshots. Tool count 34 → 36. Not breaking.

**New tools (read-only, envelope-wrapped):**
- `get_counter_series` — returns raw or 5-minute-bucketed counter samples for
  a single node within an optional time window, plus per-counter
  `{delta, reset_detected, first, last}` summary. `resolution` is `raw` or
  `5min`.
- `compare_node_counters` — runs `get_counter_series` for two nodes over the
  same window and produces a `peer_summary` flagging counters whose deltas
  diverge by ≥ 2× between the pair (useful for "is this one node behaving
  worse than its neighbors?").

**Storage / schema:**
- Schema v19. New table `node_counter_samples (eui64, observed_at, tick_id,
  counters_json)` with composite PK `(eui64, observed_at)` plus per-eui and
  per-timestamp indexes.
- New `SQLiteStore` methods: `record_counter_sample`, `get_counter_samples`,
  `count_counter_samples`, `prune_counter_samples`.

**Pipeline:**
- `device_discovery` now records a counter sample for each node after
  `set_node_diagnostics` succeeds (14 MAC/MLE counter keys; `None` values
  filtered).
- After the existing `purge_expired_nodes` call, the runner now also calls
  `prune_counter_samples` using the configured retention window
  (`full_resolution_days=3`, `sampled_archive_days=14` by default): rows
  older than the archive horizon are deleted; rows in the middle window are
  down-sampled to 5-minute buckets via numeric averaging.

**Tests:** 249 passed, 1 skipped (was 235 / 1).

## 0.9.57 — Phase 3: triage entry points

Adds three high-signal read tools so an AI agent (or human) can drive a triage
session without having to pick among 31 tools blind on the first call.

**New tools (read-only, envelope-wrapped):**
- `start_triage` — first-call entry point. Returns `{environment, health,
  active_issues, recommended_next}` where `recommended_next` is up to 3
  follow-up tool calls inferred from active issues and pipeline state.
- `get_environment` — one-shot bundle of every version/identity surface:
  addon, HA Core, Supervisor, OTBR add-on, Matter Server add-on, Thread
  network identity, and the pipeline runner state.
- `get_pipeline_health` — recent pipeline ticks + summary including
  `consecutive_failed_ticks`, `stages_currently_failing`,
  `avg_duration_seconds`, and the current runner state.

**Supervisor client:** new helpers `get_core_info` (GET `/core/info`) and
`get_supervisor_info` (GET `/supervisor/info`) used by `get_environment`.

Tool count: 31 → 34. All three new tools are in `_READ_TOOLS` and emit the
standard `{data, meta}` envelope. No breaking changes.

## 0.9.56 — Phase 2: tool catalog reshape (BREAKING)

Tightens the MCP surface area from 40 to 31 tools. **Hard cut, no shim.**

**Removed (9 tools)** — functionality preserved internally or via other tools:
- `get_partition_state` — partition info is in `get_mesh_state` response
- `list_phantom_nodes` — use `list_all_nodes` with `status_filter="phantom"`
- `run_reasoner` — runs automatically on every pipeline tick
- `query_events` — superseded by `query_history` (unified timeline)
- `get_node_flap_history`, `get_link_flap_history` — covered by `analyze_node`
- `insert_test_event` — was a dev-only helper, no production callers
- `get_node_metadata` — covered by `analyze_node` and `list_all_nodes`
- `set_node_friendly_name` — friendly names sync via `sync_ha_devices`

**Renamed (6 tools)** — clearer names for AI-agent first-pass discovery:
- `get_network_topology` → `get_mesh_state`
- `query_timeline` → `query_history`
- `get_topology_snapshot` → `get_topology_history_entry`
- `list_topology_snapshots` → `list_topology_history`
- `diff_topology` → `diff_topology_history`
- `discover_thread_devices` → `sync_ha_devices`

**Enhanced**:
- `list_all_nodes` accepts `status_filter` (healthy/stale/offline/phantom)
- Descriptions for renamed tools follow Use-when / Returns / Caveats format

Internal helper functions `_build_partition_state` and `_build_phantom_list`
remain (consumed by the dashboard endpoints in `http_api.py`).

## 0.9.55 — Redact `ha_admin_token` in `get_config` response

Security fix. `get_config` was returning `ha_admin_token` in plaintext,
which leaks the admin LLT to any caller of the unauthenticated MCP port.
Now redacted to `"***"` alongside the existing `influx.token` redaction.

Found while verifying 0.9.54 envelope was live on device — the wrapped
response made the leak obvious.

## 0.9.54 — Phase 1: temporal-honesty envelope on read tools

Read-only MCP tools now return `{"data": ..., "meta": {...}}` so callers
can see at a glance how fresh the underlying SQLite-cached data is and
which pipeline tick produced it. Write/mutating tools (`ha_restart_addon`,
`ha_update_addon`, `close_issue`, etc.) pass through unchanged — their
existing `{action, result, requested_at}` shapes are correct as-is.

The `meta` block:

```jsonc
{
  "tool": "list_all_nodes",
  "as_of": "2026-05-12T19:23:45.123+00:00",
  "data_source": "sqlite_cache",
  "cache_age_s": 12.4,           // seconds since last pipeline tick finished
  "stale_after_s": 60.0,         // 2x pipeline interval; older than this = suspect
  "pipeline_tick": {
    "tick_count": 4271,
    "started_at": "...",
    "finished_at": "...",
    "duration_seconds": 3.8,
    "current_stage": null,
    "running": false,
    "error": null
  }
}
```

**Schema v18: `pipeline_ticks` table.** Every completed pipeline tick is
now persisted (id, started_at, completed_at, duration, per-stage JSON,
ok/fail counts, error). Best-effort write — persistence failure never
breaks a tick. Two new store methods: `record_pipeline_tick(tick)` and
`get_recent_pipeline_ticks(limit=20)`.

Wrapping happens at the dispatcher boundary (`_dispatch_and_wrap` in
`mcp_tools.py`), not inside individual tool functions, so existing unit
tests of those functions remain unchanged. Both transport layers — REST
`/mcp/call/{tool_name}` and JSON-RPC `tools/call` — go through the
wrapper.

Backward-compat: hard cut, no shim. There are no external consumers of
the old un-wrapped shape, so adding a layer of indirection would just be
noise.

## 0.9.53 — `ha_update_addon` actually works (admin token + `update.install`)

**The real fix.** After 0.9.49–0.9.52 each tested a different in-process
self-update route and each hit a different Supervisor lockdown layer, we
verified the canonical modern path:

```
POST http://homeassistant:8123/api/services/update/install
Authorization: Bearer <admin_long_lived_access_token>
{"entity_id": "update.thread_observability_update"}
→ 200 OK
```

This bypasses Supervisor entirely. HA Core's modern `update` integration
exposes one `update.<addon>_update` entity per managed add-on; calling
`update.install` on that entity triggers the update through the integration
plumbing rather than through any of the three blacklisted Supervisor paths.

**New `ha_admin_token` config option.** A new sensitive (password-typed)
option in the add-on configuration. When set to a long-lived access token
for an admin user, `ha_update_addon` calls `update.install` directly. When
left empty, the tool falls back to forcing `auto_update=true` and returns
`status="queued"` so Supervisor's periodic sweep still lands the update.

**Three blocked paths, documented for posterity.** From inside the add-on
container:

1. `POST /store/addons/{slug}/update` (Supervisor direct) →
   `403 "App can't update itself!"` — self-update guard.
2. `POST /core/api/services/hassio/addon_update` (HA Core service) →
   400; the `hassio` domain on modern HA only registers
   start/stop/restart — no `addon_update` service exists.
3. `POST /core/api/hassio/addons/{slug}/update` (HA Core hassio proxy) →
   blocked by Supervisor's API security middleware as `"... is blacklisted!"`.

The token is never logged, never echoed in tool output, and is sent
directly to HA Core (not through Supervisor's `/core/` proxy, which
strips and replaces the `Authorization` header). The HA Core URL defaults
to `http://homeassistant.local.hass.io:8123` and is overridable via the
`HA_CORE_URL` env var.

**Tests.** `tests/test_supervisor_client.py` rewritten for the new
shape: token-present path, token-absent fallback, dry-run, store-slug
fallback, transport-error surfacing, HTTP-error surfacing, and the token
never leaking into the result payload.

## 0.9.52 — Round-trip test for HA-Core hassio proxy `ha_update_addon`

No-op bump to verify the 0.9.51 routing end-to-end from MCP. If
`ha_update_addon` lands 0.9.52 cleanly without a UI click, the in-loop
deploy cycle is fully unblocked and the standing rule blocking
`ha_update_addon` is lifted.

## 0.9.51 — Switch `ha_update_addon` to HA Core hassio HTTP proxy

**Discovery from 0.9.50 test.** The HA Core *service* path
`POST /core/api/services/hassio/addon_update` returns HTTP 400 because
`hassio.*` services are registered with `async_register_admin_service` and
the Supervisor token is not admin-equivalent on HA Core.

**Fix.** `update_addon()` now POSTs to
`POST /core/api/hassio/addons/{store_slug}/update` — Home Assistant Core's
hassio HTTP proxy view (the same path the HA frontend uses when you click
"Update" in the UI). HA Core forwards the request to Supervisor under its
own admin identity, so Supervisor's self-update guard does not fire.

**Response shape additions.**
- `endpoint`: `/core/api/hassio/addons/<slug>/update`.
- `via`: `"ha_core_hassio_proxy"`.

Tests assert the new endpoint and that no JSON body is sent (the slug is
in the URL).

**Operational note.** If this path ever fails, `ha_set_auto_update(enabled=true)`
followed by waiting for the next Supervisor sweep remains a verified
fallback — used successfully to bootstrap 0.9.48 → 0.9.49 during this
investigation.

## 0.9.50 — Verify HA-Core-routed `ha_update_addon` (no functional change)

No-op version bump to exercise the 0.9.49 update path from MCP end-to-end:
`ha_update_addon(dry_run=true)` followed by a real `ha_update_addon` call.
If 0.9.50 lands without the 403 self-update guard tripping, the in-loop
deploy cycle is fully unblocked.
## 0.9.49 \u2014 Route `ha_update_addon` through Home Assistant Core service

**Root cause uncovered by the 0.9.48 dry-run test.** Calling
`POST /store/addons/{slug}/update` from inside the add-on's own process is
refused by Supervisor with HTTP 403 and the body
`{"message": "App {slug} can't update itself!"}`. The 0.9.47 fix correctly
resolved the slug but still tripped this self-update guard on the real call.
The dry-run was correct \u2014 the failure happened the moment we actually POSTed.

**Fix.** `update_addon()` now POSTs to
`/core/api/services/hassio/addon_update` with body `{"addon": store_slug}`,
which dispatches through Home Assistant Core's `hassio.addon_update`
service. Supervisor sees HA Core as the caller, not the add-on, and the
self-update guard does not fire. The add-on already declares
`homeassistant_api: true` so the existing `SUPERVISOR_TOKEN` works against
HA Core. The Supervisor direct path is no longer auto-attempted as a
fallback because it will always 403; it remains documented in the response
under `endpoint_fallback` for diagnostic clarity.

**Response shape (new fields).**
- `endpoint`: now the HA Core service path.
- `endpoint_fallback`: the (intentionally unused) Supervisor direct path.
- `via`: `"ha_core_service"`.
- On success, `note` explains that Supervisor will restart the add-on
  asynchronously, so the active MCP connection will drop.

Tests updated to assert the HA Core path is the one POSTed and that the
body is `{"addon": store_slug}`.
## 0.9.48 — Dev-loop smoke test (no functional change)

Version bump only. Used to verify that the 0.9.47 fix to `ha_update_addon` correctly resolves the store slug and dispatches the update end-to-end on the live install. If you are reading this entry, the in-loop deploy cycle is unblocked.

## 0.9.47 — Fix `ha_update_addon` (correct store slug, honest error reporting, dry-run)

Prerequisite for the 0.10.0 catalog rework: a tight in-loop deploy cycle. Three bugs in `update_addon()` were causing the addon to occasionally self-uninstall when the MCP tool was invoked, which is why the prior standing rule was to update only via the HA UI.

- **Use the resolved store slug.** Previously the code tried `/addons/self/update` first, then `/store/addons/{self_slug}/update` where `self_slug` came from `/addons/self/info` and carried the local-repo hash prefix (`9e5048e8_thread-observability`). The store endpoint expects the *store* slug (`thread-observability`); on recent Supervisor versions, calling the store endpoint with the prefixed slug has been observed to be interpreted as a fresh install of a non-existent add-on, silently clearing the installed instance. The new `_resolve_store_slug()` reads `/store/addons`, matches by exact-slug or suffix-after-`_`, prefers installed entries, and returns the canonical store slug.
- **Stop calling `/addons/self/update`.** It is unreliable across Supervisor versions and was the source of the misrouted-call symptom above. Only `/store/addons/{store_slug}/update` is dispatched.
- **Stop swallowing transport errors as success.** Previously any `httpx.RequestError` mid-POST was coerced to `{status: "accepted", performed: true}`. Now it returns `{status: "transport_error", performed: "unknown", error_class, error, note}` so the caller knows to consult supervisor logs.
- **New `dry_run=true` argument.** Performs slug resolution + version check + endpoint computation and returns the resolved endpoint *without* POSTing. Use this to verify the fix end-to-end on a live install before any real update is dispatched.
- **Richer `http_error` shape.** Non-transport HTTP errors now include `http_status` and a truncated `response_body` so the caller can distinguish "no update available" (403/404), "bad request" (400 with detail), and "auth failure" (401) without parsing exception text.

Tests: six new unit tests covering slug resolution, dry-run, store-unreachable fallback, transport-error surfacing, and HTTP-error reporting. Previous tests that asserted the now-removed `/addons/self/update` fallback were rewritten for the new behavior.

## 0.9.46 — Per-node Thread network identity, duplicate physical device detection, `wrong_network` reasoner rule

Motivated by a live diagnosis where the addon could not self-diagnose a partition split caused by a re-commissioned device joining the *wrong* Thread network — the cluster 53 attributes that prove it (`NetworkName`, `ExtendedPanId`) were polled every cycle but never persisted. The fix is to extend already-collected data rather than add new pipelines.

- **Schema v17 — six new columns on `nodes`.** `network_name` and `extended_pan_id` carry per-node Thread network identity from Matter cluster 53 (attrs `0x0002`, `0x0004`); `vendor_id`, `product_id`, `serial_number` carry hardware identity from BasicInformation cluster `0x0028` (attrs `0x0002`, `0x0004`, `0x000F`). Two new indexes: `idx_nodes_extended_pan_id` and `idx_nodes_physical_identity (vendor_id, product_id, serial_number)`.

- **`_extract_thread_diagnostics` populates the new fields.** `ExtendedPanId` arrives as an int or hex string depending on matter-server SDK version; the extractor normalizes both to lowercase 16-char hex so persistence and comparison are stable. Base64 (8-byte octstr) fallback included.

- **`partition_split` evidence is now self-diagnosing.** Each partition entry in the issue's evidence includes `network_name` and `extended_pan_id`. Two new top-level fields surface the credentials-vs-RF distinction explicitly: `distinct_extended_pan_ids` lists every EPID seen across live partitions, and `credentials_mismatch_suspected: true` when more than one is present. A consultant inspecting the issue can now answer "is this RF fragmentation or stale credentials?" without leaving the tool.

- **Reasoner rule `wrong_network`.** Computes the modal `extended_pan_id` across non-phantom nodes; any node with a non-NULL EPID that disagrees with the majority gets a per-node `wrong_network` issue. Evidence carries both the node's and the modal EPID/name plus member counts so the playbook can frame the remediation. Added to `managed_kinds` so the rule auto-closes when the minority node re-joins the right network.

- **`analyze_node` surfaces `physical_identity`.** When the node has vendor/product/serial, the response includes a new `physical_identity` block listing every *other* EUI in the database sharing the same hardware-identity triple. This is the live signal for "this device was re-commissioned and the old EUI64 was never cleaned up" — exactly the failure mode the live diagnosis turned up.

- **Health snapshot summary gained three counters.** `duplicate_physical_device_groups` and `duplicate_physical_device_rows` count hardware-identity collisions across non-phantom nodes; `distinct_thread_networks` counts distinct EPIDs. Any value > 1 on the last is the same red flag the new reasoner rule fires on.

- **New MCP tool `list_thread_datasets`.** Wraps HA core's `thread/list_datasets` WebSocket command through the Supervisor proxy at `ws://supervisor/core/websocket` using the existing `SUPERVISOR_TOKEN`. Returns `{datasets: [{network_name, extended_pan_id, channel, source, preferred, ...}], count, fetched_at, cached, cache_ttl_seconds}` — `extended_pan_id` normalized to the same 16-char lowercase hex as the per-node columns so the values compare directly. Cached in-memory for 5 minutes.

- **Playbook `wrong_network`.** New `playbooks.json` entry walking the consultant through deleting the stale dataset in HA Settings → Thread, factory-resetting the device, re-commissioning, and using `analyze_node` on the new EUI to detect the duplicate `physical_identity` row.

Tests: 5 new unit tests covering network-identity extraction (int/hex/short EPID), wrong_network open/close/no-fire, duplicate physical identity in analyze_node, and the two new health counters. Migration test updated to v17.

## 0.9.45 — Three live-network fixes: analyze_node membership binding, reasoner-owned partition_split close, re_attached_node reporter-reattach guard

Three independent bugs surfaced during the first live assessment of 0.9.44 against a real Thread network. All three were correctness gaps where a code path silently dropped or misattributed signal; fixing them required no schema change and no API change.

- **Fix A — `analyze_node` now binds global issues via evidence inspection.** `analyze_node(eui)` previously filtered `list_active_issues()` by `issue.eui64 == eui`, which works for per-node issues but misses *global* issues (those opened with `eui64=None`) such as `partition_split`. A node sitting alone in a minority partition is clearly the affected device, yet the consultant view showed it with no open issues. New helper `_evidence_implicates_eui` scans the standard evidence shapes (`partitions[].members`, top-level `members`) and binds any global issue whose evidence references the queried EUI, tagging it with `implicated_via: "evidence"`. New tests cover both the positive bind and the negative (unrelated nodes do not pick the issue up).

- **Fix B — reasoner is now the authoritative owner of `partition_split` close.** `partition_split` was opened by `matter_discovery._persist_matter_diagnostics` and only closed there — which means a single missed close path (matter-server WS hiccup, an empty poll where the close-on-empty fallback raced an unrelated exception, etc.) leaves the issue immortal. The reasoner runs every tick regardless of matter-server health, so it now also calls `topology.build_topology()` at the end of `run_reasoner` and closes any still-open `partition_split` whose live topology shows ≤ 1 partition. Discovery still owns the open path (it has the full per-router evidence); the reasoner owns the close-on-resolve path. New test covers the close-on-resolve flow.

- **Fix C — `re_attached_node` suppressed when the reporter itself just re-attached.** When a router increments its own `parent_change_count` (it re-attached to a new parent), MLE establishes new sessions with every neighbor and each neighbor's `link_frame_counter` / `mle_frame_counter` — *as seen by this reporter* — resets to a fresh value. Without a guard the existing "frame counter dropped → emit `re_attached_node`" rule fires once per neighbor per poll attributed to the wrong devices. Observed live as one router triggering a storm of 6 spurious `re_attached_node` events every 35 s, same EUIs every cycle. Fix pre-computes the set of reporters whose `parent_change_count` strictly increased this cycle and skips the `re_attached_node` emission block for any reporter in that set. Two new tests cover both the suppression and the genuine-drop-still-fires path.

Test baseline: 208 passed, 1 skipped (203 prior + 5 new).

## 0.9.44 — Bundle playbook corpus as package data

Hotfix for 0.9.43. The playbook corpus loader resolved `_DEFAULT_CORPUS` by walking three directories up from `pipeline/playbooks.py`, which works for editable installs in dev but resolves to `/usr/lib/python3.12/playbooks/playbooks.json` after `pip install` inside the addon container — a path that doesn't exist. `analyze_node` and `lookup_playbook` therefore returned `{"error": "[Errno 2] No such file or directory: ..."}` in production.

- **`app/playbooks/playbooks.json` → `app/src/thread_observability/pipeline/playbooks.json`.** The corpus now lives next to the module that loads it.
- **`pyproject.toml`** declares `"thread_observability.pipeline" = ["playbooks.json"]` under `[tool.setuptools.package-data]` so `pip install` ships it alongside the `.py` files.
- **`pipeline/playbooks.py`** resolves `_DEFAULT_CORPUS = Path(__file__).resolve().parent / "playbooks.json"`, which is correct under both editable and regular installs.

No schema change. No test changes — the existing test suite (203 passed, 1 skipped) covers both code paths because the path resolution is identical for editable and installed packages once the file is co-located.

## 0.9.43 — Consultant tier: timeline, topology snapshots, playbook corpus, analyze_node + observer-events suppression

Ships three intertwined layers in one release because they all feed the same end goal: turning the addon into a problem-solving consultant rather than a passive observer. **Tier 2** ("observer events") records when the *addon itself* lost ground-truth signal — OTBR/Matter polls failing, Supervisor unreachable, the addon restarting — so downstream reasoning can distinguish "device went offline" from "we stopped watching". **Tier 3** ("suppression") teaches the reasoner to demote `offline_node` issues whose trigger window overlaps a known observer outage, so a Supervisor hiccup no longer manifests as 40 false critical alerts. **Tier 4** ("consultant") layers four new analysis surfaces on top: a unified `query_timeline` that merges events + issues + observer outages on one axis, periodic deduplicated `topology_snapshots` with a structural `diff_topology`, a 15-entry remediation `playbooks` corpus addressable by failure-kind, and an `analyze_node` bundled tool that composes all of the above into a single payload an LLM consultant can reason over.

- **Schema migrations v14→v16 (`storage/sqlite_store.py`).** v14 introduces `observer_events(id, kind, source, started_at, ended_at, opened_event_id, closed_event_id, payload_json, error_text)` indexed on `(kind, source, started_at)` for outage windows, plus a `observer_state` key/value table for the observer's last-known status between ticks. v15 adds `suppressed`, `suppression_source`, `suppression_evidence_json` to `issues` so the reasoner can downgrade severity without losing the original signal. v16 adds `topology_snapshots(id, captured_at, snapshot_hash, partition_id, node_count, link_count, snapshot_json)` with indices on `captured_at DESC` and `snapshot_hash` for the dedup-and-diff path. Migrations are forward-only and idempotent — existing 0.9.42 databases pick them up on first start.
- **Observer events pipeline (`pipeline/observer_events.py`, `pipeline/runner.py`, new stage `observer_events`).** Owns `otbr_rest`, `matter_supervisor`, `addon_self`, and `ha_supervisor` sources. Each tick polls source liveness, opens an observer event on the first failure, and closes it (writing `ended_at` + `closed_event_id`) on first recovery. Open events are visible to the reasoner via `list_observer_events_in_window` (overlap predicate: `started_at <= until AND (ended_at IS NULL OR ended_at >= since)`). The `addon_self` source records a point-in-time event at startup so we know *when* the addon came up, which is enough to bound any "did we miss something" reasoning across a restart.
- **Reasoner suppression of `offline_node` during observer outages (`pipeline/reasoner.py`).** Before opening or escalating an `offline_node` issue, the reasoner queries observer events in the trigger window. If any `otbr_rest` or `matter_supervisor` event overlaps, the issue is annotated `suppressed=true`, `suppression_source=<observer source>`, and the severity is downgraded from `crit` to `warn`. The original evidence is preserved; the suppression hint plus the offending observer event id lands in `suppression_evidence_json` so the UI can render "this was probably us, not the radio". Suppression is reapplied each tick, so an event that was correctly suppressed during the outage stays suppressed afterwards — re-evaluating the same trigger window keeps the verdict stable.
- **`query_timeline` MCP tool (`pipeline/timeline.py`).** Unifies three sources onto one chronological axis: `events` (verbatim), `issues` (synthesized `issue.opened` at `opened_at` and `issue.closed` at `closed_at`), and `observer_events` (synthesized `observer.<kind>` at `started_at` and `observer.<kind>.ended` at `ended_at` only when distinct, so a point-in-time event doesn't double). Every row normalises to `{ts, source, kind, eui64?, severity?, details, ref_id}` and is sorted newest-first, deterministically broken by `ref_id`. Optional filters: `since`/`until`, `eui64`, `kinds` whitelist, `sources` allow-list (defaults to all three), and a hard `limit` (default 500).
- **Topology snapshots: capture, list, get, diff (`pipeline/topology_snapshot.py`, MCP tools `get_topology_snapshot` / `list_topology_snapshots` / `diff_topology`).** Each pipeline tick builds the current topology, computes a canonical SHA-256 fingerprint over sorted `{nodes, links, partitions}` *excluding* churning fields (`computed_at`, `friendly_name`, `last_seen`), and inserts only when the fingerprint differs from the latest stored snapshot — unless the heartbeat window (default 60 minutes) has elapsed, in which case an unchanged snapshot is rewritten so a long-stable network still leaves a trail. `diff_topology(a, b)` reports added/removed/changed nodes (role, routing_role, partition_id, parent_eui64) and added/removed links keyed by `(from, to, source)`. Returns `{error: "snapshot_not_found", a_found, b_found}` if either id is missing rather than failing silently.
- **Playbook corpus (`app/playbooks/playbooks.json`, `pipeline/playbooks.py`, MCP tools `list_playbooks` / `lookup_playbook`).** 15 remediation entries covering the recognized failure kinds: `parent_churn`, `attach_failures`, `offline_node`, `re_attach_storm`, `mesh_disagreement`, `observer_suppressed`, `partition_split`, `sed_battery_drain`, `rf_coexistence`, `multi_br`, `srp_outage`, `channel_mismatch`, `leader_churn`, `dataset_rotation`, `phantom_nodes`. Each entry carries `summary`, `evidence_to_collect`, `remediation_steps`, and external `references`. Lookup priority is `id > kind > substring query` across id/title/summary. The `observer_suppressed` entry applies to all reasoner kinds so any issue marked `suppressed=true` automatically matches its remediation playbook in addition to the kind-specific one.
- **`analyze_node` bundled MCP tool (`pipeline/analyze_node.py`).** One call returns the full consultant view for a single EUI: `node` metadata, `parent` (derived from `is_child=1` links), `neighbors`, `open_issues`, `recent_issues` (last 5), a windowed `timeline` (default 24h), `baselines` (parent-change count and status-change count compared against the trailing equal-length prior window — default 7d), `playbooks` (deduplicated union of playbooks matching all open + recent issue kinds), and `matched_issue_kinds`. Designed so an LLM consultant can plan remediation off a single tool call without orchestrating six.
- **Pipeline stages now six.** Order: `otbr_log_ingest` → `observer_events` → `otbr_rest` → `matter_discovery` → `topology_snapshot` → `reasoner`. The new stages are isolated under the existing per-stage failure boundary — a snapshot or observer-events failure does not block reasoning, and the failure surfaces in `dev_status.pipeline_stages`.
- **Tests.** 35 new tests across 8 files. New: `test_observer_events.py` (4), `test_timeline.py` (3), `test_topology_snapshot.py` (6), `test_playbooks.py` (6), `test_analyze_node.py` (4). Extended: `test_reasoner.py` (+7 for suppression cases including the strictly-before-trigger non-suppression case), `test_sqlite_store.py` (+5 for observer events store methods and v13 router/diagnostics roundtrips), `tests/integration/test_pipeline_tick.py` (`STAGE_NAMES` expanded to six). Schema version assertion bumped to 16. Total suite: 203 passed, 1 skipped.

## 0.9.42 — SED mesh-alive + MAC error counters + link-flap + parent-change events

Closes four diagnostic gaps that landed on the same release because they all hang off the cluster-53 sweep. **(1)** Sleepy end devices that HA marks `available=False` because they haven't published an attribute update in the LWT window are now distinguishable from genuinely offline SEDs: the topology output carries `mesh_alive`, `parent_link_age_seconds`, and a `sed_classification` of `fresh`/`stale`/`orphan` derived from the parent router's `NeighborTable`. A SED whose parent claimed it within the last 5 minutes is `fresh` regardless of what HA's MQTT view thinks. **(2)** Per-node Tx/Rx MAC counters (cluster 53 attrs 0x13, 0x14, 0x16, 0x21, 0x24–0x27, 0x31, 0x32, 0x35, 0x36) are persisted on each sweep so radio-layer problems (CCA failures, FCS errors, MIC failures, duplicate-Rx storms) surface independently of MLE state. **(3)** Per-edge `link_acquired`/`link_lost` events are emitted when a reporter's `NeighborTable` or `RouteTable` changes between sweeps, with a `get_link_flap_history` MCP tool that ranks unstable edges by an order-independent (reporter, neighbor) pair so a single flap surfaces once. **(4)** `parent_change` events are emitted when a SED's monotonic `ParentChangeCount` increments, recording `delta` and the partition the swap happened in.

- **Schema v13 (`storage/sqlite_store.py`).** Twelve new columns on `nodes`: `partition_id_change_count`, `better_partition_attach_attempt_count`, `tx_total_count`, `tx_retry_count`, `tx_err_cca_count`, `tx_err_abort_count`, `tx_err_busy_channel_count`, `rx_total_count`, `rx_duplicated_count`, `rx_err_no_frame_count`, `rx_err_sec_count`, `rx_err_fcs_count`. All sourced from the existing cluster-53 sweep (no new HA roundtrips); attribute IDs documented inline in the migration block. `set_node_diagnostics` extended with the matching keyword arguments — overwrite semantics, not COALESCE, because every counter is sourced from the same poll and a `None` legitimately means "device didn't expose this attribute this cycle".
- **`replace_links_for_reporter` now returns a diff (`storage/sqlite_store.py`).** Previously returned `int` (count inserted); now returns `{"inserted": N, "added": [eui...], "removed": [eui...]}`. The prior-neighbor set is snapshotted inside the same transaction as the DELETE so a flap that completes within one tick (child detaches and re-attaches) shows up as removed+added on the parent without losing the new row.
- **Link gain/loss event emission (`pipeline/device_discovery.py`).** `_persist_matter_diagnostics` consumes the diff and inserts `link_acquired` / `link_lost` rows into `events` with payload `{reporter_eui64, neighbor_eui64, source, partition_id}`. The very first observation of a (reporter, source) tuple is suppressed (using `prior_node.diag_updated_at IS NULL` as the proxy) so a cold start doesn't fire `link_acquired` for every existing edge. Summary log line gained `parent_changes`, `link_acq`, `link_lost` counts.
- **Parent-change event emission (`pipeline/device_discovery.py`).** For every node, diff the new `parent_change_count` against the prior snapshot from `prior_by_eui`. When it strictly increased, insert a `parent_change` event with `{from_count, to_count, delta, partition_id}`. A drop (counter reset on firmware update or factory reset) is treated as a re-baseline, no event. Counters that disappear or stay equal are no-ops.
- **`get_link_flap_history` store method + MCP tool (`storage/sqlite_store.py`, `api/mcp_tools.py`).** Filters `events` by `type IN ('link_acquired', 'link_lost')` with optional `reporter_eui64`, `neighbor_eui64`, `source`, and `since` parameters; returns transitions newest-first plus a `flap_counts` map keyed by the sorted pair `reporter|neighbor` so both directions of an edge land in the same bucket. Each pair bucket carries `total`, `acquired`, `lost`. Available since v0.9.42 — earlier transitions are not recorded.
- **SED mesh-alive classifier (`pipeline/nodes.py`).** New `_build_sed_mesh_state` joins parent-side `is_child=1` link rows with `max(observed_at)` per child EUI; classifies the link age against a 5-minute window (`_SED_MESH_ALIVE_WINDOW_SECONDS`). `build_topology` and `get_node_summary` enrich SED rows with `mesh_alive: bool`, `parent_link_age_seconds: int|None`, `sed_classification: 'fresh'|'stale'|'orphan'`. Routers/FEDs get neither key — the fields only make sense for sleepy devices. An operator querying "why does HA say my Hue motion sensor is unavailable when I'm standing in front of it" now sees `available=false, mesh_alive=true, sed_classification='fresh'` and immediately knows the integration is wrong, not the radio.
- **Cluster 53 attribute extraction (`pipeline/device_discovery.py`).** `_extract_thread_diagnostics` extended with twelve `_get_int` reads against attribute suffixes 19/20/22/33/36/37/38/39/49/50/53/54. Decimal suffixes (not hex) match how `python-matter-server` keys attribute values.
- **Tests.** Schema version assertion bumped to 13. New: `test_replace_links_returns_diff`, `test_get_link_flap_history_aggregates_pairs`, `test_set_node_diagnostics_persists_error_counters`. The first-observation suppression heuristic in `_persist_matter_diagnostics` is covered indirectly by the migration test plus the existing diagnostics-persist tests; tighter coverage will land alongside Tier 2.

## 0.9.41 — Flap history: persist status transitions + per-EUI flap counts

Closes the observability gap that made automated flap diagnosis impossible. Until this release `recompute_node_statuses` updated `nodes.status` and bumped `status_changed_at` in place — only the *most recent* transition was recoverable; everything earlier was overwritten. `query_events` therefore returned `count = 0` for transition events because none were ever inserted. v0.9.41 emits a `status_change` row into the `events` table for every transition, retained per `full_resolution_days`, and exposes a `get_node_flap_history` MCP tool that returns the raw transitions plus an aggregate `flap_counts` map so flappers can be ranked over any window. Also fixes a latent bug in `ha_get_addon_logs` that silently ignored the `slug` parameter.

- **`recompute_node_statuses` now persists transitions (`storage/sqlite_store.py`).** Replaced the single UPDATE with a select-then-update-and-insert sequence inside one transaction: a common-table-expression computes `(eui64, old_status, new_status)` for every row, transitions where `old != new` are `executemany`-inserted into `events` with `type = 'status_change'` and `payload_json = {"from": old, "to": new}`, then the same projection drives the UPDATE. Result dict gains a `transitions` count alongside the existing `changed` count. The `events.id` AUTOINCREMENT means transitions are totally ordered even within a single tick; the existing `idx_events_eui64_ts` and `idx_events_type_ts` indices make the flap-history query cheap without a new index.
- **`get_node_flap_history` store method + MCP tool (`storage/sqlite_store.py`, `api/mcp_tools.py`).** Filters `events` by `type = 'status_change'` with optional `eui64` and `since` (ISO-8601) parameters; returns the most-recent `limit` transitions (1 ≤ limit ≤ 5000, default 500) newest-first, plus a `flap_counts` map keyed by EUI with `total` and `by_transition` ("online->offline": N) breakdowns. Suitable for "rank the top flappers in the last 24h" queries directly from VS Code or Claude Desktop. Available since v0.9.41 — transitions older than the upgrade are not recorded.
- **`ha_get_addon_logs` slug parameter fix (`api/mcp_tools.py`).** The handler always called `supervisor_client.get_addon_logs(n)` regardless of input and hard-coded `source = "supervisor:/addons/self/logs"`, so requests for `core_openthread_border_router` or `core_matter_server` silently returned the addon's own logs. Now reads `arguments["slug"]`, passes it through to the existing `get_addon_logs(lines, slug=...)` keyword, returns the correct `source` URI, and echoes the resolved `slug` in the response. The tool's `inputSchema` was expanded to declare the `slug` parameter and its description was updated so callers understand the contract.

## 0.9.40 — Schema cleanup: drop `is_phantom` column + `sweep_phantoms`

Internal cleanup release. The `is_phantom` boolean column on `nodes` and the `sweep_phantoms` method were carried as transitional mirrors after v7 introduced the `status` enum as the authoritative lifecycle signal. Every consumer in the codebase already preferred `status == 'phantom'` first; the column was just an aging shim. This release drops it, simplifies `recompute_node_statuses` to one UPDATE clause, and renames the misleadingly-named `_fallback_device_registry` helper to `_load_device_registry` since it has been the primary (and only) source of HA device-registry data for a long time.

- **Schema v12 (`storage/sqlite_store.py`).** New migration drops `idx_nodes_phantom` then the `is_phantom` column itself (SQLite ≥ 3.35 `ALTER TABLE DROP COLUMN`). `recompute_node_statuses` no longer writes `is_phantom`; `list_phantom_nodes` and `purge_phantom_nodes` now select `WHERE status = 'phantom'`. The `sweep_phantoms` method is gone — `recompute_node_statuses` already covered the same transition since v7.
- **API surface cleanup.** `TopologyNode` in `api/schemas.py` drops `is_phantom`. The `pipeline/nodes.py` enrichment output stops emitting the mirrored field. `http_api.py` and `mcp_tools.py` phantom checks read `status` directly. The dashboard JS computes phantom-ness from `status === 'phantom'` inline.
- **`_load_device_registry` rename (`pipeline/device_discovery.py`).** The function previously named `_fallback_device_registry` is the only source of HA device-registry data — the "fallback" naming dated back to when OTBR `/api/topology` was supposed to provide device_id mappings, which never materialised. Renamed and docstring updated to reflect reality.
- **Tests.** Schema version assertion bumped to 12. `test_sweep_phantoms_marks_old_and_clears_fresh` collapsed to a documented no-op (kept as a placeholder so anyone grepping for the old name finds the explanation). `test_purge_phantom_nodes_removes_links` and `test_topology` switched to driving phantom state through `recompute_node_statuses`.

## 0.9.39 — "Online" now means "HA can control it"

Realigns the canonical online/offline signal with what the user sees in the Home Assistant UI. Until now `nodes.status` was derived from `last_referenced_at` recency — i.e. "did we see this EUI in any Matter cluster sweep recently". That definition diverged from the user's mental model: what actually matters is whether HA can send commands to the device right now. A bridged Matter light with a healthy radio but a broken integration was reported `online`; a sleepy child that hadn't been re-observed in 5 minutes was reported `offline` even though HA could turn it on instantly. v0.9.39 sources online state from HA's per-entity availability and keeps `last_referenced_at` as an independent diagnostic field, so disagreement between the two becomes useful (mesh sees it but HA can't = integration bug; HA sees it but mesh doesn't = sleepy child or stale registry).

- **Schema v11 (`storage/sqlite_store.py`).** Three new columns on `nodes`: `available` (1/0/NULL — HA can reach at least one primary entity), `availability_source` (`'ha_entity'` | `'otbr_rest'` | `'unknown'`), `availability_checked_at` (ISO timestamp of the last lookup). New `apply_availability(updates)` method stamps all three in one transaction, UPDATE-only to preserve the registry-first contract from v9. Indexed on `available` for the recompute query.
- **`recompute_node_statuses` rewrite.** Primary signal is now `available`: `online` iff `available = 1`, `offline` iff `available = 0 AND device_id IS NOT NULL`. When `available IS NULL` (probe hasn't run yet, or HA token missing) we fall back to the legacy `last_referenced_at` heuristic so a fresh install isn't blank for the first cycle. Phantom rules unchanged — non-HA-registered nodes still age out after `phantom_seconds`.
- **HA entity availability fetcher (`pipeline/ha_availability.py`, new).** Reads `core.entity_registry` from `/config/.storage`, builds `{device_id: [(domain, entity_id), ...]}` filtered to non-disabled entities, then fetches `/api/states` via the Supervisor HA proxy (`http://supervisor/core/api/states` with `SUPERVISOR_TOKEN`). A device scores `True` if any primary-domain entity (light/switch/cover/fan/lock/climate/etc.) is in a non-`unavailable`/`unknown` state; falls back to diagnostic domains (binary_sensor/sensor/event/select/number) when no primaries exist. Returns `None` for devices with zero scoreable entities so the caller leaves the column NULL rather than guessing.
- **Wired into `discover_and_sync`.** After the registry sync + Matter cluster sweep, before `recompute_node_statuses`: fetch availability, join `device_id → eui64` from `list_nodes()`, call `apply_availability`. Best-effort — any failure (missing token, REST 4xx, JSON error) is logged and the recompute falls back to the legacy path.
- **OTBR fallback (`pipeline/otbr_rest.py`).** OTBR has no HA entity, so HA-availability scoring would leave it `available IS NULL` forever. After a successful `/node` fetch in `ingest_once`, stamp `available = True, source = 'otbr_rest'` — getting data back from `/node` *is* the OTBR equivalent of "reachable".
- **API exposure (`pipeline/nodes.py`, `api/mcp_tools.py`).** `list_nodes_enriched` and `get_node_summary` now surface `available`, `availability_source`, `availability_checked_at` alongside the existing `last_referenced_at`. The dashboard's status pill already reads `n.status`, which is now availability-driven; no UI changes needed for v0.9.39, but the two timestamps make mesh-vs-HA disagreement visible to consumers that want it.
- **Tests: 163 passing.** New: `test_apply_availability_updates_columns`, `test_recompute_node_statuses_availability_first` (proves HA availability dominates mesh recency in both directions), and a full `test_ha_availability.py` suite covering `_score_device` (primary wins, all-unavailable, fallback domain, no-data, blank-state) and `_build_device_to_entities` (disabled/hidden filtering).

## 0.9.38 — Phantom-loop fix + OTBR as a first-class reporter

Two related fixes for the mesh graph. The first kills a visual artifact in the route walker that made any two routers with direct OTBR links appear to route through each other (no actual on-wire loop — just data we were misreading). The second closes the OTBR's own blind spot: previously the border router only showed up as a destination of other routers' edges, never as a reporter, because we never ingested its NeighborTable or RouteTable. Now we do.

- **Phantom-loop fix (`pipeline/routing.py`).** OpenThread fills `RouteTable.NextHopRouterId` even when the reporter has a direct MLE link to the destination — the field names the *route-advertisement relay* that last gossiped this route, not the actual forwarding next hop. Path reconstruction must check `PathCost == 1 && LinkEstablished == 1` first: when both hold, the row's destination *is* the forwarding next hop and `NextHopRouterId` must be ignored. Without this rule, two routers (e.g. Office Light + Downstairs Hallway in the live test mesh) both with direct OTBR links can cross-point at each other and produce a `loop_detected` walk. Applied to `walk_route_to_otbr` (short-circuits the next-hop chain) and to `list_neighbors_enriched` (route rows now expose `effective_next_hop_eui64`, `effective_next_hop_name`, and `is_direct_link` derived fields; raw `next_hop_*` fields preserved for diagnostics).
- **OTBR NeighborTable + RouteTable ingest (`pipeline/otbr_rest.py`).** OTBR REST exposes `/node/neighbors` (NeighborInfo array — RSSI, LQI, frame counters, IsChild, RxOnWhenIdle, FullThreadDevice) and `/node/routers` (RouterInfo array — RouterId, NextHop, PathCost, LqiIn/Out, Allocated, LinkEstablished). New `fetch_otbr_neighbors` / `fetch_otbr_routers` helpers + `_decode_otbr_neighbors` / `_decode_otbr_routers` mappers feed `replace_links_for_reporter` under the OTBR's own EUI. The OTBR's `router_id` is now also derived from its routers self-entry. Best-effort: older OTBR builds without these endpoints degrade gracefully (debug log + skip). The `ingest_once` return now includes `otbr_neighbors_persisted` / `otbr_routes_persisted` counters; the per-cycle log line surfaces them too.
- **Tests: 154/155 passing** (1 deliberate skip). New: `test_walk_route_direct_link_short_circuit`, `test_list_neighbors_effective_next_hop_direct`, `test_walk_route_genuine_multihop_unchanged` (regression guard), `test_decode_otbr_neighbors_maps_fields`, `test_decode_otbr_routers_maps_fields`, `test_decode_otbr_skips_missing_eui`.
- **Future work flagged.** `/v1/topology` still treats raw `next_hop_router_id` as ground truth for `route`-class edges; once we've validated the live behaviour with this release we'll switch it to the derived `effective_next_hop_eui64` so the topology view stops drawing phantom edges. Out of scope here to keep the change reviewable.

## 0.9.37 — Children roster, partition Network Data, role-stability counters

Three additions that turn previously-invisible Thread telemetry into queryable surfaces.

- **`GET /v1/children/{eui64}`** — child-attachment roster as seen from a parent router. Sleepy / MTD children only appear in their parent's NeighborTable (they don't broadcast their own diagnostics), so this is the canonical "which end devices have attached to this router right now" view. Returns per-child RSSI/LQI/frame-error/age, ``rx_on_when_idle`` (0 = sleepy), ``registered`` (true iff the child EUI is in the HA registry — false flags a recommissioned/unpaired ghost), and a ``capacity_hint`` / ``is_at_capacity`` pair against the practical 10-child cap. Backed by new `routing.list_children_enriched`.
- **Thread Network Data persistence + `GET /v1/network-data[/{partition_id}]`.** OTBR ingest now best-effort fetches `/node/dataset/active` and `/node/network` and writes the partition's identity (PAN ID, Ext PAN ID, network name, channel, channel mask, mesh-local prefix, active timestamp) plus the leader's Network Data (on-mesh prefixes, external routes, BR Servers, SRP services) to a new `network_data` table keyed by `partition_id`. Two rows = the mesh is partitioned; the surface is meant to make split-brain diagnosable. Endpoint-list returns all known partitions newest-first; single-partition endpoint returns one. Older OTBR builds that don't expose the dataset/network endpoints degrade gracefully — the helper returns `None` and ingest logs at debug.
- **Per-node cluster-53 stability counters.** `_extract_thread_diagnostics` now reads `DetachedRoleCount` (0x000E), `RouterRoleCount` (0x0010), `LeaderRoleCount` (0x0011), `AttachAttemptCount` (0x0012), and `ParentChangeCount` (0x0015) from Thread Network Diagnostics and persists them on the node row via `set_node_diagnostics`. These are monotonic counters since the device last booted, so deltas across two polls tell you "this node spent the interval reattaching" or "this router cycled through leader role twice". **`ChildRoleCount` (spec 0x000F = attribute 15) is deliberately skipped** — the python-matter-server build in use maps attribute 15 to `ExtAddress` rather than the spec counter (see existing `_MATTER_THREAD_DIAG_EXTADDR_SUFFIX = "/53/15"`). Child population is already surfaced via `/v1/children/{eui64}` from the parent's NeighborTable.
- **Schema v10.** Adds five `nodes.*_role_count` columns and a new `network_data` table (partition_id PK, OTBR EUI, descriptive fields, JSON columns for on-mesh prefixes / external routes / services / BR servers, `observed_at` index). `stats()` and `reset_data()` include the new table.
- **Tests: 148/149 passing** (1 deliberate skip). New: `test_set_node_diagnostics_role_counts`, `test_upsert_network_data_roundtrip`, `test_list_children_filters_neighbors`. Schema-version assertions bumped 9 → 10.

## 0.9.36 — Registry-first node model (phantom-creation killed at source)

The `nodes` table is now authoritative for "what devices exist", sourced from the HA device registry (plus the OTBR). Stray EUIs seen in another router's NeighborTable or RouteTable no longer create node rows — they become a `neighbor_known = 0` flag on the link row instead. This kills the phantom problem at its root: dead-link references can't masquerade as devices, and offline / online state stays with HA where it belongs.

- **Schema v9.** Adds `nodes.is_thread` (1 = Thread, 0 = WiFi/other Matter, NULL = unknown) and `links.neighbor_known` (1 = neighbor EUI is in the registry, 0 = stale reference). Two indexes: `idx_nodes_is_thread`, partial `idx_links_stale WHERE neighbor_known = 0`.
- **Storage is UPDATE-only for unknown EUIs.** `bump_last_referenced` and `insert_event` no longer auto-create node rows. Only `upsert_node_metadata` (driven by the HA registry sync + OTBR ingest) inserts rows. Returns count of rows actually touched.
- **`replace_links_for_reporter` computes `neighbor_known` per row** against the current node set; stale references surface immediately.
- **`SQLiteStore.refresh_neighbor_known()`** reconciles every existing link row's flag after a registry sync adds or removes nodes — newly-registered devices flip their stale links to known without waiting for the next reporter poll.
- **`SQLiteStore.list_stale_links()`** returns every link whose neighbor EUI isn't in the registry. These are the troubleshooting bait: a router is forwarding to (or seeing as a neighbor) an EUI no Matter device is registered under — usually a recommissioned device that left a stale router cache, or a failed pairing whose Thread frame counter survived.
- **New `GET /v1/links/stale`** endpoint returning `{count, links}`. `GET /v1/dev/status` gains a `stale_link_count` summary.
- **OTBR ingest stamps `is_thread=True`** alongside `role=border_router`. The HA device registry sync stamps `is_thread=True` on every Thread Matter device it materializes.
- **Tests migrated to the new contract.** `test_insert_event_creates_node` → `test_insert_event_updates_known_node_only` (verifies events to unknown EUIs do NOT create node rows). `test_bump_last_referenced_creates_node` → `test_bump_last_referenced_skips_unknown_and_touches_known`. Schema-version assertions bumped 8 → 9. Tests that previously relied on auto-creation now pre-seed via `upsert_node_metadata`. **145/146 passing** (1 deliberate skip; same as 0.9.35).
- **Deferred to 0.9.37:** retirement of the `phantom` / `unregistered` status enum and the legacy `is_phantom` column. Both remain in place for one release of backwards compatibility — but they are now factually unreachable for any HA-registered device, and the troubleshooting workflow has moved to `/v1/links/stale`.

## 0.9.35 — Scenario fixtures + pipeline integration + property tests

- **Scenario fixtures** (`tests/scenarios/`). JSON-driven mesh shapes drive a parametrized test matrix. Five shipped: single OTBR with three routers, two-partition network split, stale phantom singleton, REED attached as child, and cold-start empty. Adding a new mesh quirk is now a new JSON file, not new Python.
- **Pipeline integration tests** (`tests/integration/test_pipeline_tick.py`, 7 tests). Stage stubs replace the I/O-touching pipeline stages; the tests assert per-stage failure isolation, tick-count accounting, runner-state hygiene, and that downstream stages still run when an upstream one raises.
- **Property-based route walker tests** (`tests/integration/test_routing_property.py`, 5 tests, hypothesis). Random partitions of 2–8 routers with arbitrary next-hop pointers verify the walker: always terminates, returns acyclic chains, starts at the requested source, and ``complete=True`` iff the final hop is the OTBR.
- **Test count: 146** (up from 114; 145 pass, 1 deliberately skipped). `hypothesis>=6.0` added to test extras.
- No runtime behaviour change. Pure test infrastructure: build confidence is now strong enough that an API or routing regression is caught at PR time, not in the field.

## 0.9.34 — API contract tests + Pydantic response schemas

- **Pydantic response contracts** (`api/schemas.py`). Every public `/v1/...` endpoint now has a declared response model: `HealthResponse`, `TopologyResponse`, `RouteWalkResponse`, `NeighborsResponse`, `PartitionsResponse`, `IssuesResponse`, `PhantomsResponse`, `DevStatusResponse`, plus their nested row models. Models use `extra="allow"` so adding fields server-side is non-breaking, but required-field types are now enforced by tests.
- **Contract test suite** (`tests/contract/`, 20 tests). Each endpoint is hit through FastAPI's `TestClient` against a seeded SQLite store and the response is validated against its Pydantic model. Catches every "API and UI disagree" class of bug at build time — e.g., a field rename, a type change, or a dropped key would fail CI instead of silently breaking the dashboard or an MCP consumer.
- **Test count: 114** (up from 94). Existing unit tests untouched.
- No runtime behaviour change — endpoint handlers still return plain `dict[str, object]` for forward compatibility. The schemas are the *contract*, validated in tests; we can opt individual endpoints into FastAPI's `response_model=` later if we want stricter shape enforcement at the wire.

## 0.9.33 — First-class routing + server-side path walk

- **Schema v8: richer link rows.** The `links` table now carries the Matter NeighborTable fields previously dropped — `rx_on_when_idle`, `full_thread_device`, `full_network_data`, `link_frame_counter`, `mle_frame_counter` — plus the RouteTable flags `link_established` and `allocated`, plus a `partition_id` stamp so stale rows from prior partitions are detectable. Every Matter cluster-53 field that matters for routing decisions is now persisted as a first-class column.
- **Server-side route walker** (`pipeline/routing.py`). Walks `node → OTBR` via the RouteTable, with cycle detection, partition checks, and a structured `issues` list (`no_otbr`, `no_route_to_otbr`, `loop_detected`, `unknown_next_hop`, `different_partition`, `max_hops_exceeded`, `self_is_otbr`). Replaces the JS path walk previously done client-side — MCP / AI consumers now see the same hop chain the UI does.
- **New endpoints**:
  - `GET /v1/routes/{eui64}` — full hop chain to the OTBR with per-hop LQI, path cost, link-established state, and any path issues.
  - `GET /v1/neighbors/{eui64}` — enriched NeighborTable + RouteTable rows for one reporter with names resolved and `next_hop_router_id` mapped back to its EUI64.
- **`/v1/dev/status` enrichments** so the dashboard (and any other consumer) doesn't recompute data shape:
  - `otbr_eui64` exposed at the top level (no more JS heuristics).
  - `all_nodes` pre-sorted (phantoms last, then by display name).
  - `node_counts` summary (`{total, online, offline, unregistered, phantom}`).
  - `partitions.summary` human-readable string (`"single partition"`, `"network is split across 2 partitions"`).
  - `pipeline.stages_failed` array pre-computed.
- **`/v1/topology` link enrichment**: every link row now has an `edge_class` (`peer` / `child` / `route` / `other`). Router-router neighbor pairs reported by both ends are deduplicated server-side. The graph renderer just consumes; no more `nodeKind` / symmetric-key logic in JS.
- **Separation of concerns**: data transformation moved out of the dashboard. Every transform that used to happen in JS (OTBR finding, route walking, node sorting, edge deduplication, partition summary, failed-stage filtering) now lives in the API. The UI renders; MCP and AI get parity for free.

## 0.9.32 — Atomic pipeline + live status pill

- **One pipeline, one tick.** Replaced the four independent background loops (otbr-ingest 10s, otbr-rest 60s, matter-discovery 300s, reasoner 120s) with a single atomic tick: `otbr_log_ingest → otbr_rest → matter_discovery → reasoner`. Each stage runs in dependency order, so the reasoner always reads data discovery just wrote. Per-stage failures are isolated; the rest of the tick still runs.
- **Immediate-then-cadence.** The tick fires once at startup (no more 5-minute empty-table window after a restart) and then every `pipeline_interval_seconds` (default 30) after the previous tick *completes* — ticks never overlap.
- **Live UI indicator.** New header pill shows `pipeline: idle / matter_discovery / tick #N · 1.2s / error`. Pulsing blue dot while running, green on success, yellow when a stage failed, red on runner error. Hover for stage timings.
- **New endpoints**: `GET /v1/pipeline/state` (last tick summary, polled every 1s by the dashboard) and `POST /v1/pipeline/run` (force an out-of-band tick). `/v1/dev/status` now embeds `pipeline` for completeness.
- **Config**: `scheduler.pipeline_interval_seconds` (10–600s, default 30). The old per-loop interval knobs remain in config for backwards compat but are no longer wired.
- **Partition list fixes**: (a) dashboard was reading the wrong field name (`node_count` vs API's `member_count`), so every partition rendered "0 nodes" — now shows correct counts; (b) a single node with a stale `partition_id` and no leader no longer registers as a separate partition (always a stale-data artifact, not a real split).
- **HA registry metadata in the UI**: the Thread Nodes table now has an **Area** column, a sub-line under each device name showing manufacturer / model / sw version, and a tooltip on the EUI cell with the HA `device_id`, hw version, and `ha_device_path`. All these fields were already in the API; the dashboard just wasn't surfacing them.

## 0.9.31 — Status enum + graph click-to-trace

- **Node status enum** (replaces the binary `is_phantom` flag as the primary lifecycle signal):
  - `online`       — referenced in the current discovery window (`OFFLINE_AFTER_SECONDS`, default 900s).
  - `offline`      — not referenced recently, OR HA-registered (`device_id` present) regardless of age. **HA-registered nodes never auto-purge.**
  - `unregistered` — observed via mesh but never had a recent reference and no HA `device_id`.
  - `phantom`      — stale beyond `PHANTOM_AFTER_SECONDS` (default 24h) AND not HA-registered. Eligible for purge.
- **Schema v7** (auto-migration): `nodes.status` (NOT NULL DEFAULT 'online'), `nodes.status_changed_at`, indexed on `status`. `is_phantom` retained and mirrored from `status='phantom'` for backwards compat.
- **Atomic recompute**: new `recompute_node_statuses(offline_seconds, phantom_seconds)` evaluates the state machine for every row in a single SQL pass; `status_changed_at` only updates when the value actually flips.
- **Offline retention**: new `purge_expired_nodes(max_offline_seconds)` deletes phantom-state rows and `offline` rows older than the retention window (default 30 days via `OFFLINE_RETENTION_SECONDS`), but never touches HA-registered nodes. Both hooks run at the end of every discovery cycle.
- **API**: `/v1/dev/status all_nodes[*]` and the single-node summary now include `status` (from the new column, falling back to the legacy heuristic for un-recomputed rows) and `status_changed_at`.
- **Graph click-to-trace**: clicking any node in the Network Graph tab highlights its forwarding path to the OTBR by walking `next_hop_to_otbr.eui64` (with cycle detection, depth≤32). Path nodes get a yellow border, the OTBR gets a green border, and non-path nodes/edges fade to 12% opacity. Click the empty canvas to clear. (This was the deferred 0.9.29 visual; rebuilt cleanly on a single style array.)
- **Skipped from earlier plan**: Influx time-series snapshots. The HA InfluxDB add-on isn't deployed in this env; existing `InfluxConfig` + `SQLiteFallbackStore` plumbing stays in place, ready to light up when a token is provisioned. Until then, SQLite remains the only backend and stays a wipe-on-boot cache.

## 0.9.30 — HA registry reconciler + link TTL

- **Persist full HA metadata** on every node: `area_id`, `area_name`, `manufacturer`, `model`, `sw_version`, `hw_version`, `ha_device_path` (deep link to `/config/devices/device/<id>`). Was silently dropping all of these in 0.9.29 — the reconciler collected them but only wrote `friendly_name` + `device_id`.
- **Area registry resolution**: reads `/config/.storage/core.area_registry` once per discover cycle to map `area_id → area_name`. Logs the count so we can spot `/config` mount issues immediately (`area_registry=0` means the addon can't see HA storage).
- **Schema v6** (auto-migration): new columns on `nodes` for the fields above, indexed on `area_id`. Legacy `area` column is mirrored from `area_name` for backwards compat.
- **Don't skip metadata-only registry rows**: 0.9.29 skipped any device without a `friendly_name` *and* a `device_id`. Now we persist anything that has *any* of friendly_name / device_id / area_id / manufacturer / model so the UI can render context even for unnamed devices.
- **Link-row TTL**: new `sweep_stale_links(ttl_seconds)` runs at the end of every discovery cycle (default 900s, configurable via `LINK_TTL_SECONDS`). Reporter rows that aren't refreshed within ~3× the discover interval are evicted. This is the second line of defense after wipe-on-boot — zombie peers can't survive even a single missed restart.
- **API surface**: `/v1/dev/status all_nodes` now returns `area`, `area_id`, `area_name`, `manufacturer`, `model`, `sw_version`, `hw_version`, `ha_device_path` per node.
- **What's still next**: status enum (`online / offline / unregistered / phantom`) with 30-day offline retention; Influx time-series snapshots per ingestion run (the path to graduating SQLite from cache to system-of-record); Graph tab click-to-trace highlighting. Queued for 0.9.31.

## 0.9.29 — The DB is a cache, not a system of record

- **Wipe-on-startup**: `nodes`, `links`, `events`, `issues`, `metadata_cache`, and `ingest_state` are now truncated on every addon start. The Thread fabric and the HA device registry are the sources of truth; SQLite is a live cache of what those sources currently report. Anything that survives a restart but does not come back in the next poll cycle was, by definition, stale.
- **Why**: a year-dead soil-moisture sensor was still appearing as a "peer of router X" because `replace_links_for_reporter` never deletes rows for reporters that go silent, and `bump_last_referenced` kept resurrecting the EUI from those stale rows. Truncate-on-boot eliminates the entire class of zombie-row bugs in one stroke; if a node reappears, it is real.
- **New config**: `reset_db_on_start: bool` (default `true`). Set to `false` to preserve previous DB contents across restarts (debugging only — production should leave the default).
- **New API**: `SQLiteStore.reset_data()` — truncates cache tables, preserves `schema_version`, runs `VACUUM`. Logged at INFO with the row count deleted.
- **What's still next**: HA-registry reconciler fix (every node currently shows `friendly_name: null` in production), persist `area_id`/`manufacturer`/`model`, and Graph-tab click-to-trace next-hop highlighting. Queued for 0.9.30.

## 0.9.28 — Next-hop to OTBR per router (the actual forwarding view)

- **Thread Nodes table**: every router now shows its next forwarding hop on the path to the OTBR, e.g. `→ OTBR via Eve Energy (cost 3)` or `→ OTBR direct (cost 1)` when it is a direct mesh neighbor of the border router. This is what you read first when troubleshooting — it tells you the exact router a packet leaves through, not just who the partition leader is.
- **Schema v5** (auto-migration): `nodes.router_id` and `links.next_hop_router_id` columns added.
- **Discovery**: `_decode_route_table` now keeps non-link-established entries (so multi-hop routes are visible), captures each entry's `RouterId` (destination) and `NextHop` (the router_id to forward through). Each router's own Thread Router ID is auto-detected from its RouteTable self-entry and persisted.
- **OTBR REST**: parses `Rloc16` and persists the OTBR's Router ID (`router_id = rloc16 >> 10`), required to resolve next-hop chains that terminate at the border router.
- **Enrichment**: new `next_hop_to_otbr` field on every node = `{eui64, name, router_id, path_cost, is_direct}`, computed from each router's route-table entry pointing at the OTBR. End devices (SED/FED) implicitly inherit their parent router's path.
- **What's still next**: highlight the next-hop chain in the Graph tab when a node is clicked (visual breadcrumb to OTBR). Queued for 0.9.29.

## 0.9.27 — OTBR ingestion (border router now appears in the table)

- Added `pipeline/otbr_rest.py`: a new scheduled loop (default 60s) that fetches the HA OTBR add-on's REST `/node` endpoint and upserts the Thread Border Router as a first-class node in our store. Captures ExtAddress (EUI64), State (→ routing_role), PartitionId, LeaderRouterId, Weighting, and NumOfRouter.
- Probes multiple candidate base URLs (env override → Supervisor proxy → direct container hostnames) and caches whichever responds, so the same code works across HA OTBR add-on versions. Failures are logged at debug and the loop keeps trying.
- A user-set friendly name on the OTBR row is preserved on subsequent ingest cycles — we only fill in `Thread Border Router` if no name has been set yet.
- New config option `scheduler.otbr_rest_interval_seconds` (default 60, range 15–3600).
- This is the foundation for the next-hop / path-to-OTBR view: once the OTBR is a known node, every router's existing RouteTable entries can be resolved to a named target, and we can highlight `from → next-hop → … → OTBR` traversals in the graph. Route-table aggregation via `/diagnostics` is queued for 0.9.28.

## 0.9.26 — Leader is control-plane only (label clarified)

- The Thread partition **Leader** is a control-plane coordinator (assigns Router IDs, maintains Network Data) — it is **not** a forwarding hop. Packets do not have to traverse the Leader; each router picks its own next-hop per destination via its RouteTable.
- Relabeled the role-column caption:
  - Router rows: `partition N · K peers · partition leader: X (control-plane)` (was `leader: X`).
  - Leader row: `partition N · K router peers · control-plane only`.
- Updated the Role legend on the Network tab to call out that Leader is control-plane only and packets don't have to go through it.
- Next-hop / path-to-OTBR visualization is queued for after OTBR ingestion lands (so we can resolve actual forwarding paths from each router's RouteTable to the border router).

## 0.9.25 — Router↔router mesh backbone visualized, peer names in table

- **Graph tab** now distinguishes the three kinds of edges so router↔router peer links (the mesh backbone) are visually distinct from router→child attachments and multi-hop route-table entries:
  - solid thick = router↔router peer (deduped — each pair is one line, no arrows)
  - dashed thin = router→child (arrow toward child)
  - dotted = route_table multi-hop entry
  Routers are also rendered larger and the Leader gets a highlighted border so the mesh backbone pops at a glance.
- **Thread Nodes table** now lists peer router names beneath each router / leader row ("peers: A, B, C"). Backend exposes `router_peers: [{eui64, name}]` on every node.
- This is what answers “who is the Leader connected to in order to reach the OTBR / each other?” — the chain of router↔router edges in the Graph tab.

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
