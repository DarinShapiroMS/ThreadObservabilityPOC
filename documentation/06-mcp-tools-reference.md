# MCP Tools Reference

This document is generated from `thread_observability.api.mcp_tools.TOOL_DEFS` and `RESOURCE_DEFS`.
Generated at: `2026-05-16T06:50:27+00:00`
Tool count: `43`
Resource count: `1`

Shared background resource: [glossary.md](glossary.md)

## Resources

### `glossary`

- URI: `thread-observability://glossary`
- MIME type: `text/markdown`
- Description: Shared background for Thread, Matter, and Home Assistant terms used across the MCP tool catalog, including spec links and field meanings such as RLOC16, partition_id, LQI, and MAC/MLE counters.

## Tools

### `get_mesh_state`

Use when: starting a triage session or answering 'what does the mesh look like right now?'. Returns the live Thread mesh: nodes + links + partition_id, computed deterministically from the latest retained Thread events and most-recent Matter discovery tick. Phantom nodes are excluded by default. Returns: {nodes:[{eui64, role, partition_id, parent_eui64, last_rssi, last_lqi, status, ...}], links:[...], partition_id, computed_at, node_count, link_count}. Caveats: derived from the latest persisted pipeline state. Check meta.cache_age_s on the response; if stale, call ingest_now to force a refresh.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `freshness_minutes` | `integer` | no | `60` | Window (minutes) for inferring current parent links. Default 60. |
| `include_phantoms` | `boolean` | no | `False` | If true, include phantom (stale-reference) nodes in the snapshot. Default false. |

### `list_active_issues`

Return all currently-open Thread network issues computed by deterministic rules. Each issue includes the affected EUI64 (or null for mesh-wide issues), `first_seen_at`, `last_seen_at`, a severity that reflects actionability × freshness, and an evidence payload that includes the EUIs involved and the observation that triggered it. Current rule taxonomy: `real_partition_split`, `dead_link_reference`, `route_to_otbr_unreachable`.

Arguments: none

### `get_health_snapshot`

Return current health snapshot: node counts by status (healthy / stale / offline), active issue counts, and data freshness age.

Arguments: none

### `close_issue`

Manually close an active issue by id.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `id` | `integer` | yes | `` | Open issue id to close. |

### `get_recent_logs`

Return recent add-on log lines from the add-on's internal file logger.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `lines` | `integer` | no | `100` | Number of log lines to return (default 100, max 200). |

### `ha_get_addon_state`

Return Supervisor's view of this add-on: install state, current version, latest available version, boot/watchdog flags, ingress URL, and raw info. Use this from VS Code to verify a deploy without opening the HA UI.

Arguments: none

### `ha_get_addon_logs`

Return the tail of the Supervisor container log for an add-on. Defaults to this add-on (self) when ``slug`` is omitted; pass a Supervisor add-on slug (e.g. ``core_openthread_border_router``, ``core_matter_server``) to fetch that add-on's container log instead. Captures s6-overlay/startup output that the in-process Python logger misses. Use this to diagnose crash loops, boot failures, or correlate OTBR/Matter server events with Thread mesh state.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `lines` | `integer` | no | `200` | Lines to return (default 200, max 1000). |
| `slug` | `string` | no | `` | Supervisor add-on slug. Omit (or null) for this add-on's own logs. |

### `ha_get_supervisor_logs`

Return the tail of the Home Assistant Supervisor's own log. Useful for diagnosing why Supervisor rejected or killed the add-on (permissions, port conflicts, AppArmor, image pull failures).

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `lines` | `integer` | no | `200` | Lines to return (default 200, max 1000). |

### `ha_restart_addon`

Ask Supervisor to restart this add-on (fast; no image rebuild). Use after config or option changes to verify behaviour without a full deploy.

Arguments: none

### `ha_rebuild_addon`

Ask Supervisor to rebuild this add-on from its repository source, then restart. Use after pushing a new commit so VS Code can complete the change→deploy→observe loop without manual uninstall/reinstall.

Arguments: none

### `ha_check_for_update`

Force Supervisor to re-scan add-on repositories, then report current vs latest version. Returns {current, latest, update_available, auto_update, state}. Use right after pushing a new version bump to avoid waiting for Supervisor's periodic poll.

Arguments: none

### `ha_update_addon`

Update this add-on to the latest version available in the store (equivalent to clicking 'Update' in the HA UI). Supervisor pulls the new image / rebuilds from source and restarts. Resolves the store-side slug from /store/addons (NOT /addons/self/info, whose slug carries a repo-hash prefix that the store endpoint rejects on some installs, silently clearing the install). Pass dry_run=true to verify the resolved endpoint without dispatching the update. Pair with ha_check_for_update first.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `dry_run` | `boolean` | no | `` | If true, resolve the slug and report what endpoint would be called, without POSTing. Default false. |

### `ha_set_auto_update`

Enable or disable Supervisor's auto-update flag for this add-on.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `enabled` | `boolean` | yes | `` | True to enable, false to disable. |

### `ha_reinstall_addon`

Uninstall then reinstall this add-on from the store. Destructive: clears the add-on container and terminates the MCP process making the call (the HTTP response will be cut short). Treat connection-reset as expected success and poll ha_get_addon_state afterwards.

Arguments: none

### `list_thread_datasets`

Return the Thread Border Router credential datasets known to Home Assistant (network_name, extended_pan_id, channel, source, preferred). Pair with get_node_metadata or analyze_node to determine whether a node reporting an unexpected extended_pan_id is on a stale Thread dataset. Cached for 5 minutes.

Arguments: none

### `get_storage_stats`

Return storage stats for retained network data (schema version, file size, row counts, oldest/newest event timestamps) plus the active time-series backend.

Arguments: none

### `get_chat_stats`

Use when: reviewing dashboard chat usage and grounding behavior without inspecting raw messages. Returns aggregate turn counts, latency, tool-call counts, error breakdown, and a small recent-turn summary. Read-only.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `since` | `string` | no | `` | ISO-8601 lower bound; default all retained chat telemetry. |

### `query_history`

Tier 4 unified timeline. Return a single newest-first stream that merges canonical events, issue open/close lifecycle, and observer (addon/OTBR/Matter Server) outage windows over a time range. Each row is normalized to {ts, source, kind, eui64?, severity?, details, ref_id} so an AI consultant can correlate Thread-side, issue-side and observer-side activity in one round-trip. Filter by eui64, kind list, or source list. Use this for chronology questions like what happened when, not for per-link RSSI/LQI trends or structural topology diffs. Prefer get_signal_series for per-node signal over time, get_node_link_signal_history for adjacent-link quality changes, and diff_topology_history for before/after topology structure.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `since` | `string` | yes | `` | ISO-8601 lower bound (inclusive). Required. |
| `until` | `string` | no | `` | ISO-8601 upper bound (inclusive). Defaults to now. |
| `eui64` | `string` | no | `` | Optional EUI-64 to limit the merged timeline to one node. |
| `kinds` | `array` | no | `` | Optional kind allow-list. Examples: ['attach','parent_change'], ['issue.opened','issue.closed'], ['observer.outage','observer.outage.ended']. |
| `sources` | `array` | no | `` | Optional source allow-list. Defaults to all three. |
| `limit` | `integer` | no | `500` | Maximum rows to return, newest first. Default 500. |

### `get_topology_history_entry`

Tier 4. Return a persisted topology snapshot row. Pass ``snapshot_id`` to fetch one by id, or ``at`` (ISO-8601) to fetch the most-recent snapshot captured on or before that time. With no arguments, returns the newest snapshot.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `snapshot_id` | `integer` | no | `` | Exact topology snapshot id to fetch. |
| `at` | `string` | no | `` | ISO-8601 timestamp |

### `list_topology_history`

Tier 4. List topology snapshot summaries (id, captured_at, hash, partition_id, node_count, link_count) newest-first. Snapshot bodies are NOT returned — use ``get_topology_history_entry`` or ``diff_topology_history`` to drill in.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `since` | `string` | no | `` | ISO-8601 lower bound |
| `until` | `string` | no | `` | ISO-8601 upper bound |
| `limit` | `integer` | no | `100` | Maximum snapshot summaries to return. Default 100. |

### `diff_topology_history`

Tier 4. Return a structured diff between two topology snapshots: added/removed nodes, per-node role/partition/parent transitions, and added/removed links. ``snapshot_id_a`` is the older / baseline, ``snapshot_id_b`` is the newer / candidate. Use this for structural network-change questions, not to claim that signal quality improved or degraded. For RSSI/LQI evidence use get_signal_series or get_node_link_signal_history instead.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `snapshot_id_a` | `integer` | yes | `` | Older or baseline snapshot id. |
| `snapshot_id_b` | `integer` | yes | `` | Newer or comparison snapshot id. |

### `list_playbooks`

Tier 4. Return summaries (id, title, applies_to) of every Thread/Matter playbook in the bundled corpus. Use ``lookup_playbook`` to fetch full entries.

Arguments: none

### `lookup_playbook`

Tier 4. Return playbook entries matching one of: an exact ``playbook_id``; an issue ``kind`` (returns every playbook whose applies_to includes the kind); or a free-text ``query`` (case-insensitive substring across id/title/summary). Each entry includes summary, evidence_to_collect, remediation_steps, references.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `playbook_id` | `string` | no | `` | Exact playbook id to fetch. |
| `kind` | `string` | no | `` | Issue kind to match against a playbook's applies_to list. |
| `query` | `string` | no | `` | Case-insensitive free-text search across playbook id, title, and summary. |

### `analyze_node`

Use when: drilling into a single suspected-bad EUI-64. One-call structured payload: node metadata, parent + neighbors, open issues, recent closed issues, unified timeline (events + issue lifecycle + observer events), per-node baselines (parent_change rate this period vs. previous, status_change count), and full playbook entries matching the union of issue kinds. Prefer over composing list_all_nodes + list_active_issues + query_history + lookup_playbook by hand. Returns: rich JSON keyed by section. Caveats: timeline_hours and baseline_days are capped; very large windows truncate.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `eui64` | `string` | yes | `` | Target node EUI-64 to analyze. |
| `timeline_hours` | `integer` | no | `24` | How many recent hours of unified timeline to include. Default 24. |
| `baseline_days` | `integer` | no | `7` | How many historical days to use for baseline rate comparisons. Default 7. |

### `get_config`

Return the typed add-on configuration (merged from /data/options.json plus env overrides).

Arguments: none

### `get_timeseries_health`

Probe the active time-series backend and return status.

Arguments: none

### `list_otbr_candidates`

Return Supervisor add-ons that look like OpenThread Border Router hosts (slug or name contains 'openthread', 'otbr', or 'silabs-multiprotocol'). Use to discover the slug to feed into set_otbr_slug.

Arguments: none

### `set_otbr_slug`

Set the OTBR add-on slug used by the background ingestion loop. Resets the cursor so the next poll will re-scan all currently-available log lines.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `slug` | `string` | yes | `` | Supervisor add-on slug to treat as the OTBR log source. |

### `ingest_now`

Run one OTBR ingestion pass synchronously: fetch logs from Supervisor, parse new lines, insert canonical events. Returns line/event counts.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `slug` | `string` | no | `` | Optional slug override. |

### `get_ingest_state`

Return the current OTBR ingestion state: configured slug, lines processed, events inserted, last event timestamp, last run timestamp, last error.

Arguments: none

### `list_all_nodes`

Use when: enumerating every known Thread node (including phantoms) or building a device-by-device inventory. Returns: {nodes:[{eui64, friendly_name, role, area, device_id, status, first_seen, last_seen, last_rssi, last_lqi, ...}], count}. Ordered most-recently-seen first. Use ``status_filter='phantom'`` to drill into stale-reference cleanup candidates. Caveats: sourced from the latest persisted pipeline state; check meta.cache_age_s.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `status_filter` | `string` | no | `` | Restrict to nodes whose status matches this value. |

### `sync_ha_devices`

Use when: HA shows a Thread device the addon hasn't seen yet, or after a fresh commission, or when phantom counts look wrong. Queries the HA device registry for Thread/Zigbee devices and correlates IEEE addresses with extracted EUI64 nodes. Auto-populates friendly_name and device_id for matching nodes. Returns: {matched, updated, ...}. Caveats: This is a mutation (writes friendly_name/device_id back to SQLite); not a read tool.

Arguments: none

### `start_triage`

Use when: starting any new investigation, or as the first call in a session. Returns the consolidated environment (addon/HA/OTBR/Matter/network/pipeline versions) plus the health snapshot plus active issues plus a `recommended_next` list of up to 3 follow-up tool calls chosen from the catalog. Returns: {as_of, environment, health, active_issues_count, active_issues[<=10], recommended_next[<=3]}. Caveats: snapshot from SQLite cache; refresh by waiting one pipeline tick.

Arguments: none

### `get_environment`

Use when: you need versions/identity of every relevant component in one shot — addon version, HA Core version, Supervisor version, OTBR add-on state, Matter Server add-on state, Thread network identity (name/pan_id/channel/leader), and pipeline runner state. Returns: {addon, home_assistant, otbr, matter_server, network, pipeline}. Caveats: Supervisor calls may fail outside the HA container; those sections fall back to `{error: ...}`.

Arguments: none

### `get_pipeline_health`

Use when: data looks stale, the dashboard is empty, or the model needs to know whether the pipeline is actually running. Returns the last N pipeline ticks (newest first) plus a summary including consecutive_failed_ticks, stages_currently_failing, avg_duration_seconds, and the current runner state. Returns: {summary: {...}, recent_ticks: [...]}. Caveats: only ticks recorded in schema v18+ are visible; backfill is not retroactive.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `limit` | `integer` | no | `20` | Maximum recent pipeline ticks to return. Default 20. |

### `get_counter_series`

Use when: investigating whether a node's MAC/MLE counters are climbing (tx_retry, tx_err_cca, parent_change, attach_attempt). Returns the time-series of selected counter values for one node over [since, until], plus per-counter deltas (last - first). Detects counter resets (re-attach) and reports them explicitly instead of misreading them as a huge negative spike. Returns: {eui64, since, until, resolution, series: [{observed_at, counters}, ...], deltas: {<name>: {delta, reset_detected, first, last}}}. Caveats: requires Phase 4 schema (v19+); samples only exist for ticks recorded after upgrade.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `eui64` | `string` | yes | `` | Target node EUI-64 whose counter samples should be returned. |
| `counter_names` | `array` | no | `` | Optional subset of counter names to include; omit for the default diagnostic set. |
| `since` | `string` | no | `` | ISO-8601; default 6h ago |
| `until` | `string` | no | `` | ISO-8601; default now |
| `resolution` | `string` | no | `raw` | Return raw stored samples or a 5-minute rollup. |

### `compare_node_counters`

Use when: a node looks unhealthy and you want to know whether a peer on the same partition is degrading the same way. Returns counter series for two nodes side-by-side over the same window, plus a peer_summary flagging counters where one side's delta is at least 2x the other. Returns: {a: {series, deltas}, b: {series, deltas}, peer_summary: {flagged, flagged_count}}. Caveats: requires Phase 4 schema (v19+); use list_all_nodes to find a healthy peer first.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `eui64_a` | `string` | yes | `` | First node EUI-64 to compare. |
| `eui64_b` | `string` | yes | `` | Second node EUI-64 to compare against the first. |
| `counter_names` | `array` | no | `` | Optional subset of counters to compare; omit for the default diagnostic set. |
| `since` | `string` | no | `` | ISO-8601 lower bound for both series; default 6h ago. |
| `until` | `string` | no | `` | ISO-8601 upper bound for both series; default now. |
| `resolution` | `string` | no | `raw` | Return raw samples or 5-minute rollups for both nodes. |

### `get_signal_series`

Use when: you need before/after per-device signal evidence over time, such as whether a node's RSSI or LQI got better or worse across a troubleshooting window. Returns event-backed RSSI/LQI samples for one node over [since, until], plus summary metrics (first, last, delta, min, max, avg). Use this for one node's own observed signal samples, not for peer-by-peer adjacent-link comparison. Caveats: event-driven telemetry only; sparse series mean the backend did not observe signal-bearing events in that window. If the question is which peer or link improved, degraded, appeared, or disappeared over time, prefer get_node_link_signal_history.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `eui64` | `string` | yes | `` | Target node EUI-64 whose signal series should be returned. |
| `since` | `string` | no | `` | ISO-8601 lower bound; default 24h ago. |
| `until` | `string` | no | `` | ISO-8601 upper bound; default now. |
| `resolution` | `string` | no | `raw` | Return raw event samples or 5-minute averages. |

### `get_node_link_signal_history`

Use when: you need retained historical link-by-link signal changes for one node across pipeline observations. Returns adjacent-link history over [since, until], including added/changed/heartbeat/removed samples and per-link RSSI/LQI summaries. Prefer this for network-change questions about which links or peers improved or degraded over time. Use this instead of get_signal_series when the question is about adjacent peers, link appearance/disappearance, or per-link quality change rather than one node's event-backed signal samples.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `eui64` | `string` | yes | `` | Target node EUI-64 whose adjacent-link history should be returned. |
| `since` | `string` | no | `` | ISO-8601 lower bound; default 24h ago. |
| `until` | `string` | no | `` | ISO-8601 upper bound; default now. |
| `peer_eui64` | `string` | no | `` | Optional peer EUI-64 to limit history to one adjacent link. |
| `source` | `string` | no | `` | Optional source filter. |
| `limit` | `integer` | no | `5000` | Maximum historical samples to scan. |

### `get_assessment_state`

Use when: you need to know whether Background Diagnostics is currently scheduled, when the next assessment will run, and how much of today's call budget has been used. Returns the live scheduler snapshot (state, next_assessment_at, budget). Read-only.

Arguments: none

### `list_assessment_findings`

Use when: surfacing or reviewing AI-flagged conditions on the Thread mesh. Returns finding rows (headline, severity, confidence, evidence, suggested_starter_prompt, node_eui64) for the requested state (default: open). Read-only.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `state` | `string` | no | `open` | Which finding state bucket to return. Default open. |
| `limit` | `integer` | no | `50` | Maximum findings to return. Default 50. |

### `mark_finding_outcome`

Use when: the user (or a downstream agent) wants to confirm whether an AI-surfaced finding was actionable. Records an outcome (resolved / wrong / ignored_dismissed) for the finding and updates the finding's state. Powers the precision metrics returned by ``get_assessment_quality``.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `finding_id` | `string` | yes | `` | Assessment finding id to update. |
| `outcome` | `string` | yes | `` | Outcome to record for the finding. |
| `notes` | `string` | no | `` | Optional operator notes explaining why the outcome was chosen. |

### `get_assessment_quality`

Use when: reviewing how the Background Diagnostics AI has been performing — precision estimate, outcome breakdown, and any signal types whose false-positive rate looks high. Read-only.

| Argument | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `since` | `string` | no | `` | ISO-8601; default 7d ago |

