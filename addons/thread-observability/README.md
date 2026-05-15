# Thread Mesh Detective Add-on

Ingests Thread/Matter logs, enriches with Home Assistant metadata, and exposes the result over MCP and a small HTTP surface for AI-assisted triage.

**Current version**: 0.11.53 / schema v19 / 41 MCP tools.

## Branding and compatibility

Thread Mesh Detective is the product name shown in Home Assistant UI surfaces (add-on name, ingress dashboard title, and API docs).
To preserve upgrade continuity, the add-on slug remains `thread-observability` and internal module paths remain `thread_observability`.

## What it does

- Two-process service model in one container:
  - Core API on port 8099 (`/health`, `/v1/health/snapshot`, `/v1/issues/active`, `/v1/topology`)
  - MCP server on port 8100 (`GET /mcp/tools`, `GET /mcp/resources`, `POST /mcp`, `GET /mcp/sse`, `POST /mcp/stream`)
- Each pipeline tick:
  - Discovers Thread nodes via OTBR / Matter Server
  - Correlates EUI-64 with HA device registry
  - Records per-node MAC/MLE counter samples (schema v19, Phase 4)
  - Recomputes health/topology state and records a pipeline-tick row for temporal honesty (`meta.pipeline_tick`)
  - Calls the paused issue-reasoner shim, which closes any residual legacy issues and returns a paused summary until the rule redesign tracked by GitHub issue #5 lands
- Retention prunes counter samples older than `full_resolution_days` (default 3) into 5-minute averaged buckets up to `sampled_archive_days` (default 14).

## MCP surface

All read tools return a `{data, meta}` envelope. See [../../documentation/06-mcp-tools-reference.md](../../documentation/06-mcp-tools-reference.md) for the full, auto-generated catalog.

## Connecting an AI agent

Use the Home Assistant MCP Client integration to expose the Thread Mesh Detective tool catalog to your chosen conversation agent.

1. Follow [../../documentation/10-ha-mcp-client-setup.md](../../documentation/10-ha-mcp-client-setup.md).
2. In Home Assistant, add the MCP Client integration and use `http://9e5048e8-thread-observability:8100/mcp/sse` as the server URL.
3. Pick the conversation agent you want Assist and the dashboard chat panel to use.
4. Try one of the starter prompts from the setup guide or the dashboard chat card.

First call for any new triage session:

```
POST http://<host>:8100/mcp/call/start_triage
Content-Type: application/json

{"arguments": {}}
```

Returns environment + health + active issues + up to 3 `recommended_next` tool calls.

## Local development

1. Build the add-on image with Home Assistant add-on tooling, or push to a git repo configured as an HA add-on repository.
2. Install from the repository URL in Home Assistant.
3. Configure options in add-on settings (`ha_admin_token` long-lived access token, optional retention overrides).
4. Run tests: `cd app && PYTHONPATH=src pytest -q`.

For API-surface regression without a Home Assistant deployment, run `PYTHONPATH=app/src python ../../scripts/api_surface_smoke.py` from `addons/thread-observability`.

## Repository notes

- Legacy node-shaping reference exports now live under `../../samples/addon/`. They are retained for offline inspection only and are not runtime inputs for the add-on.
- The ad hoc OTBR parser smoke helper lives at `../../scripts/test_real_logs.py` rather than the repository root.
- `app/src/thread_observability/pipeline/reasoner.py` intentionally retains the pre-redesign rule body as reference code while the active runtime keeps issue detection paused pending GitHub issue #5.

## Ingress dashboard styling notes

- The dashboard now prefers Home Assistant theme variables (for example `--primary-background-color`, `--ha-card-background`, `--primary-text-color`, `--secondary-text-color`, and `--accent-color`) so ingress surfaces track active HA light/dark themes.
- We intentionally keep product-specific diagnostic colors for Thread role classes (Leader/Router/REED/FED/SED/phantom) and graph/risk overlays because those hues encode operational meaning across table pills, graph legend, and topology rendering.

### Manual validation: chat markdown tables

The dashboard chat panel renders assistant replies as a sanitized subset of Markdown (tables, lists, emphasis, inline code, fenced code blocks).

To validate table rendering:
1. Open the ingress dashboard, go to the Chat panel, and ask the agent to “Summarize the last health snapshot as a Markdown table”.
2. Confirm the assistant response shows an actual HTML table (not raw pipe characters) and the table scrolls horizontally on narrow/mobile widths.
3. Click “Copy” on the message to ensure the raw assistant text is copied to the clipboard.
