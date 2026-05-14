# Thread Observability Add-on

Ingests Thread/Matter logs, enriches with Home Assistant metadata, and exposes the result over MCP and a small HTTP surface for AI-assisted triage.

**Current version**: 0.11.36 / schema v19 / 41 MCP tools.

## What it does

- Two-process service model in one container:
  - Core API on port 8099 (`/health`, `/v1/health/snapshot`, `/v1/issues/active`, `/v1/topology`)
  - MCP server on port 8100 (`GET /mcp/tools`, `GET /mcp/resources`, `POST /mcp`, `GET /mcp/sse`, `POST /mcp/stream`)
- Each pipeline tick:
  - Discovers Thread nodes via OTBR / Matter Server
  - Correlates EUI-64 with HA device registry
  - Records per-node MAC/MLE counter samples (schema v19, Phase 4)
  - Runs deterministic reasoner rules and persists open issues
  - Records a pipeline-tick row for temporal honesty (`meta.pipeline_tick`)
- Retention prunes counter samples older than `full_resolution_days` (default 3) into 5-minute averaged buckets up to `sampled_archive_days` (default 14).

## MCP surface

All read tools return a `{data, meta}` envelope. See [../../documentation/06-mcp-tools-reference.md](../../documentation/06-mcp-tools-reference.md) for the full, auto-generated catalog.

## Connecting an AI agent

Use the Home Assistant MCP Client integration to expose the Thread Observability tool catalog to your chosen conversation agent.

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

## Repository notes

- Legacy node-shaping reference exports now live under `../../samples/addon/`. They are retained for offline inspection only and are not runtime inputs for the add-on.
- The ad hoc OTBR parser smoke helper lives at `../../scripts/test_real_logs.py` rather than the repository root.
