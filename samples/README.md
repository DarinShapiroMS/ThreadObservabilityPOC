# Data Service Samples

Captured JSON returns from each upstream data service the addon consumes.
Useful for offline schema inspection and parser test fixtures.

Sources captured (one folder each):

- `addon/` — legacy Thread Mesh Detective add-on reference exports retained for offline inspection of node-shaping output
- `chat/` — reusable live-chat smoke matrices for add-on acceptance runs
	Includes `live_browser_smoke.js`, a browser-authenticated smoke helper for HA ingress sessions where terminal requests return `401 Unauthorized`. The live smoke matrix also supports transcript-level assertions against persisted chat sessions, including assistant completions and reviewer/audit prompts.
- `mcp/` — Thread Mesh Detective MCP tool responses (`http://192.168.68.90:8100/mcp`)
- `matter_server/` — `python-matter-server` WebSocket API responses (`ws://core-matter-server:5580/ws`)
- `otbr/` — OpenThread Border Router REST API (`/api/topology`, etc.)
- `ha/` — Home Assistant Supervisor / registry data

Each file is named `<tool_or_endpoint>__<timestamp>.json` so multiple
captures over time can coexist. Files with `redacted` in the name have
had IEEE addresses / tokens / SSIDs scrubbed for sharing.

Capture date is in each filename and inside each file under `_captured_at`.
