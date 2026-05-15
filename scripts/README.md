# Script Helpers

This folder contains local developer helpers and repository-maintenance scripts.
They are not imported by the add-on at runtime.

- `assess.ps1` runs focused assessment checks against a live environment.
- `api_surface_smoke.py` runs the HTTP API in-process with FastAPI `TestClient`, seeded SQLite state, and local stubs so the API surface can be regression-tested without deploying to Home Assistant. It covers health, dev status, topology/history, partitions, routing, stale links, network data, assessment, chat telemetry, and the prompt corpus through `/v1/chat/turn`.

Run it locally with `PYTHONPATH=addons/thread-observability/app/src python scripts/api_surface_smoke.py`.
- `chat-smoke.ps1` exercises chat flows against the add-on, including persisted transcript inspection through `/v1/chat/transcript/{conversation_id}` when transcript persistence is enabled.
- `dashboard-loop.ps1` repeats dashboard-oriented checks during live validation.
- `generate_mcp_reference.py` regenerates the MCP reference documentation from the live tool registry.
- `test_real_logs.py` is an ad hoc OTBR parser smoke helper for quickly checking a few real log lines outside the automated test suite.