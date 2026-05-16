# HA MCP Client Setup

This guide takes a Home Assistant user from a fresh Thread Mesh Detective install (formerly Thread Observability) to a working Assist agent with Thread tools.

## Prerequisites

- Home Assistant 2025.x or newer.
- Thread Mesh Detective installed and running.
- At least one configured Home Assistant conversation agent.
- The MCP server reachable at the add-on hostname on port 8100 (no host port mapping required).

## MCP URL

Use this MCP server URL in the Home Assistant MCP Client integration:

`http://9e5048e8-thread-observability:8100/mcp/sse`

The add-on also exposes `POST /mcp/stream` for streamable HTTP clients, but the setup flow should start with the SSE URL above.

## Five-minute setup flow

1. Open Home Assistant.
2. Go to **Settings → Devices & services**.
3. Choose **Add integration**.
4. Search for **MCP Client**.
5. When prompted for the server URL, paste `http://9e5048e8-thread-observability:8100/mcp/sse`.
6. Choose the conversation agent that should receive the Thread Mesh Detective tools.
7. Finish the integration flow and wait for the agent to refresh.
8. Open the Thread Mesh Detective dashboard and reload it once.
9. Open the **Chat** panel. If the integration is visible, the setup card disappears and the chat composer becomes active.

## What the dashboard shows before setup

If no usable Thread-enabled agent is detected, the chat panel shows a **Connect an AI agent** card instead of leaving the chat surface looking broken.

That card provides:

- A copy button for the MCP URL.
- A deep link to the Home Assistant integrations page.
- Starter prompts you can try once the integration is complete.

## Starter prompts

- Give me a quick health summary of this Thread mesh.
- How many partitions do I have right now, and why?
- Which nodes look stale or offline right now?
- Explain the riskiest links or path bottlenecks in the current graph.
- What changed most recently in the mesh history?

## Quick verification

After the integration is added, try these checks:

1. In the dashboard chat panel, ask: `How many partitions do I have right now, and why?`
2. In Assist, ask: `Any stale nodes?`
3. Confirm that the answer references current mesh data instead of generic assistant text.

## Notes

- The add-on still supports the legacy `POST /mcp` JSON-RPC route for VS Code MCP clients.
- If you want to connect from outside Home Assistant (for example, a LAN VS Code MCP client), map port 8100 in the add-on Network settings.
- If the dashboard still shows the setup card after configuration, reload the dashboard and confirm that the selected Home Assistant conversation agent is the one attached to the MCP Client integration.
- Direct chat in the add-on remains an alternative path if you do not want to route through Home Assistant's MCP Client integration.
