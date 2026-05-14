# Work Buckets

This document groups the active GitHub backlog into delivery buckets that can be executed with minimal cross-stream contention.

## Bucket 1: Release Hardening and Operator UX

Goal: close the loop on issues that directly affect what an operator sees in the add-on today.

Active issues:
- #82 Diagnostics chat suggests nonexistent UI actions
- #92 Graph UX: show node/link metadata on hover

Recently shipped and expected to be closed:
- #80 Graph area filter does not populate with real Home Assistant areas
- #81 Graph inspector and side panels fail to update on node selection
- #83 Filter nodes table by Network Partition
- #84 Nodes table identity hover/popover
- #85 Graph grouping by Area
- #86 Graph control UX and 10s auto-refresh fix

Exit criteria:
- Chat guidance only references real UI affordances.
- Graph and network views expose enough inline metadata that operators do not need to guess what they are looking at.
- No recently fixed UX issues remain open unless there is a verified residual defect.

## Bucket 2: Proactive Monitoring and Home Assistant Surfacing

Goal: turn background diagnostics from a passive side panel into a system that surfaces actionable issues.

Issues:
- #21 HA integration: device + entities + Repairs + events + blueprint

Completed foundation:
- #18 Background Diagnostics adaptive scheduler (closed)

Exit criteria:
- Findings can surface through Home Assistant entities, Repairs, and events.
- Operators can trigger a manual assessment without leaving the normal HA workflow.
- The dashboard clearly explains why a finding is being surfaced now.

## Bucket 3: Core AI and HA Integration

Goal: finish the platform integration layer that lets HA agents and the dashboard share one tool-backed conversation surface.

Issues:
- #5 Redesign issue definitions (tracking)
- #6 Agentic AI chat integration sprint
- #7 MCP: add SSE / Streamable-HTTP transport for HA MCP-Client compatibility

Linked follow-up:
- #82 belongs under this bucket because it is a chat-grounding defect.

Exit criteria:
- HA-native and in-dashboard AI experiences share the same tool surface.
- Transport, prompt context, and chat-grounding behavior are stable enough for routine operator use.

## Bucket 4: Maintainability and Internal Shape Cleanup

Goal: reduce structural drift so future feature work does not keep paying the same complexity tax.

Issues:
- #67 Shared datetime/coercion helpers
- #69 Split mcp_tools catalog/dispatch/transport
- #71 Decompose http_api routes/helpers
- #73 Modularize direct_chat orchestration and tests
- #75 Cleanup stale rules/docs/artifacts
- #78 Maintainability sprint epic
- #91 Unify Node Inventory Logic between topology.py and nodes.py

Tracking note:
- #91 is the preferred canonical issue for the node inventory unification work.
- #87 is a duplicate candidate if it remains open with less detail than #91.

Exit criteria:
- Major backend surfaces have clear ownership boundaries.
- Node identity/topology shaping uses one canonical internal representation.
- Cleanup work is tied back to a parent maintainability track instead of floating independently.

## Bucket 5: Spatial Diagnostics and RF Intelligence

Goal: add physically grounded diagnostics that still degrade gracefully when no floorplan integration exists.

Issues:
- #93 Spatial model and authored adjacency
- #94 Integrate ha-floorplan as optional spatial anchor source
- #95 Manual layout and area adjacency authoring
- #96 Spatial inference from anchors and radio metrics
- #97 RF intensity and trouble-spot scoring
- #98 Floorplan overlays and radio intensity UI
- #99 Spatial diagnostics UX and remediation cues
- #100 Validation pack for spatial diagnostics
- #101 Floorplan/RF epic

Exit criteria:
- The product works without a floorplan addon, but gains richer placement and heatmap fidelity when ha-floorplan is present.
- Multi-floor handling and grounded remediation guidance are first-class requirements.

## Recommended Execution Order

1. Bucket 1: keep the current operator experience credible.
2. Bucket 2: make findings show up proactively.
3. Bucket 3: finish the HA/AI integration path.
4. Bucket 4: pay down structural debt that slows the first three buckets.
5. Bucket 5: build the larger spatial diagnostics program on top of the stabilized platform.
