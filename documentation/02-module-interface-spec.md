# Module Interface Specification

## Overview

This document defines the contract that each reasoning module (Thread v1, Energy v2, Climate v3, etc.) must implement to integrate with the HA Reasoning Platform core.

## Runtime Chat Endpoints

The add-on also exposes a lightweight dashboard chat endpoint that can use either:

- a Home Assistant conversation agent via `conversation.process`, or
- a directly-configured model provider for operators who do not want to wire an
    Assist agent first.

### `GET /v1/chat/agents`

Returns the conversation agents Home Assistant currently exposes, preferring HA's `conversation/agent/list` WebSocket command and falling back to scanning `conversation.*` entities. When direct model chat is configured, the response also includes a synthetic `direct:<provider>` agent row plus `default_backend` / `default_label` to describe what happens when the UI leaves `agent_id` unset.

### `POST /v1/chat/turn`

Request:

```json
{
    "message": "why are there two partitions right now?",
    "conversation_id": "optional-existing-thread",
    "agent_id": "optional-agent-id",
    "page_context": {"page": "dashboard"},
    "streaming": false
}
```

Response:

```json
{
    "conversation_id": "optional-existing-thread",
    "agent_id": "conversation.claude",
    "response": {"text": "...", "card": null},
    "tool_calls": [],
    "duration_ms": 1834,
    "model": "claude-sonnet-4.5",
    "streaming": false
}
```

Behavior:

- Rejects `streaming=true` with `501` for forward compatibility; sync-only in v1.
- When `agent_id` is a Home Assistant agent (or omitted and the default backend is HA), uses the Supervisor token to proxy to HA Core's conversation API.
- When `agent_id` is `direct:<provider>` (or omitted and the default backend is configured as direct), calls the configured provider directly through an OpenAI-compatible `/chat/completions` API.
- In direct mode, the backend exposes a curated read-only MCP tool subset to the
    model (`start_triage`, `get_mesh_state`, `get_health_snapshot`,
    `list_active_issues`, `list_all_nodes`, `list_thread_datasets`,
    `query_history`, `analyze_node`, `get_counter_series`,
    `compare_node_counters`) and executes those calls server-side.
- Returns `412` when the selected backend is not fully configured.
- Returns `502` when the upstream model provider rejects the request.

---

## Data Adapter Interface

Each module must provide a data adapter that normalizes its source into canonical events.

### Adapter Contract

```python
class DataAdapter(ABC):
    """
    Adapters convert source data (logs, APIs, events) into platform events.
    """
    
    def __init__(self, config: dict):
        """Initialize adapter with module-specific config."""
        pass
    
    async def start(self) -> None:
        """Start ingestion (polling, streaming, file tailing)."""
        pass
    
    async def stop(self) -> None:
        """Stop ingestion cleanly."""
        pass
    
    async def get_events(self, since: datetime, limit: int = 1000) -> List[PlatformEvent]:
        """
        Retrieve normalized events.
        
        Returns:
            List[PlatformEvent]: Canonical events from this adapter
        """
        pass

class PlatformEvent(TypedDict):
    """Canonical event schema (all adapters produce this)."""
    timestamp: datetime        # When event occurred (UTC)
    source_adapter: str        # "thread", "energy", "climate", etc.
    entity_ref: str            # Canonical entity identifier (eui64, entity_id, etc.)
    event_type: str            # "attach", "detach", "power_spike", "temp_anomaly", etc.
    severity: str              # "info", "warning", "error"
    metric_type: Optional[str] # "rssi", "power", "temp", etc. (for metrics)
    value: Optional[float]     # Numeric value if metric
    raw_data: dict             # Source-specific raw fields for audit
    enriched_context: dict     # Added by enrichment engine (filled later)
```

### Thread Adapter Example

- **Source**: `/config/logs/matter_*.log` and `/config/logs/thread_*.log`
- **Produces events**: attach, detach, parent_change, rejoin, link_quality_update, etc.
- **Entity ref**: EUI-64 (canonicalized)
- **Metrics**: RSSI, LQI, parent node, routing info
- **Schema discovery**: Deterministic regex profiles (v1); model-assisted if unknown patterns

---

## Enrichment Hook Interface

After ingestion, core enrichment engine calls module hooks to add HA context.

```python
class EnrichmentHook(ABC):
    """
    Modules can register enrichment hooks to add context to events.
    """
    
    async def enrich(self, event: PlatformEvent, ha_context: HAContext) -> dict:
        """
        Add module-specific context to event.
        
        Args:
            event: Normalized platform event
            ha_context: Provides access to HA devices, automations, history
        
        Returns:
            dict: Additional fields to merge into enriched_context
        
        Example:
            Thread module enrichment:
            {
                "device_name": "Bedroom Light",
                "area": "Bedroom",
                "device_type": "light",
                "last_state_change": "2025-05-11T14:30:00Z",
                "active_automations": ["bedroom_motion"]
            }
        """
        pass

class HAContext:
    """Provided by platform to enrichment hooks."""
    
    async def get_device_by_entity_id(self, entity_id: str) -> HADevice:
        pass
    
    async def get_device_metadata(self, device_id: str) -> dict:
        # Name, area, model, manufacturer, integrations
        pass
    
    async def get_automations_for_device(self, device_id: str) -> List[str]:
        pass
    
    async def query_state_history(self, entity_id: str, since: datetime) -> List[tuple]:
        # (timestamp, state, attributes)
        pass
```

---

## Reasoner Module Interface

Reasoners consume normalized events and storage, produce findings (anomalies, incidents, recommendations).

```python
class ReasonerModule(ABC):
    """
    Reasoners analyze data to detect anomalies, incidents, and produce insights.
    """
    
    def __init__(self, config: dict, storage: StorageLayer, ha_context: HAContext):
        """Initialize reasoner with config, DB access, HA context."""
        pass
    
    async def run(self, query: ReasonerQuery) -> List[Finding]:
        """
        Execute reasoner task.
        
        Args:
            query: Time range, entity filter, optional parameters
        
        Returns:
            List[Finding]: Anomalies, incidents, or insights
        """
        pass

class ReasonerQuery(TypedDict):
    """Parameters for a reasoner execution."""
    entity_refs: Optional[List[str]]     # Filter to specific entities (None = all)
    since: datetime                      # Start of time window
    until: datetime                      # End of time window
    module_params: Optional[dict]        # Module-specific config overrides
    use_model: Optional[str]             # Request specific model provider

class Finding(TypedDict):
    """Output from a reasoner."""
    finding_type: str                    # "anomaly", "incident", "insight", "recommendation"
    severity: str                        # "info", "warning", "error"
    affected_entity: str                 # Which entity (node, device, etc.)
    title: str                           # Short summary
    description: str                     # Detailed explanation
    confidence: float                    # 0.0-1.0
    evidence_event_ids: List[str]       # Log entries that support this finding
    recommended_actions: List[str]       # Proposed HA automations or manual steps
    source_module: str                   # "thread", "energy", etc.
    detected_at: datetime
    model_provider: Optional[str]        # If AI-assisted, which provider
    provenance: dict                     # Audit trail: what data, what logic
```

### Thread Reasoner Example

**Deterministic**:
- `TopologyAnalyzer`: Maintains current graph (parents, children, roles), detects missing nodes
- `IncidentDetector`: Flags repeated attach failures, parent churn, link quality drops
- `CorrelationEngine`: Links RSSI dips to other anomalies (power events, HA state changes)

**Optional Model-Assisted**:
- `RootCauseAnalyzer`: "Why is this node offline? Let me check recent logs + topology + HA automations + energy state"

---

> **Status note (2026-05):** this document captures the original V1 module-interface design. The shipped implementation evolved through Phases 1-4 (envelope, catalog reshape, triage entry points, counter time-series). For the current, runtime-accurate tool list see [`06-mcp-tools-reference.md`](06-mcp-tools-reference.md).

## Storage Layer Interface

Reasoners and MCP tools access data via a consistent query interface.

```python
class StorageLayer(ABC):
    """
    Unified access to SQLite + InfluxDB for modules.
    """
    
    # Relational queries (SQLite)
    async def get_node_mappings(self, filters: dict) -> List[NodeMapping]:
        pass
    
    async def get_automations_for_area(self, area: str) -> List[str]:
        pass
    
    # Time-series queries (InfluxDB)
    async def query_events(
        self,
        source_adapter: str,
        entity_refs: Optional[List[str]],
        since: datetime,
        until: datetime,
        event_types: Optional[List[str]] = None
    ) -> List[Event]:
        pass
    
    async def query_metrics(
        self,
        entity_ref: str,
        metric_type: str,
        since: datetime,
        until: datetime,
        aggregation: str = "raw"  # "raw", "5m_mean", "1h_max", etc.
    ) -> List[Metric]:
        pass
    
    async def query_anomalies(
        self,
        entity_refs: Optional[List[str]],
        since: datetime,
        severity: Optional[str] = None
    ) -> List[Anomaly]:
        pass
    
    # Writes (write-protected; only core + reasoners can call)
    async def write_events(self, events: List[Event]) -> None:
        pass
    
    async def write_findings(self, findings: List[Finding]) -> None:
        pass
```

---

## MCP Tool Interface

Modules define MCP tools that expose their capabilities to external reasoners (LLMs, HA automations).

```python
class MCPToolRegistry:
    """
    Modules register tools that will be exposed via MCP server.
    """
    
    def register_tool(
        self,
        name: str,
        description: str,
        input_schema: dict,  # JSON Schema
        handler: Callable,
        requires_model: bool = False  # If True, cannot run in no-model mode
    ) -> None:
        """Register a tool."""
        pass
```

### Thread Module Tools (shipped 0.10.0)

*The original V1 design targeted three core tools. The shipped surface is 36 tools across triage, mesh state, counter time-series, history, issues, discovery, storage, playbooks, and HA/Supervisor lifecycle. See [`06-mcp-tools-reference.md`](06-mcp-tools-reference.md) for the live catalog.*

Original V1 design intent:

1. **get_network_topology** (now `get_mesh_state`)
   - Input: (entity_filter?, time_point?)
   - Output: {nodes: [...], links: [...], partition_id, computed_at}
   - No model required

2. **get_node_details** (now `analyze_node`)
   - Input: eui64
   - Output: structured node payload incl. parent + neighbors, open issues, recent timeline, baselines, playbook entries
   - No model required

3. **list_active_issues**
   - Input: (severity_threshold?)
   - Output: [Finding, ...]
   - No model required

4. **explain_incident** (deferred; reasoner runs deterministically per tick)

---

## Scheduling and Trigger Interfaces

v1 uses a hybrid model:
- Internal scheduler for platform-maintenance jobs (required)
- HA automations for user-centric workflows (recommended)

### Internal Scheduler Interface (required)

```python
class InternalJobRegistry:
    """
    Modules register maintenance jobs that keep the platform healthy.
    These jobs run without user-created HA automations.
    """

    def register_maintenance_job(
        self,
        module_name: str,
        job_name: str,
        cadence: str,  # ISO duration or interval alias, e.g. "30s", "10m"
        handler: Callable,
        retry_policy: dict,
        timeout_seconds: int = 30
    ) -> str:
        """Register an internal maintenance job. Returns job_id."""
        pass

    def register_event_job(
        self,
        module_name: str,
        job_name: str,
        event_filter: dict,
        handler: Callable,
        debounce_seconds: int = 0
    ) -> str:
        """Register an internal event-driven job (e.g., topology recompute)."""
        pass

    async def get_job_history(self, job_id: str) -> List[TaskExecution]:
        pass
```

### HA Automation Trigger Interface (user-facing)

```python
class TriggerExecutionAPI:
    """
    Endpoints/services callable by HA automations or external clients.
    HA owns scheduling for these user-centric workflows.
    """

    async def run_module_summary(self, module_name: str, scope: dict) -> dict:
        pass

    async def run_global_summary(self, scope: dict) -> dict:
        pass

    async def explain_issue(self, issue_id: str) -> dict:
        pass
```

### Default Internal Jobs (Thread v1)

- Ingestion tick: 5-15 seconds (or file-tail event-driven)
- Topology recompute: 30-60 seconds plus event-triggered updates
- Metadata refresh: 10-30 minutes
- Backlog watchdog: 1 minute
- Retention/downsampling: hourly or daily

---

## Module Configuration Schema

All modules must define a JSON schema for their config.

### Thread Module Config (v1)

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "enabled": {"type": "boolean"},
    "log_paths": {
      "type": "array",
      "items": {"type": "string"},
      "description": "Glob patterns for Matter/Thread logs"
    },
    "parser_profiles": {
      "type": "array",
      "items": {"type": "string"},
      "description": "Named parser versions to use"
    },
    "retention": {
      "type": "object",
      "properties": {
        "full_resolution_days": {"type": "integer"},
        "sampled_archive_days": {"type": "integer"}
      }
    },
    "anomaly_thresholds": {
      "type": "object",
      "properties": {
        "parent_churn_in_60_minutes": {"type": "integer"},
        "attach_failures_before_alert": {"type": "integer"},
        "rssi_drop_db": {"type": "number"}
      }
    }
  },
  "required": ["enabled", "log_paths"]
}
```

---

## Deployment Lifecycle

### Module Registration

1. Module provides adapter, enrichment hook, reasoner(s), MCP tools, config schema.
2. Platform discovers and validates module.
3. Platform initializes storage schema if needed.
4. Adapter starts ingestion.
5. Internal maintenance jobs are registered with default cadence.
6. User-facing trigger endpoints are registered for HA automations.
7. MCP tools become available.

### Configuration Changes

1. User updates module config (via HA add-on UI or YAML).
2. Platform validates against schema.
3. Module receives `on_config_updated(new_config)` callback.
4. Module restarts adapters/reasoners as needed.

### Uninstall

1. Platform stops all module tasks.
2. Module cleanup called.
3. Storage schema retained (for archive access).

---

## Testing Harness

Platform provides a test fixture to validate module implementations.

```python
async def test_module_adapter():
    """Verify adapter produces canonical events."""
    adapter = MyAdapter(test_config)
    events = await adapter.get_events(since=...)
    assert all(isinstance(e, PlatformEvent) for e in events)
    assert all(e.source_adapter == "my_module" for e in events)

async def test_module_reasoner():
    """Verify reasoner produces findings."""
    reasoner = MyReasoner(config, storage, ha_context)
    findings = await reasoner.run(query)
    assert all(isinstance(f, Finding) for f in findings)
    assert all(f.source_module == "my_module" for f in findings)

async def test_module_mcp_tools():
    """Verify MCP tools are registered and callable."""
    tools = registry.get_tools_for_module("my_module")
    assert len(tools) > 0
    result = await tools[0].handler({...})
    assert result is not None
```

---

## V1 Module Example: Thread

| Component | Status | Notes |
|-----------|--------|-------|
| Adapter | Required | Log parsing + event normalization |
| Enrichment | Required | Correlate with HA device metadata |
| Reasoner (deterministic) | Required | Topology + incident detection |
| Reasoner (model-assisted) | Optional | v1.5 or v2 |
| MCP tools | Required | 3 core tools minimum |
| Config schema | Required | Thresholds, log paths, retention |

