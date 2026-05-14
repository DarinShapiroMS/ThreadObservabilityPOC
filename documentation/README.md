# Thread Observability Platform - Design Documentation

> **Status (2026-05): the add-on is on version 0.11.37, schema v19, with 41 MCP tools.** The documents in this folder capture the original V1 design intent. They are *not* an authoritative API reference - several tool names and field shapes evolved during Phases 1-4 (envelope, catalog reshape, triage entry points, counter time-series).
>
> For the current, runtime-accurate tool surface see [06-mcp-tools-reference.md](06-mcp-tools-reference.md). For per-release detail see [`addons/thread-observability/CHANGELOG.md`](../addons/thread-observability/CHANGELOG.md).

## Quick Navigation

1. **[Architecture Decision Document](01-architecture-decision.md)**
   - Overview of the multi-tier platform design
   - Component architecture and data flow
   - Storage schema (SQLite + InfluxDB)
   - Resource budgets for HA Yellow
   - Privacy and security model
   - Extensibility checklist

2. **[Module Interface Specification](02-module-interface-spec.md)**
   - Data adapter contract
   - Enrichment hook interface
   - Reasoner module interface
   - Storage layer query API
   - MCP tool registration
   - Testing harness
   - Thread v1 module example

3. **[V1 Product Specification](03-v1-product-spec.md)**
   - User stories (3 key workflows)
   - Core features (9 features)
   - Feature breakdown with UI mockups
   - Implementation plan (4 phases, 4 weeks)
   - Success metrics
   - Future directions (v1.5+)

4. **[Deployment Profiles](04-deployment-profiles.md)**
   - Profile 1: Yellow-Only (recommended baseline)
   - Profile 2: Yellow + Sidecar (power users, LLM reasoning)
   - Profile 3: Yellow + Cloud (privacy-conscious, external API)
   - Profile 4: Air-Gapped (offline, fully local)
   - Migration path between profiles
   - Comparison matrix
   - Recommendations by user type

5. **[Install Strategy: Add-on vs HACS](05-install-strategy-addon-vs-hacs.md)**

6. **[MCP Tool Reference](06-mcp-tools-reference.md)** *(authoritative, auto-generated from the live `/mcp/tools` registry)*

7. **[Sprint Design: Agentic AI Integration](07-agentic-ai-sprint.md)**

8. **[Work Buckets](08-work-buckets.md)** *(current backlog grouped into rational delivery buckets)*

9. **[Live AI Evaluation](08-live-ai-eval.md)** *(release-time live acceptance harness for the dashboard chat assistant)*

10. **[Backlog Roadmap](09-backlog-roadmap.md)** *(current dependency-ordered execution plan for the remaining GitHub backlog)*

11. **[HA MCP Client Setup](10-ha-mcp-client-setup.md)** *(operator walkthrough for connecting Home Assistant Assist agents to Thread Observability MCP tools)*

---

## Executive Summary

**What**: A general-purpose Home Assistant reasoning platform for continuous monitoring, anomaly detection, and AI-assisted diagnostics. V1 focuses on Thread network observability.

**Why**: 
- Home Assistant logs are hard to parse without context
- Users cannot quickly diagnose network connectivity issues
- Raw logs lack device metadata (name, location, relationships)
- No structured way to cross-correlate events with HA state

**How**:
1. Ingest and normalize Thread/Matter logs
2. Enrich with HA device metadata
3. Maintain real-time network topology
4. Detect anomalies deterministically
5. Expose insights via web UI + MCP tools
6. Optional model-assisted reasoning on sidecar/cloud

**Where it runs**:
- **Primary**: HA Yellow add-on (core deterministic layer)
- **Optional**: Sidecar or cloud (model inference, heavy compute)

**Key constraints**:
- All raw data stays local by default
- Deterministic baseline works without any AI
- Designed for HA Yellow constraints (<15% CPU, <250 MB RAM)
- Extensible architecture for future modules (energy, climate, security)

---

## V1 Scope Lock

### What's Included

**Core Platform**
- [ ] Data ingestion framework (pluggable adapters)
- [ ] SQLite + InfluxDB storage layer
- [ ] Enrichment engine (HA device correlation)
- [ ] MCP server process (separate process in add-on container) with read-only query API
- [ ] Internal scheduler for platform maintenance jobs (no user automation required)

**Thread Module (V1)**
- [ ] Log adapter (deterministic parsing)
- [ ] Topology analyzer (graph of nodes/links)
- [ ] Anomaly detector (6 rule-based checks)
- [x] 41 MCP tools across triage, mesh state, history, issues, counter time-series, discovery, storage, playbooks, assessment diagnostics, and HA/Supervisor lifecycle (see [06-mcp-tools-reference.md](06-mcp-tools-reference.md))
- [ ] Web UI dashboard (topology graph, issues list, node details)

**Add-on Packaging**
- [ ] HA add-on manifest
- [ ] Configuration schema
- [ ] Installation from GitHub repo
- [ ] User documentation

### What's NOT Included (v1.5+)

- Model-assisted log parsing (deterministic profiles only)
- Root cause analysis LLM reasoning
- Sidecar orchestration
- HA automation generation
- Energy/climate/security modules
- Multi-provider MoE routing
- Cross-home benchmarking
- GPU acceleration

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Runtime** | Python | Log parsing, async I/O, data science libs |
| **Primary storage** | InfluxDB | Time-series cardinality, downsampling, retention policies |
| **Identity key** | EUI-64 > Thread Node ID > Entity ID | EUI-64 is stable; Node ID can be ephemeral |
| **Deployment** | HA add-on | Users can install directly from GitHub repo |
| **Baseline operation** | Fully deterministic | No model dependency for core monitoring |
| **Model deployment** | Optional sidecar or cloud | Heavy compute off Yellow; local-first privacy |
| **Graph algorithm** | Force-directed layout | Intuitive visualization for network topology |
| **Fault detection** | Query rate vs. expected | Detects ingestion backlog automatically |

---

## Implementation Roadmap

### Week 1-2: Core Platform + Thread v1

**Goals**: 
- Prove architecture with Thread observability
- Get topology + anomalies working end-to-end
- Deploy as working HA add-on

**Deliverables**:
- Storage schema (SQLite + InfluxDB)
- Thread log adapter + parser
- Thread reasoner (topology + anomalies)
- Internal scheduler defaults (ingestion, topology, metadata, watchdog, retention)
- MCP server with 3 tools
- HA add-on manifest

### Week 3: Web UI

**Goals**:
- Real-time topology visualization
- Node drill-down with metrics
- Issues dashboard

**Deliverables**:
- Web UI (React/Vue + Cytoscape.js or D3)
- Responsive design (mobile-friendly)
- Integration with MCP tools backend

### Week 4: Polish + Testing

**Goals**:
- End-to-end testing with real logs
- Documentation
- Performance optimization
- Add-on packaging

**Deliverables**:
- GitHub repo with add-on
- User guide
- Developer guide
- Performance benchmarks

### Week 5+: v1.5 / v2 Features

- Model-assisted parsing (parser discovery)
- Sidecar coordination
- Root cause analysis
- Energy/climate/security modules
- HA automation generation
- MoE reasoning

---

## Storage Decisions

### SQLite (Metadata)

```sql
-- Node identity mappings
node_mappings (id, eui64, thread_node_id, ha_entity_id, ha_device_id, 
               friendly_name, area, confidence, last_seen, created_at)

-- Parser profiles
parser_registry (id, version, adapter_name, signature, confidence, 
                 test_samples, created_at)

-- Task execution audit trail
task_executions (id, task_name, reasoner_module, model_provider, 
                 started_at, completed_at, status, findings, evidence_ids)
```

### InfluxDB (Time-Series)

```
events
  - source_adapter (tag)
  - entity_ref (tag)
  - event_type (tag)
  - severity (tag)
  - timestamp (time)
  - value, confidence (fields)

anomalies
  - entity_ref (tag)
  - anomaly_type (tag)
  - module (tag)
  - confidence_score (field)
  - timestamp (time)

metrics
  - entity_ref (tag)
  - metric_type (tag)
  - timestamp (time)
  - value (field)
```

### Retention Policy

```yaml
- Full resolution: 3 days (1-minute granularity)
- Sampled archive: 14 days (5-minute granularity)
- Anomaly records: 30 days (always full-res)
- Audit trail: 7 days
```

---

## Resource Budget (HA Yellow)

| Component | CPU | RAM | Storage |
|-----------|-----|-----|---------|
| Python runtime + core | 5-10% | 50-100 MB | <100 MB |
| Thread log ingestion | 2-5% | 30 MB | -- |
| InfluxDB buffer | 2-5% | 100-150 MB | 2-5 GB |
| Reasoners | 1-3% | 20-30 MB | -- |
| Web UI | 1-2% | 20 MB | -- |
| **Total baseline** | **~15%** | **200-250 MB** | **2-5 GB** |

**Headroom**: 250 MB remains for spike handling

**Fault detection**: Monitor `SELECT COUNT(*) FROM events WHERE timestamp > now() - 1h`. If count drops, backpressure alarm.

---

## Privacy Model

**Default: Local-only, no AI**
```yaml
ai:
  enabled: false
  provider: local
  fallback_to_cloud: false
```

**When enabled**:
- User explicitly opts in per provider
- Data sent: only aggregated/redacted
- Raw logs stay local
- All reasoning is audited

**Options**:
1. Ollama (local, privacy-first)
2. OpenAI/Anthropic (cloud, opt-in)
3. Future: other providers via plugin

---

## MCP Tools (current)

The shipped add-on (0.10.0) exposes 36 read- and lifecycle tools. See [06-mcp-tools-reference.md](06-mcp-tools-reference.md) for the full, auto-generated catalog with arguments and descriptions.

Highlights:

- **Triage entry points**: `start_triage`, `get_environment`, `get_pipeline_health`, `get_health_snapshot`
- **Mesh state**: `get_mesh_state`, `list_all_nodes`, `analyze_node`
- **Counter time-series (Phase 4, schema v19)**: `get_counter_series`, `compare_node_counters`
- **History**: `query_history`, `list_topology_history`, `get_topology_history_entry`, `diff_topology_history`
- **Issues**: `list_active_issues`, `close_issue`
- **HA/Supervisor lifecycle**: `ha_get_addon_state`, `ha_update_addon`, `ha_restart_addon`, `ha_check_for_update`, ...

All read tools return a `{data, meta}` envelope. `meta` carries `as_of`, `data_source`, `cache_age_s`, and the latest `pipeline_tick` block so clients can decide whether the cached data is fresh enough.

---

## Extensibility Checklist (for v2 modules)

When adding a new module (energy, climate, security):

- [ ] Can implement DataAdapter interface?
- [ ] Can resolve entities to HA device records?
- [ ] Can define module-specific anomaly types?
- [ ] Can query module facts via generic MCP tools?
- [ ] Can register internal maintenance jobs with safe defaults?
- [ ] Can integrate with HA automations?
- [ ] Can be tested independently?

---

## How This Supports Future AI Watchdog

The architecture is designed to scale to a "household AI watchdog" that reasons over all HA data:

1. **Multiple data sources**: Thread, energy, climate, security, etc. all feed the same platform
2. **Cross-domain correlation**: Reasoners can query facts from any module
3. **Scheduled reasoning**: "Every hour, analyze HA state and decide if alerting is needed"
4. **Model-agnostic**: Whether using local Ollama or Claude via MCP, routing is configurable
5. **Action execution**: Findings can trigger HA automations or notify user
6. **Auditability**: Every reasoning step is logged with provenance

Example future query:
```
"Why did my bedroom light flickering spike at 14:00?
 Check: Thread network status, power grid events, climate sensors, 
 recent automations, device error logs."
```

---

## Getting Started

### As an Architect

Read in order:
1. [Architecture Decision Document](01-architecture-decision.md) - understand layers & trade-offs
2. [Module Interface Specification](02-module-interface-spec.md) - understand how to extend
3. [Deployment Profiles](04-deployment-profiles.md) - understand hardware options

### As a Developer

Read in order:
1. [V1 Product Specification](03-v1-product-spec.md) - understand what to build
2. [Module Interface Specification](02-module-interface-spec.md) - understand contracts
3. [Architecture Decision Document](01-architecture-decision.md) - understand storage & APIs

### As an End User

Read:
- [Deployment Profiles](04-deployment-profiles.md) - choose your setup
- [V1 Product Specification](03-v1-product-spec.md#user-stories) - understand capabilities

---

## Questions to Lock Before Coding

1. **Resource budget**: Agree on Yellow CPU/memory targets ✅ (locked: <15% CPU, <250 MB RAM)
2. **Retention policy**: Default retention periods ✅ (locked: 3d full, 14d sampled)
3. **No-model mode**: Baseline functionality without AI ✅ (locked: fully usable)
4. **Privacy defaults**: Local-only or cloud-first ✅ (locked: local-only default)
5. **Deployment**: v1 targets HA add-on ✅ (locked: yes)
6. **Scope**: Thread observability only, extensible platform ✅ (locked: yes)

**All locked. Ready to code.**

