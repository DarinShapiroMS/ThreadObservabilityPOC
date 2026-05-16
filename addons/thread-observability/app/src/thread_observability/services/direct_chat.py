"""Direct model chat client for dashboard requests.

Uses an OpenAI-compatible chat-completions API so the add-on can talk
directly to providers like Cerebras or OpenAI without going through
Home Assistant's Assist agent layer.
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from ..config import AIConfig
from ..utils.datetime import parse_iso_datetime
from . import web_search

_DIRECT_AGENT_PREFIX = "direct:"
_MAX_TOOL_ROUNDS = 4
_MAX_TOOL_CALLS = 8
_MAX_TOOL_RESULT_MESSAGE_CHARS = 3500
_MAX_EVIDENCE_MESSAGE_CHARS = 5000


_DEFAULT_SYSTEM_PROMPT = """# Role
You are the Thread Mesh Detective, a Thread-network troubleshooting
assistant. You answer questions about a live Thread mesh using backend
diagnostic tools, retained conversation context, and, when needed, web
search for external protocol documentation.

The user does not have direct access to tools, MCP services, functions,
or internal data. Anything that can be retrieved from the backend, you
retrieve yourself.

# Tool usage (mandatory)

Call tools before answering whenever current mesh state, counters,
history, routing, health, or node-specific evidence is relevant to the
question.

- If you identify a tool that would provide information needed to
    answer, call it in this turn. Do not describe the call, propose it,
    or ask permission - call it.
- If you find yourself writing "I would need to check," "let me
    investigate," or "we should look at," stop and call the tool instead.
    Narrating intent to call a tool is treated as a failure to call it.
- Never instruct the user to run, query, check, inspect, or use
    internal tools or services. Do it yourself.
- Never expose tool-call JSON, meta plans, or self-referential
    commentary about your process.
- For multi-part questions, gather evidence for every requested
    dimension the available tools can cover before answering. Do not
    stop at a partial answer because one slice is already grounded.
- Use web_search when current Matter or Thread specifications, vendor
    documentation, or external protocol context is needed and the answer
    is not in backend evidence.

Ask the user only for information they uniquely have, for example
physical observations, intent, what they were doing when the issue
occurred, or for manual actions you cannot perform from the backend.

# Response contract

- First sentence: the strongest supported direct answer to the user's
    actual question. No tool-plan language. No self-referential
    commentary. No "investigate further" wording unless the gathered
    evidence genuinely cannot support a best conclusion.
- Second sentence: concrete evidence anchors from this turn. Name
    at least one tool result explicitly - the tool or evidence source
    plus the specific fact you relied on.
- Then: what it means and what to do next, concisely.

If a top-line conclusion would be contradicted by later caveats, lead
with the mixed or uncertain conclusion directly instead. Do not
summarize overall health as "good," "healthy," or "clear" when
gathered evidence shows active issues, offline nodes, stale data, or
explicit warnings - lead with the strongest supported concern, then
qualify.

# Response shape by question type

- Yes/no or presence/absence: answer "yes," "no," or "no clear
    evidence right now."
- Ranking, for example biggest risk or most likely cause: name the
    strongest supported candidate, or say none clearly stands out. Do
    not promote a speculative watch item into the top risk just because
    the user asked for one. If evidence shows only mild or ambiguous
    concerns, say no major current risk stands out, then name the mild
    concern.
- Forced-choice classification: pick the best-supported bucket
    when evidence rules out broader alternatives. Do not stop at
    "cannot determine" if one class is clearly the best fit. Example:
    if evidence rules out a full-mesh outage and a border-router
    outage, say so plainly and classify as device-specific or no outage
    evident.
- History or change-over-time: cite the actual comparison anchors
    - timestamps and before/after values when available - and make
    them visible in the answer. Do not answer history questions with
    current-snapshot absence language. If the historical anchor for the
    requested window is missing, say so.
- Why or cause: do not invent a root cause from weak current-state
    evidence. If the cause is not established, say so directly and name
    what the current evidence does rule out or weakly suggests.
- Multi-part: address each requested dimension, gathering
    evidence for each one before answering.

# Evidence grounding

- Ground every claim in tool output. Clearly separate observed facts
    from hypotheses.
- When data is stale or cache-aged, mention that before making a
    strong claim.
- Absence claims, for example no chokepoint, no outage, no split, or no
    change, must cite the supporting tool result. Phrase
    current-state absence as "no clear evidence in the current
    snapshot" unless evidence truly proves absence more strongly. Never
    claim "no issue," "no change," or "no current evidence" unless
    gathered evidence explicitly supports that absence.
- When evidence is genuinely missing, say exactly what is missing -
    do not prescribe unperformed internal analyses as next steps for
    the user.
- When multiple explanations fit the evidence, name the top
    hypotheses and say what tool result would distinguish them.
- Do not reason from UI controls, page state, or view-specific
    labels. If a claim is not present in backend evidence, do not use
    it.

# Thread domain rules

- The Leader is not a mandatory forwarding hop. Do not treat traffic
    as routed "through the Leader" by default.
- Parent-child attachment matters for end devices. Do not infer a
    specific node's parent or partition instability from generic
    topology additions, removals, or link diffs unless those changes
    are explicitly tied to that node.
- RouteTable next-hop semantics are Thread-specific and are not
    generic IP routing - interpret accordingly.
- Use a node's friendly/display name when present. On first mention,
    include its EUI64 only when needed to disambiguate.

# Priority when rules conflict

1. Direct, evidence-grounded conclusion
2. Honest scoping of uncertainty and missing evidence
3. Completeness across all dimensions the user asked about
4. Conciseness

Be concise and practical, but do not trade completeness for brevity
when the user asks for multiple dimensions of analysis. Be explicit
about uncertainty only after using the relevant available tools and
only when the remaining gap cannot be closed from available evidence.

# Examples

The blocks below illustrate patterns to follow and avoid. The data is
synthetic - node-alpha, node-beta, T+0, all-zero EUI64s, and example
tool names are placeholders. Do not copy them. Use actual node names,
identifiers, timestamps, and tool names from your real tool calls.

---

Pattern: punting to the user when a tool could answer.

    Avoid:
        "To determine node-alpha's parent, I would need to check the
        route table. Could you confirm which node you mean?"

    Good:
        "node-alpha's parent is node-beta (EUI64 0000000000000001), per
        route_table at T+0. This matches the T-15 snapshot - attachment
        is stable."

---

Pattern: burying active issues under a healthy summary.

    Avoid:
        "Overall the mesh looks healthy. Note: node-alpha has been
        offline for some time and node-beta's last sync was stale."

    Good:
        "Two active concerns: node-alpha offline for about 6h per node_health
        (last_seen T-6h), and stale sync on node-beta per border_status
        (last_sync T-18h). Remaining nodes report nominal state in the
        current snapshot."

---

Pattern: answering a history question with current-snapshot language.

    Avoid:
        "No changes detected in the topology."
        in response to: "what's changed in the last hour?"

    Good:
        "The earliest available comparison anchor is from T-3h per
        topology_history - I don't have a one-hour-ago snapshot. Against
        T-3h, current topology adds node-gamma and drops node-delta; no
        other diffs."

---

Pattern: inventing a root cause from weak evidence.

    Avoid:
        "node-alpha is probably experiencing RF interference."
        when no tool result actually indicates interference

    Good:
        "Root cause is not established from current evidence. node_health
        shows node-alpha online with normal link metrics at T+0, and
        route_table shows stable parent attachment. What's ruled out:
        full disconnection and parent loss. What's not yet checked:
        historical link-quality trend - happy to pull that next."
"""
_CHAT_TOOL_EXCLUDE: frozenset[str] = frozenset(
    {
        "get_health_snapshot",
        "get_mesh_state",
        "get_config",
        "get_recent_logs",
        "ha_get_addon_state",
        "ha_get_addon_logs",
        "ha_get_supervisor_logs",
        "ha_check_for_update",
        "list_otbr_candidates",
        "list_thread_datasets",
        "get_chat_stats",
        "list_playbooks",
        "lookup_playbook",
        "get_environment",
        "get_pipeline_health",
        "start_triage",
    }
)
_WEB_SEARCH_TOOL_NAME = "web_search"
_NODE_EUI64_RE = re.compile(r"\b([0-9a-f]{16})\b", re.IGNORECASE)
_POTENTIAL_NODE_ID_RE = re.compile(r"\b([0-9a-f]{12,16})\b", re.IGNORECASE)
_STRICT_EUI64_RE = re.compile(r"^[0-9a-f]{16}$", re.IGNORECASE)
_NODE_HISTORY_WINDOW = timedelta(hours=2)


class DirectChatConfigError(ValueError):
    """Raised when direct chat is selected but the model config is incomplete."""


@dataclass(slots=True)
class DirectChatTarget:
    provider: str
    model: str
    base_url: str
    api_key: str
    temperature: float

    @property
    def agent_id(self) -> str:
        return f"{_DIRECT_AGENT_PREFIX}{self.provider}"

    @property
    def display_name(self) -> str:
        return f"Direct {self.provider.title()} · {self.model}"


@dataclass(slots=True)
class AnswerReview:
    verdict: str
    critique: str = ""

    @property
    def failed(self) -> bool:
        return self.verdict == "fail"


@dataclass(slots=True)
class AuditVerdict:
    answered_question: bool = True
    grounded_in_evidence: bool = True
    hallucinated_ui_or_actions: bool = False
    tool_choice_ok: bool = True
    missing_tool_opportunities: list[str] = field(default_factory=list)
    contains_extraneous_content: bool = False
    rewrite_needed: bool = False
    repair_action: str = "accept"
    critique: str = ""

    @property
    def failed(self) -> bool:
        return self.repair_action != "accept"

    @property
    def requires_rewrite(self) -> bool:
        return self.repair_action == "rewrite_once"

    @property
    def requires_missing_evidence(self) -> bool:
        return self.repair_action == "gather_missing_evidence_once"


def _coerce_audit_verdict(review: AuditVerdict | AnswerReview | Any) -> AuditVerdict:
    if isinstance(review, AuditVerdict):
        return review
    if isinstance(review, AnswerReview):
        if review.failed:
            return AuditVerdict(
                answered_question=False,
                grounded_in_evidence=False,
                rewrite_needed=True,
                repair_action="rewrite_once",
                critique=review.critique,
            )
        return AuditVerdict()
    return AuditVerdict()


def _default_evaluator_model(target: DirectChatTarget) -> str:
    provider = _normalize_provider(target.provider)
    override = str(os.getenv("THREAD_OBS_AI_EVALUATOR_MODEL", "")).strip()
    if override:
        return override
    if provider == "openai":
        return "gpt-4o-mini"
    if provider == "cerebras":
        return "llama3.1-8b"
    return target.model


def _answer_review_target(target: DirectChatTarget) -> DirectChatTarget:
    return replace(target, model=_default_evaluator_model(target), temperature=0.0)


def _normalize_provider(provider: str | None) -> str:
    return str(provider or "").strip().lower()


def _default_base_url(provider: str) -> str:
    return {
        "openai": "https://api.openai.com/v1",
        "cerebras": "https://api.cerebras.ai/v1",
    }.get(provider, "")


def direct_agent_requested(agent_id: str | None) -> bool:
    return str(agent_id or "").strip().lower().startswith(_DIRECT_AGENT_PREFIX)


def resolve_direct_chat_target(ai: AIConfig) -> DirectChatTarget | None:
    provider = _normalize_provider(ai.provider)
    if provider not in {"openai", "cerebras", "local"}:
        return None
    model = str(ai.model or "").strip()
    base_url = str(ai.base_url or "").strip() or _default_base_url(provider)
    api_key = str(ai.api_key or "").strip()
    if not model or not base_url:
        return None
    if provider != "local" and not api_key:
        return None
    return DirectChatTarget(
        provider=provider,
        model=model,
        base_url=base_url,
        api_key=api_key,
        temperature=float(ai.temperature),
    )


def default_chat_backend(ai: AIConfig, target: DirectChatTarget | None) -> str:
    requested = str(ai.chat_backend or "ha").strip().lower()
    if requested in {"direct", "auto"} and target is not None:
        return "direct"
    return "ha"


def default_chat_label(ai: AIConfig, target: DirectChatTarget | None) -> str:
    if default_chat_backend(ai, target) == "direct" and target is not None:
        return f"Auto ({target.display_name})"
    return "Home Assistant default"


def direct_agent_row(target: DirectChatTarget) -> dict[str, Any]:
    return {
        "agent_id": target.agent_id,
        "name": target.display_name,
        "source": "direct",
        "tool_names": [],
        "has_thread_tools": True,
    }


def direct_chat_preferred(ai: AIConfig, agent_id: str | None, target: DirectChatTarget | None) -> bool:
    if direct_agent_requested(agent_id):
        return True
    if agent_id:
        return False
    return default_chat_backend(ai, target) == "direct"


def require_direct_chat_target(ai: AIConfig) -> DirectChatTarget:
    target = resolve_direct_chat_target(ai)
    if target is None:
        raise DirectChatConfigError(
            "Direct chat is not fully configured. Set ai.enabled=true plus ai.provider, "
            "ai.model, ai.base_url (or use a provider default), and ai.api_key for cloud providers."
        )
    return target


def _extract_message_text(payload: dict[str, Any]) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("direct chat response missing choices")
    first = choices[0] if isinstance(choices[0], dict) else {}
    message = first.get("message") if isinstance(first, dict) else {}
    message = message if isinstance(message, dict) else {}
    content = message.get("content", "")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        bits: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = item.get("text")
                if text:
                    bits.append(str(text))
        return "\n".join(bits).strip()
    return str(content or "").strip()


def _extract_message(payload: dict[str, Any]) -> dict[str, Any]:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("direct chat response missing choices")
    first = choices[0] if isinstance(choices[0], dict) else {}
    message = first.get("message") if isinstance(first, dict) else {}
    if not isinstance(message, dict):
        raise RuntimeError("direct chat response missing message")
    return message


def _json_copy(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value))
    except Exception:  # noqa: BLE001
        return value


def _chat_tools() -> list[dict[str, Any]]:
    from ..api import mcp_tools

    defs = []
    for row in mcp_tools.TOOL_DEFS:
        name = row.get("name")
        if name not in mcp_tools._READ_TOOLS or name in _CHAT_TOOL_EXCLUDE:
            continue
        defs.append(
            {
                "type": "function",
                "function": {
                    "name": row["name"],
                    "description": row.get("description") or "",
                    "parameters": row.get("inputSchema") or {"type": "object", "properties": {}},
                },
            }
        )
    defs.append(
        {
            "type": "function",
            "function": {
                "name": _WEB_SEARCH_TOOL_NAME,
                "description": (
                    "Search the public web for external context such as vendor docs, protocol behavior, "
                    "or product-specific error explanations. Use only when on-box Thread tools are not enough."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query."},
                        "max_results": {
                            "type": "integer",
                            "description": "Maximum results to return (default 5, max 10).",
                            "default": 5,
                        },
                    },
                    "required": ["query"],
                },
            },
        }
    )
    return defs


def _parse_tool_arguments(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if raw in (None, ""):
        return {}
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid tool arguments JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError("tool arguments must decode to an object")
        return parsed
    raise RuntimeError("tool arguments must be an object or JSON string")


def _extract_tool_calls(message: dict[str, Any]) -> list[dict[str, Any]]:
    rows = message.get("tool_calls")
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        function = row.get("function") if isinstance(row.get("function"), dict) else {}
        name = str(function.get("name") or "").strip()
        if not name:
            continue
        out.append(
            {
                "id": str(row.get("id") or f"tool-{uuid.uuid4()}").strip(),
                "type": row.get("type") or "function",
                "name": name,
                "arguments": _parse_tool_arguments(function.get("arguments")),
            }
        )
    return out


def _looks_like_node_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    return (
        "tell me what is going on with node" in normalized
        or "what is going on with node" in normalized
        or "about node " in normalized
        or " eui64" in normalized
        or "eui-64" in normalized
    )
def _looks_like_history_comparison_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    comparison_markers = (
        "compare",
        "comparison",
        "now and",
        "versus",
        " vs ",
    )
    subject_markers = ("channel", "topology", "partition", "mesh")
    return any(marker in normalized for marker in comparison_markers) and any(
        marker in normalized for marker in subject_markers
    )


def _looks_like_counter_or_rf_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    return any(
        marker in normalized
        for marker in (
            "channel",
            "rf",
            "counter",
            "counters",
            "retry",
            "retries",
            "cca",
            "parent change",
            "attach attempt",
        )
    )


def _looks_like_network_risk_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    return any(
        marker in normalized
        for marker in (
            "most risky",
            "what looks most risky",
            "looks risky",
            "should i investigate first",
            "what looks wrong",
        )
    )


def _looks_like_network_health_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    return any(
        marker in normalized
        for marker in (
            "overall health of my thread network",
            "overall health of the thread network",
            "health of my thread network",
            "health of the thread network",
            "offline or stale nodes",
            "border-router problem or a mesh problem",
            "mesh outage",
            "few devices dropping off",
        )
    )


def _looks_like_current_state_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    if (
        _looks_like_network_risk_question(normalized)
        or _looks_like_network_health_question(normalized)
        or _looks_like_partition_split_question(normalized)
    ):
        return True
    return any(
        marker in normalized
        for marker in (
            "right now",
            "current mesh state",
            "current evidence",
            "currently",
            "chokepoint",
            "weak links",
            "error-prone right now",
            "border router right now",
            "phantom router",
            "ghost node in the mesh",
            "stale routing entries",
            "retry storm right now",
            "bad path to the border router",
        )
    )


def _looks_like_partition_split_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    return any(
        marker in normalized
        for marker in (
            "why are there two thread networks",
            "why are there two networks",
            "two thread networks",
            "two networks showing up",
            "mesh split",
            "split into two partitions",
        )
    )


def _looks_like_internal_tool_request(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    return any(
        marker in normalized
        for marker in (
            "what internal mcp tool should i call",
            "which internal mcp tool should i call",
            "what tool should i call",
            "which tool should i call",
            "what function should i call",
            "which function should i call",
        )
    )
def _topology_history_is_empty(result: Any) -> bool:
    if isinstance(result, dict):
        snapshots = result.get("snapshots")
        count = result.get("count")
        if isinstance(snapshots, list):
            return len(snapshots) == 0
        if isinstance(count, int):
            return count == 0
    return False


def _tool_result_data(result: Any) -> Any:
    if isinstance(result, dict) and isinstance(result.get("data"), dict):
        return result["data"]
    return result


def _is_valid_eui64(value: Any) -> bool:
    return bool(_STRICT_EUI64_RE.fullmatch(str(value or "").strip()))


def _parse_iso8601(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    return parse_iso_datetime(text)


def _extract_snapshot_summaries(result: Any) -> list[dict[str, Any]]:
    data = _tool_result_data(result)
    if isinstance(data, dict):
        snapshots = data.get("snapshots")
        if isinstance(snapshots, list):
            return [row for row in snapshots if isinstance(row, dict)]
        if data.get("id") is not None or data.get("snapshot_id") is not None:
            return [data]
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    return []


def _compact_topology_diff(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "snapshot_id_a": result.get("snapshot_id_a"),
        "snapshot_id_b": result.get("snapshot_id_b"),
        "added_nodes": (result.get("added_nodes") or [])[:12],
        "removed_nodes": (result.get("removed_nodes") or [])[:12],
        "changed_nodes": (result.get("changed_nodes") or [])[:12],
        "added_links": (result.get("added_links") or [])[:12],
        "removed_links": (result.get("removed_links") or [])[:12],
    }


def _compact_node_inventory(result: dict[str, Any]) -> dict[str, Any]:
    data = _tool_result_data(result)
    nodes = data.get("nodes") if isinstance(data, dict) and isinstance(data.get("nodes"), list) else []
    return {
        "count": data.get("count") if isinstance(data, dict) else None,
        "nodes": [
            {
                "eui64": row.get("eui64"),
                "friendly_name": row.get("friendly_name") or row.get("display_name"),
                "status": row.get("status"),
                "partition_id": row.get("partition_id"),
                "signal_strength": {
                    "rssi": ((row.get("signal_strength") or {}).get("rssi") if isinstance(row.get("signal_strength"), dict) else None),
                    "lqi": ((row.get("signal_strength") or {}).get("lqi") if isinstance(row.get("signal_strength"), dict) else None),
                    "strongest_available_rssi": ((row.get("signal_strength") or {}).get("strongest_available_rssi") if isinstance(row.get("signal_strength"), dict) else None),
                    "strongest_available_lqi": ((row.get("signal_strength") or {}).get("strongest_available_lqi") if isinstance(row.get("signal_strength"), dict) else None),
                    "best_reporter_name": ((((row.get("signal_strength") or {}).get("best_reporter") or {}).get("name")) if isinstance((row.get("signal_strength") or {}).get("best_reporter"), dict) else None),
                    "best_reporter_eui64": ((((row.get("signal_strength") or {}).get("best_reporter") or {}).get("eui64")) if isinstance((row.get("signal_strength") or {}).get("best_reporter"), dict) else None),
                    "source": ((row.get("signal_strength") or {}).get("source") if isinstance(row.get("signal_strength"), dict) else None),
                },
            }
            for row in nodes[:20]
            if isinstance(row, dict)
        ],
    }


def _compact_health_snapshot(result: dict[str, Any]) -> dict[str, Any]:
    data = _tool_result_data(result)
    summary = data.get("summary") if isinstance(data, dict) and isinstance(data.get("summary"), dict) else {}
    active_issues = data.get("active_issues") if isinstance(data, dict) and isinstance(data.get("active_issues"), dict) else {}
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    return {
        "computed_at": data.get("computed_at") if isinstance(data, dict) else None,
        "status": data.get("status") if isinstance(data, dict) else None,
        "data_age_seconds": data.get("data_age_seconds") if isinstance(data, dict) else None,
        # Flatten the load-bearing fields so prompt compaction doesn't
        # collapse them behind a max-depth boundary.
        "healthy_nodes": summary.get("healthy_nodes"),
        "online_nodes": summary.get("online_nodes"),
        "sleeping_nodes": summary.get("sleeping_nodes"),
        "stale_nodes": summary.get("stale_nodes"),
        "offline_nodes": summary.get("offline_nodes"),
        "total_nodes": summary.get("total_nodes"),
        "duplicate_physical_device_groups": summary.get("duplicate_physical_device_groups"),
        "duplicate_physical_device_rows": summary.get("duplicate_physical_device_rows"),
        "distinct_thread_networks": summary.get("distinct_thread_networks"),
        "active_issue_count": active_issues.get("count"),
        "active_issue_severity_counts": active_issues.get("by_severity"),
        "as_of": meta.get("as_of"),
        "data_source": meta.get("data_source"),
        "cache_age_s": meta.get("cache_age_s"),
        "stale_after_s": meta.get("stale_after_s"),
    }


def _compact_current_mesh_state(result: dict[str, Any]) -> dict[str, Any]:
    data = _tool_result_data(result)
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    nodes = data.get("nodes") if isinstance(data, dict) and isinstance(data.get("nodes"), list) else []
    partitions = data.get("partitions") if isinstance(data, dict) and isinstance(data.get("partitions"), list) else []
    status_counts: dict[str, int] = {}
    for row in nodes:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "unknown").strip().lower() or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "computed_at": data.get("computed_at") if isinstance(data, dict) else None,
        "freshness_minutes": data.get("freshness_minutes") if isinstance(data, dict) else None,
        "node_count": data.get("node_count") if isinstance(data, dict) else None,
        "link_count": data.get("link_count") if isinstance(data, dict) else None,
        "split": data.get("split") if isinstance(data, dict) else None,
        "partition_count": len(partitions),
        "partition_ids": [row.get("partition_id") for row in partitions[:6] if isinstance(row, dict)],
        "partition_leaders": [row.get("leader_eui64") for row in partitions[:6] if isinstance(row, dict)],
        "partition_member_counts": [row.get("member_count") for row in partitions[:6] if isinstance(row, dict)],
        "node_status_counts": status_counts,
        "as_of": meta.get("as_of"),
        "data_source": meta.get("data_source"),
        "cache_age_s": meta.get("cache_age_s"),
        "stale_after_s": meta.get("stale_after_s"),
    }


def _tool_result_for_prompt(name: str, arguments: dict[str, Any], result: Any) -> Any:
    data = _tool_result_data(result)
    if name == "list_all_nodes" and isinstance(data, dict) and isinstance(data.get("nodes"), list):
        return _compact_node_inventory(data)
    if name == "get_health_snapshot" and isinstance(data, dict):
        return _compact_health_snapshot(result if isinstance(result, dict) else {"data": data})
    if name == "get_mesh_state" and isinstance(data, dict):
        return _compact_current_mesh_state(result if isinstance(result, dict) else {"data": data})
    return result


def _validate_chat_tool_arguments(name: str, arguments: dict[str, Any]) -> dict[str, Any] | None:
    if name in {"get_counter_series", "get_signal_series", "get_node_link_signal_history", "analyze_node"}:
        eui64 = arguments.get("eui64")
        if not _is_valid_eui64(eui64):
            return {"error": "invalid eui64 argument: expected 16 hex characters"}
    if name == "compare_node_counters":
        invalid_keys = [key for key in ("eui64_a", "eui64_b") if not _is_valid_eui64(arguments.get(key))]
        if invalid_keys:
            return {"error": f"invalid eui64 argument(s): {', '.join(invalid_keys)} must be 16 hex characters"}
    return None


def _answer_review_policies(
    *,
    internal_tool_request: bool,
    counter_question: bool,
    history_comparison_question: bool,
    partition_split_question: bool,
    node_question: bool,
) -> list[str]:
    policies = [
        "Stay grounded in the gathered evidence from this turn; do not invent facts, fields, or timestamps.",
        "In the first sentence, answer the user's actual question with the strongest supported conclusion. For yes/no or presence questions, say yes, no, or no clear evidence right now. For ranking questions, name the strongest supported candidate or say none clearly stands out.",
        "Do not spend the first sentence on tool plans, self-referential commentary, or 'more investigation is needed' language when the gathered evidence already supports a best conclusion.",
        "In the next sentence, cite at least one concrete evidence anchor from this turn: name the tool or evidence source and the specific fact, count, status, timestamp, or value you relied on.",
        "For ranking questions, do not inflate a speculative watch item into the top risk. If the gathered evidence shows only mild or ambiguous concerns, say no major current risk stands out and then name the mild concern.",
        "For forced-choice classification questions, choose the best-supported category when the gathered evidence rules out broader alternatives; do not default to 'cannot determine' if one category is already the clear best fit.",
        "If the evidence is insufficient, say so explicitly and name the missing evidence instead of guessing.",
        "If the evidence shows no current problem, say that plainly instead of inventing a likely issue to be helpful.",
        "Do not summarize overall health as good, healthy, or clear when the gathered evidence includes active issues, offline nodes, stale data, or another explicit warning; lead with the strongest supported concern and then qualify the overall state.",
        "Do not give a top-line conclusion that is contradicted by later caveats or follow-up sentences. If the evidence is mixed or uncertain, lead with that mixed or uncertain conclusion directly.",
        "For why/cause questions, do not invent a root cause from weak current-state evidence. If the cause is not established, say that directly and then name what the gathered evidence does rule out or weakly suggests.",
        "Do not infer a specific node's parent or partition instability from generic topology additions, removals, or link diffs unless those changes are explicitly tied to that node.",
        "For current-state absence claims, prefer 'no clear evidence in the current snapshot' over absolute language unless the gathered evidence strongly proves absence.",
        "Fail answers that mention generic evidence without citing a concrete anchor from the gathered tool results.",
        "Do not ask the user for more context, a node selection, or an EUI64 when the available tools could gather the next relevant evidence without that user input.",
        "Do not answer with 'no evidence', 'no change', 'no issue', or similar absence claims unless the gathered evidence explicitly supports that absence claim.",
        "Do not tell the user to call internal MCP tools, functions, or backend services themselves.",
        "Do not expose raw tool-call JSON, function-call envelopes, or statements about wanting to call a backend function in the final answer. If more evidence is needed and a relevant tool exists, call it instead of describing that plan to the user.",
        "Do not reason from UI controls, page state, or display labels. Use backend evidence only.",
        "Do not translate missing evidence into interface advice or invented operator workflows.",
        "Do not prescribe backend analyses, routing-table checks, node-health checks, or other internal investigations as next steps unless you actually performed them in this turn and are summarizing their results.",
        "Do not answer with self-referential meta commentary about what you should or should not do; answer the user's question directly from evidence.",
        "Do not infer network improvement, better routing, or a better path to OTBR from node counts, link counts, or generic topology diffs alone; require explicit path, parent, route, or OTBR-role evidence before making that claim.",
        "Do not infer that signal quality improved for any device from node additions, link additions, REED/router roles, or generic topology diffs alone; require explicit before/after RSSI, LQI, parent-change, attachment, or route evidence for the affected devices.",
        "Do not imply that the retained evidence covers the full requested history window when it only spans a shorter interval; state the actual observed coverage and the missing earlier history instead.",
    ]
    if internal_tool_request:
        policies.append(
            "For internal-tool questions, either answer from gathered evidence or refuse clearly; never punt internal tool usage back to the user."
        )
        policies.append(
            "For internal-tool questions, fail any answer that names an internal MCP tool, backend function, or service for the user to call, even if that tool would be relevant."
        )
    if counter_question:
        policies.append("Do not invent node IDs or RF/channel conclusions from empty counter series or missing node inventory evidence.")
        policies.append(
            "Do not conclude that RF conditions did or did not cause a channel change unless the gathered evidence includes an observed channel-change anchor and node-grounded RF evidence around that change."
        )
    if history_comparison_question:
        policies.append("Do not claim a historical change unless the gathered evidence actually distinguishes the current and historical anchors.")
        policies.append(
            "Do not answer that no channel or topology change occurred from current-state snapshots, sparse retained history, or generic history rows unless the comparison anchors are explicitly established in the gathered evidence."
        )
        policies.append(
            "Do not answer a history question with only current-snapshot language such as 'no clear evidence in the current snapshot'; either cite the historical comparison anchors or say that the historical anchor for the requested window is missing."
        )
        policies.append(
            "When you claim a historical change, cite the compared anchors explicitly, including timestamps and before/after values when the gathered evidence provides them."
        )
        policies.append(
            "Do not imply historical comparison anchors; surface them explicitly in the answer so the operator can see what current state was compared against what older state."
        )
    if partition_split_question:
        policies.append(
            "For questions about two Thread networks or partition splits, do not claim multiple current Thread networks unless the gathered current-state evidence actually shows more than one active partition. If current evidence shows one partition or remains ambiguous, say that a live split is not confirmed and stale or historical state may explain the display."
        )
    if node_question:
        policies.append("For node troubleshooting, account for recent attach, recommission, parent-change, or partition-transition evidence before calling a node stable.")
    return policies


def _answer_review_evidence(tool_trace: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reviewed: list[dict[str, Any]] = []
    for row in tool_trace[-6:]:
        reviewed.append(
            {
                "name": row.get("name"),
                "arguments": row.get("arguments"),
                "result": _tool_result_data(row.get("result")),
            }
        )
    return reviewed


def _audit_tool_catalog_summary(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for row in tools:
        function = row.get("function") if isinstance(row.get("function"), dict) else {}
        parameters = function.get("parameters") if isinstance(function.get("parameters"), dict) else {}
        properties = parameters.get("properties") if isinstance(parameters.get("properties"), dict) else {}
        summary.append(
            {
                "name": str(function.get("name") or "").strip(),
                "description": str(function.get("description") or "").strip(),
                "parameters": sorted(str(name) for name in properties.keys())[:8],
            }
        )
    return [row for row in summary if row["name"]]


def _audit_tool_trace(
    tool_trace: list[dict[str, Any]],
    tool_catalog_summary: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    descriptions = {
        str(row.get("name") or ""): str(row.get("description") or "")
        for row in tool_catalog_summary
        if isinstance(row, dict)
    }
    audited: list[dict[str, Any]] = []
    for row in tool_trace[-6:]:
        name = str(row.get("name") or "")
        audited.append(
            {
                "name": name,
                "description": descriptions.get(name, ""),
                "arguments": row.get("arguments"),
                "result": _tool_result_data(row.get("result")),
            }
        )
    return audited


def _parse_audit_verdict(payload: dict[str, Any]) -> AuditVerdict:
    text = _extract_message_text(payload).strip()
    if not text:
        return AuditVerdict()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return AuditVerdict()
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            return AuditVerdict()
    if not isinstance(parsed, dict):
        return AuditVerdict()
    missing_tool_opportunities = parsed.get("missing_tool_opportunities")
    if not isinstance(missing_tool_opportunities, list):
        missing_tool_opportunities = []
    normalized_missing_tools = [str(name).strip() for name in missing_tool_opportunities if str(name).strip()]
    repair_action = str(parsed.get("repair_action") or "accept").strip().lower()
    if repair_action not in {"accept", "rewrite_once", "gather_missing_evidence_once"}:
        repair_action = "accept"
    rewrite_needed = bool(parsed.get("rewrite_needed"))
    tool_choice_ok = bool(parsed.get("tool_choice_ok", True))
    if normalized_missing_tools and repair_action == "accept":
        repair_action = "gather_missing_evidence_once"
    elif rewrite_needed and repair_action == "accept":
        repair_action = "rewrite_once"
    return AuditVerdict(
        answered_question=bool(parsed.get("answered_question", True)),
        grounded_in_evidence=bool(parsed.get("grounded_in_evidence", True)),
        hallucinated_ui_or_actions=bool(parsed.get("hallucinated_ui_or_actions", False)),
        tool_choice_ok=tool_choice_ok,
        missing_tool_opportunities=normalized_missing_tools,
        contains_extraneous_content=bool(parsed.get("contains_extraneous_content", False)),
        rewrite_needed=rewrite_needed,
        repair_action=repair_action,
        critique=str(parsed.get("critique") or "").strip(),
    )


def _parse_answer_review(payload: dict[str, Any]) -> AnswerReview:
    text = _extract_message_text(payload).strip()
    if not text:
        return AnswerReview(verdict="pass")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return AnswerReview(verdict="pass")
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            return AnswerReview(verdict="pass")
    if not isinstance(parsed, dict):
        return AnswerReview(verdict="pass")
    verdict = str(parsed.get("verdict") or "pass").strip().lower()
    critique = str(parsed.get("critique") or "").strip()
    if verdict not in {"pass", "fail"}:
        verdict = "pass"
    return AnswerReview(verdict=verdict, critique=critique)


def _audit_retry_message(verdict: AuditVerdict) -> str:
    critique = verdict.critique or "The prior answer was not sufficiently grounded in the gathered evidence."
    return (
        "Rewrite the prior answer once using the audit feedback below. Keep the answer grounded in the evidence already "
        "gathered in this turn. Start with the strongest supported direct answer to the user's question. Do not replace that direct answer with tool-plan language or 'investigate further' wording when the current evidence already supports a best conclusion. In the next sentence, cite at least one concrete evidence anchor from the gathered tool results. Do not ask the user for more context, a node selection, or an EUI64 unless the user uniquely has information no tool can gather. Do not ask the user to call internal tools or services. If the evidence is insufficient, "
        "say that directly instead of guessing. Keep the answer focused and operator-facing.\n\n"
        f"Audit critique: {critique}"
    )


def _audit_missing_evidence_message(verdict: AuditVerdict) -> str:
    missing_tools = ", ".join(verdict.missing_tool_opportunities) or "the missing evidence bundle"
    critique = verdict.critique or "The first answer did not gather the right evidence for this question."
    return (
        "The first answer skipped needed evidence. Gather one additional evidence bundle now before answering again. "
        f"If the prior answer already referenced one of these tools, call that exact tool now instead of describing the plan: {missing_tools}. "
        "Do not spend this retry on prose-only revision before the missing evidence is gathered. After that, answer directly from the "
        "observed results. If the evidence is still insufficient, say so plainly instead of guessing.\n\n"
        f"Audit critique: {critique}"
    )


def _answer_review_retry_message(review: AnswerReview) -> str:
    critique = review.critique or "The prior answer was not sufficiently grounded in the gathered evidence."
    return (
        "Revise the prior answer once using the evaluator feedback below. Keep the answer grounded in the evidence already "
        "gathered in this turn. Start with the strongest supported direct answer to the user's question. Do not replace that direct answer with tool-plan language or 'investigate further' wording when the current evidence already supports a best conclusion. In the next sentence, cite at least one concrete evidence anchor from the gathered tool results. Do not ask the user for more context, a node selection, or an EUI64 unless the user uniquely has information no tool can gather. Do not ask the user to call internal tools or services. If the evidence is insufficient, "
        "say that directly instead of guessing.\n\n"
        f"Evaluator critique: {critique}"
    )


async def _evaluate_answer_candidate(
    target: DirectChatTarget,
    *,
    message: str,
    candidate_text: str,
    tool_trace: list[dict[str, Any]],
    internal_tool_request: bool,
    counter_question: bool,
    history_comparison_question: bool,
    partition_split_question: bool,
    node_question: bool,
    transcript_events: list[dict[str, Any]] | None = None,
) -> AnswerReview:
    review_target = _answer_review_target(target)
    body = {
        "model": review_target.model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a strict reviewer for a Thread diagnostics assistant. Given the user request, the assistant's "
                    "candidate answer, the gathered tool evidence, and the policy bundle, decide whether the answer passes. "
                    "Output a single JSON object, no code fences, no surrounding prose: "
                    '{"verdict":"pass"|"fail","critique":"<=2 sentences of retry guidance, empty string if pass"}. '
                    "Fail if any of the following hold: the answer's load-bearing claims lack anchors to specific items in the "
                    "gathered evidence; the answer asks the user for information the assistant could have gathered with an "
                    "available tool; the answer describes a tool plan or names a tool it would call instead of having called it "
                    "and the retry must execute the call; the answer makes an unsupported absence claim such as no issue or "
                    "nothing changed without evidence ruling it out; the evidence supports a direct conclusion and the answer "
                    "hedges or defers anyway; the answer ignores a clear gap or contradiction in the evidence; or the answer "
                    "violates any rule in the policy bundle. Treat the policy bundle as mandatory, not advisory. Pass requires "
                    "a direct conclusion at the strongest level the evidence supports, anchored to specific evidence items, with "
                    "hedging calibrated to genuine evidence quality. Honest insufficiency claims pass only when no available tool "
                    "could have closed the gap."
                ),
            },
            {
                "role": "user",
                "content": (
                    "User query:\n"
                    f"{message}\n\n"
                    "Candidate answer:\n"
                    f"{candidate_text}\n\n"
                    "Policy bundle:\n"
                    + "\n".join(f"- {policy}" for policy in _answer_review_policies(
                        internal_tool_request=internal_tool_request,
                        counter_question=counter_question,
                        history_comparison_question=history_comparison_question,
                        partition_split_question=partition_split_question,
                        node_question=node_question,
                    ))
                    + "\n\nGathered evidence:\n"
                    + _serialize_for_prompt(_answer_review_evidence(tool_trace), max_chars=_MAX_EVIDENCE_MESSAGE_CHARS)
                ),
            },
        ],
        "temperature": review_target.temperature,
        "stream": False,
    }
    try:
        payload = await _post_chat_completions(review_target, body)
    except Exception:
        return AnswerReview(verdict="pass")
    if transcript_events is not None:
        transcript_events.append(
            {
                "kind": "answer_review",
                "model": review_target.model,
                "request": _json_copy(body),
                "response": _json_copy(payload),
            }
        )
    return _parse_answer_review(payload)


async def _audit_answer_candidate(
    target: DirectChatTarget,
    *,
    system_prompt: str,
    user_message: str,
    context_message: str,
    candidate_text: str,
    available_tools: list[dict[str, Any]],
    tool_trace: list[dict[str, Any]],
    internal_tool_request: bool,
    counter_question: bool,
    history_comparison_question: bool,
    partition_split_question: bool,
    node_question: bool,
    transcript_events: list[dict[str, Any]] | None = None,
) -> AuditVerdict:
    review_target = _answer_review_target(target)
    tool_catalog_summary = _audit_tool_catalog_summary(available_tools)
    body = {
        "model": review_target.model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a strict audit model for a Thread diagnostics assistant. Review the turn using the user "
                    "question, backend turn context, available tool catalog, actual tool trace, and candidate answer. "
                    "Return JSON only with this exact shape: "
                    '{"answered_question":true,"grounded_in_evidence":true,"hallucinated_ui_or_actions":false,'
                    '"tool_choice_ok":true,"missing_tool_opportunities":[],"contains_extraneous_content":false,'
                    '"rewrite_needed":false,"repair_action":"accept"|"rewrite_once"|"gather_missing_evidence_once",'
                    '"critique":"short guidance"}. '
                    "Use gather_missing_evidence_once only when the answer skipped an obviously better available tool or "
                    "missing evidence bundle. If the answer says it wants to call a specific available tool or asks for more data that an available tool could gather immediately, choose gather_missing_evidence_once rather than rewrite_once. Use rewrite_once when the existing tools were sufficient but the answer needs "
                    "to be rewritten to answer directly, stay grounded, or remove extraneous content. Treat the policy bundle "
                    "as mandatory fail criteria. When in doubt, fail rather than accept; false accepts are worse than one extra rewrite. If the answer names an internal tool or backend function for the user to call, "
                    "exposes raw function-call JSON, says it wants to call an internal tool instead of doing so, "
                    "fails to state the strongest supported direct conclusion, substitutes tool-plan or further-investigation language for a direct conclusion when the evidence already supports one, omits concrete evidence anchors from the gathered tool results, "
                    "asks the user for more context, a node choice, or an EUI64 that the assistant should have gathered itself, "
                    "claims no issue, no change, or no current evidence without explicit supporting anchors in the gathered evidence, "
                    "answers a history-comparison or RF-causation question with a definitive yes or no despite missing anchors, "
                    "or otherwise overstates certainty beyond the gathered evidence, do not accept it."
                ),
            },
            {
                "role": "user",
                "content": (
                    "System prompt:\n"
                    f"{system_prompt}\n\n"
                    "User question:\n"
                    f"{user_message}\n\n"
                    "Turn context:\n"
                    f"{context_message}\n\n"
                    "Candidate answer:\n"
                    f"{candidate_text}\n\n"
                    "Policy bundle:\n"
                    + "\n".join(f"- {policy}" for policy in _answer_review_policies(
                        internal_tool_request=internal_tool_request,
                        counter_question=counter_question,
                        history_comparison_question=history_comparison_question,
                        partition_split_question=partition_split_question,
                        node_question=node_question,
                    ))
                    + "\n\nAvailable tool catalog:\n"
                    + _serialize_for_prompt(tool_catalog_summary, max_chars=_MAX_EVIDENCE_MESSAGE_CHARS)
                    + "\n\nActual tool trace:\n"
                    + _serialize_for_prompt(
                        _audit_tool_trace(tool_trace, tool_catalog_summary),
                        max_chars=_MAX_EVIDENCE_MESSAGE_CHARS,
                    )
                ),
            },
        ],
        "temperature": review_target.temperature,
        "stream": False,
    }
    try:
        payload = await _post_chat_completions(review_target, body)
    except Exception:
        return AuditVerdict()
    if transcript_events is not None:
        transcript_events.append(
            {
                "kind": "audit_review",
                "model": review_target.model,
                "request": _json_copy(body),
                "response": _json_copy(payload),
            }
        )
    return _parse_audit_verdict(payload)


def _force_answer_retry_message() -> str:
    return (
        "Answer now from the evidence already gathered. Start with the strongest supported direct answer to the user's question. "
        "Do not call more tools. Do not replace the direct answer with tool-plan language or 'investigate further' wording when the current evidence already supports a best conclusion. In the next sentence after your direct answer, cite at least one concrete evidence anchor from the gathered tool results. Do not ask the user for more context, a node selection, or an EUI64 unless the user uniquely has information no tool can gather. If the available evidence is still insufficient, say that explicitly and name the missing evidence instead of guessing."
    )


async def _force_answer_from_existing_evidence(
    target: DirectChatTarget,
    messages: list[dict[str, Any]],
) -> str:
    body = {
        "model": target.model,
        "messages": [*messages, {"role": "system", "content": _force_answer_retry_message()}],
        "temperature": target.temperature,
        "stream": False,
    }
    payload = await _post_chat_completions(target, body)
    return _extract_message_text(payload)
def _wants_exact_count_response(message: str) -> bool:
    normalized = " ".join(str(message or "").lower().split())
    if not normalized or "count" not in normalized:
        return False
    markers = (
        "exact count",
        "exact counts",
        "specific count",
        "specific counts",
        "just the two counts",
        "answer with just the two counts",
        "quote the specific counts",
    )
    return any(marker in normalized for marker in markers)


def _build_exact_count_response(message: str, tool_trace: list[dict[str, Any]]) -> str | None:
    if not _wants_exact_count_response(message):
        return None

    history_count: int | None = None
    snapshot_count: int | None = None
    for row in tool_trace:
        name = str(row.get("name") or "")
        result = _tool_result_data(row.get("result"))
        if name == "list_topology_history" and isinstance(result, dict):
            count = result.get("count")
            if isinstance(count, int):
                history_count = count
        if name == "get_storage_stats" and isinstance(result, dict):
            sqlite_stats = result.get("sqlite") if isinstance(result.get("sqlite"), dict) else {}
            row_counts = sqlite_stats.get("row_counts") if isinstance(sqlite_stats.get("row_counts"), dict) else {}
            count = row_counts.get("topology_snapshots")
            if isinstance(count, int):
                snapshot_count = count

    if history_count is None or snapshot_count is None:
        return None
    return (
        f"list_topology_history.count={history_count}; "
        f"get_storage_stats.sqlite.row_counts.topology_snapshots={snapshot_count}"
    )


def _truncate_prompt_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    omitted = len(text) - max_chars
    if max_chars <= 64:
        return text[:max_chars]
    return text[: max_chars - 32] + f"... [truncated {omitted} chars]"


def _compact_for_prompt(
    value: Any,
    *,
    max_items: int,
    max_keys: int,
    max_string_chars: int,
    max_depth: int,
    _depth: int = 0,
) -> Any:
    if _depth >= max_depth:
        if isinstance(value, dict):
            return {"_summary": f"dict with {len(value)} keys"}
        if isinstance(value, list):
            return [f"list with {len(value)} items"]
        if isinstance(value, str):
            return _truncate_prompt_text(value, max_string_chars)
        return value
    if isinstance(value, dict):
        items = list(value.items())
        compact: dict[str, Any] = {}
        for key, inner in items[:max_keys]:
            compact[str(key)] = _compact_for_prompt(
                inner,
                max_items=max_items,
                max_keys=max_keys,
                max_string_chars=max_string_chars,
                max_depth=max_depth,
                _depth=_depth + 1,
            )
        if len(items) > max_keys:
            compact["_truncated_keys"] = len(items) - max_keys
        return compact
    if isinstance(value, list):
        compact_list = [
            _compact_for_prompt(
                inner,
                max_items=max_items,
                max_keys=max_keys,
                max_string_chars=max_string_chars,
                max_depth=max_depth,
                _depth=_depth + 1,
            )
            for inner in value[:max_items]
        ]
        if len(value) > max_items:
            compact_list.append({"_truncated_items": len(value) - max_items})
        return compact_list
    if isinstance(value, str):
        return _truncate_prompt_text(value, max_string_chars)
    return value


def _serialize_for_prompt(value: Any, *, max_chars: int) -> str:
    for max_items, max_keys, max_string_chars, max_depth in (
        (10, 28, 320, 4),
        (6, 18, 220, 3),
        (4, 12, 140, 2),
    ):
        compact = _compact_for_prompt(
            value,
            max_items=max_items,
            max_keys=max_keys,
            max_string_chars=max_string_chars,
            max_depth=max_depth,
        )
        rendered = json.dumps(compact, separators=(",", ":"), ensure_ascii=True)
        if len(rendered) <= max_chars:
            return rendered
    return _truncate_prompt_text(rendered, max_chars)


def _extract_node_eui64(message: str, tool_trace: list[dict[str, Any]]) -> str | None:
    for row in reversed(tool_trace):
        if str(row.get("name") or "") != "analyze_node":
            continue
        arguments = row.get("arguments") if isinstance(row.get("arguments"), dict) else {}
        eui64 = str(arguments.get("eui64") or "").strip().lower()
        if eui64:
            return eui64
    match = _NODE_EUI64_RE.search(str(message or ""))
    if match:
        return match.group(1).lower()
    return None


def _compact_node_history(result: dict[str, Any]) -> dict[str, Any]:
    node = result.get("node") if isinstance(result.get("node"), dict) else {}
    physical_identity = result.get("physical_identity") if isinstance(result.get("physical_identity"), dict) else None
    open_issues = result.get("open_issues") if isinstance(result.get("open_issues"), list) else []
    recent_issues = result.get("recent_issues") if isinstance(result.get("recent_issues"), list) else []
    timeline = result.get("timeline") if isinstance(result.get("timeline"), list) else []
    return {
        "eui64": result.get("eui64"),
        "node": {
            "friendly_name": node.get("friendly_name"),
            "status": node.get("status"),
            "status_changed_at": node.get("status_changed_at"),
            "last_seen": node.get("last_seen"),
            "partition_id": node.get("partition_id"),
            "parent_change_count": node.get("parent_change_count"),
            "attach_attempt_count": node.get("attach_attempt_count"),
            "partition_id_change_count": node.get("partition_id_change_count"),
            "better_partition_attach_attempt_count": node.get("better_partition_attach_attempt_count"),
        },
        "physical_identity": physical_identity,
        "open_issue_kinds": [issue.get("kind") for issue in open_issues if isinstance(issue, dict) and issue.get("kind")],
        "recent_issue_kinds": [issue.get("kind") for issue in recent_issues if isinstance(issue, dict) and issue.get("kind")],
        "timeline": timeline[:12],
    }


def _compact_mesh_view(result: dict[str, Any], eui64: str) -> dict[str, Any]:
    nodes = result.get("nodes") if isinstance(result.get("nodes"), list) else []
    links = result.get("links") if isinstance(result.get("links"), list) else []
    node_row = next((row for row in nodes if isinstance(row, dict) and row.get("eui64") == eui64), None)
    node_links = [
        row for row in links
        if isinstance(row, dict)
        and (row.get("reporter_eui64") == eui64 or row.get("neighbor_eui64") == eui64)
    ][:12]
    partitions = sorted({row.get("partition_id") for row in nodes if isinstance(row, dict) and row.get("partition_id") is not None})
    return {
        "computed_at": result.get("computed_at"),
        "partition_id": result.get("partition_id"),
        "all_partitions": partitions,
        "node": node_row,
        "links": node_links,
    }


async def _gather_backend_node_evidence(message: str, tool_trace: list[dict[str, Any]]) -> dict[str, Any] | None:
    eui64 = _extract_node_eui64(message, tool_trace)
    if not eui64:
        return None

    gathered: list[dict[str, Any]] = []

    if not any(str(row.get("name") or "") == "analyze_node" for row in tool_trace):
        arguments = {"eui64": eui64, "timeline_hours": 6}
        result = await _dispatch_chat_tool("analyze_node", arguments)
        tool_trace.append(
            {
                "id": f"backend-{uuid.uuid4()}",
                "type": "function",
                "name": "analyze_node",
                "arguments": arguments,
                "result": result,
            }
        )
        gathered.append({"tool": "analyze_node", "arguments": arguments, "result": _compact_node_history(result)})

    history_since = (datetime.now(tz=UTC) - _NODE_HISTORY_WINDOW).isoformat()
    history_arguments = {"eui64": eui64, "since": history_since, "limit": 100}
    history_result = await _dispatch_chat_tool("query_history", history_arguments)
    tool_trace.append(
        {
            "id": f"backend-{uuid.uuid4()}",
            "type": "function",
            "name": "query_history",
            "arguments": history_arguments,
            "result": history_result,
        }
    )
    history_rows = history_result if isinstance(history_result, list) else []
    gathered.append(
        {
            "tool": "query_history",
            "arguments": history_arguments,
            "result": history_rows[:20],
        }
    )

    mesh_arguments = {"freshness_minutes": max(1, int(_NODE_HISTORY_WINDOW.total_seconds() // 60))}
    mesh_result = await _dispatch_chat_tool("get_mesh_state", mesh_arguments)
    tool_trace.append(
        {
            "id": f"backend-{uuid.uuid4()}",
            "type": "function",
            "name": "get_mesh_state",
            "arguments": mesh_arguments,
            "result": mesh_result,
        }
    )
    gathered.append(
        {
            "tool": "get_mesh_state",
            "arguments": mesh_arguments,
            "result": _compact_mesh_view(mesh_result, eui64),
        }
    )

    return {
        "eui64": eui64,
        "history_since": history_since,
        "gathered": gathered,
    }


async def _dispatch_chat_tool(name: str, arguments: dict[str, Any], *, allow_excluded: bool = False) -> dict[str, Any]:
    from ..api import mcp_tools

    if name == _WEB_SEARCH_TOOL_NAME:
        return await web_search.search_web(
            str(arguments.get("query") or ""),
            max_results=int(arguments.get("max_results", 5)),
        )
    if name not in mcp_tools._READ_TOOLS or (name in _CHAT_TOOL_EXCLUDE and not allow_excluded):
        return {"error": f"tool not allowed for chat: {name}"}
    return await mcp_tools._dispatch_and_wrap(name, arguments)


async def _prefetch_turn_evidence(
    *,
    message: str,
    tool_trace: list[dict[str, Any]],
    transcript_events: list[dict[str, Any]],
) -> str | None:
    if not _looks_like_current_state_question(message):
        return None

    evidence_plan: list[tuple[str, dict[str, Any]]] = [
        ("get_health_snapshot", {}),
        ("get_mesh_state", {}),
    ]
    guidance = (
        "Use the prefetched health snapshot and current mesh state as the baseline current-state evidence for this turn. "
        "Answer directly from that evidence before asking for anything else."
    )

    if _looks_like_network_risk_question(message):
        guidance = (
            "Use the prefetched health snapshot and current mesh state to identify the most important current risks. "
            "Answer directly from that evidence before asking for anything else."
        )
    elif _looks_like_network_health_question(message):
        guidance = (
            "Use the prefetched health snapshot and current mesh state to answer the current health or outage question directly. "
            "Ground the answer in the observed health summary, node availability, and current mesh shape before asking for anything else."
        )
    elif _looks_like_partition_split_question(message):
        guidance = (
            "Use the prefetched health snapshot and current mesh state to explain whether there are multiple current partitions or only a single current Thread network. "
            "Do not claim a live split unless the current mesh evidence supports it."
        )

    gathered: list[dict[str, Any]] = []
    partition_count: int | None = None
    for name, arguments in evidence_plan:
        result = await _dispatch_chat_tool(name, arguments, allow_excluded=True)
        tool_trace.append(
            {
                "id": f"prefetch-{uuid.uuid4()}",
                "type": "function",
                "name": name,
                "arguments": arguments,
                "result": result,
            }
        )
        transcript_events.append(
            {
                "kind": "tool_result",
                "source": "prefetch",
                "name": name,
                "arguments": _json_copy(arguments),
                "result": _json_copy(result),
            }
        )
        gathered.append(
            {
                "tool": name,
                "arguments": arguments,
                "result": _tool_result_for_prompt(name, arguments, result),
            }
        )
        if name == "get_mesh_state":
            data = _tool_result_data(result)
            nodes = data.get("nodes") if isinstance(data, dict) and isinstance(data.get("nodes"), list) else []
            partitions = {
                row.get("partition_id")
                for row in nodes
                if isinstance(row, dict) and row.get("partition_id") is not None
            }
            if partitions:
                partition_count = len(partitions)

    if _looks_like_partition_split_question(message):
        if partition_count is not None and partition_count <= 1:
            guidance = (
                "The prefetched current mesh state shows one active partition, so do not claim that there are two current Thread networks. "
                "Explain that a live split is not confirmed by current evidence and that stale or historical state may explain the display."
            )
        elif partition_count is not None and partition_count > 1:
            guidance = (
                "The prefetched current mesh state shows multiple active partitions. Explain that this current partition split is why two Thread networks are showing up."
            )
        else:
            guidance = (
                "Use the prefetched mesh-state evidence to explain whether there are multiple current partitions or only stale partition identifiers. "
                "If the current evidence is ambiguous, say a live split is not confirmed."
            )

    return (
        "Backend evidence already gathered for this turn:\n"
        f"{_serialize_for_prompt(gathered, max_chars=_MAX_EVIDENCE_MESSAGE_CHARS)}\n\n"
        f"{guidance}"
    )


async def _post_chat_completions(target: DirectChatTarget, body: dict[str, Any]) -> dict[str, Any]:
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if target.api_key:
        headers["Authorization"] = f"Bearer {target.api_key}"
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(f"{target.base_url.rstrip('/')}/chat/completions", headers=headers, json=body)
        resp.raise_for_status()
        payload = resp.json()
    if not isinstance(payload, dict):
        raise RuntimeError("direct chat response must be a JSON object")
    return payload


async def direct_chat_turn(
    *,
    target: DirectChatTarget,
    message: str,
    rendered_message: str,
    conversation_id: str | None,
) -> dict[str, Any]:
    started = time.perf_counter()
    context_message = rendered_message or message
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": _DEFAULT_SYSTEM_PROMPT},
        {"role": "user", "content": context_message},
    ]
    tools = _chat_tools()
    tool_trace: list[dict[str, Any]] = []
    tool_calls_used = 0
    audit_rewrite_retries = 0
    audit_evidence_retries = 0
    topology_history_empty_hints = 0
    final_text = ""
    transcript_events: list[dict[str, Any]] = []
    node_question = _looks_like_node_question(message)
    history_comparison_question = _looks_like_history_comparison_question(message)
    counter_question = _looks_like_counter_or_rf_question(message)
    internal_tool_request = _looks_like_internal_tool_request(message)
    partition_split_question = _looks_like_partition_split_question(message)

    if node_question:
        node_evidence = await _gather_backend_node_evidence(message, tool_trace)
        if node_evidence:
            node_evidence_message = (
                "Backend node evidence already gathered for this turn:\n"
                f"{_serialize_for_prompt(node_evidence, max_chars=_MAX_EVIDENCE_MESSAGE_CHARS)}\n\n"
                "Use this node-specific evidence to answer directly from the observed status, recent history, and current mesh view before asking for anything else."
            )
            messages.append({"role": "system", "content": node_evidence_message})
            transcript_events.append(
                {
                    "kind": "system_prefetch",
                    "content": node_evidence_message,
                }
            )

    prefetched_evidence_message = await _prefetch_turn_evidence(
        message=message,
        tool_trace=tool_trace,
        transcript_events=transcript_events,
    )
    if prefetched_evidence_message:
        messages.append({"role": "system", "content": prefetched_evidence_message})
        transcript_events.append(
            {
                "kind": "system_prefetch",
                "content": prefetched_evidence_message,
            }
        )

    for _ in range(_MAX_TOOL_ROUNDS + 1):
        body = {
            "model": target.model,
            "messages": messages,
            "tools": tools,
            "tool_choice": "auto",
            "temperature": target.temperature,
            "stream": False,
        }
        payload = await _post_chat_completions(target, body)
        transcript_events.append(
            {
                "kind": "assistant_completion",
                "model": target.model,
                "request": _json_copy(body),
                "response": _json_copy(payload),
            }
        )
        assistant_message = _extract_message(payload)
        tool_calls = _extract_tool_calls(assistant_message)
        assistant_content = assistant_message.get("content")
        messages.append(
            {
                "role": "assistant",
                "content": assistant_content if isinstance(assistant_content, str) else "",
                **({"tool_calls": assistant_message.get("tool_calls")} if tool_calls else {}),
            }
        )
        if not tool_calls:
            candidate_text = _extract_message_text(payload)
            audit = _coerce_audit_verdict(
                await _audit_answer_candidate(
                target,
                system_prompt=_DEFAULT_SYSTEM_PROMPT,
                user_message=message,
                context_message=context_message,
                candidate_text=candidate_text,
                available_tools=tools,
                tool_trace=tool_trace,
                internal_tool_request=internal_tool_request,
                counter_question=counter_question,
                history_comparison_question=history_comparison_question,
                partition_split_question=partition_split_question,
                node_question=node_question,
                transcript_events=transcript_events,
            )
            )
            if audit.requires_missing_evidence and audit_evidence_retries < 1:
                audit_evidence_retries += 1
                retry_message = _audit_missing_evidence_message(audit)
                messages.append(
                    {
                        "role": "system",
                        "content": retry_message,
                    }
                )
                transcript_events.append({"kind": "system_retry", "reason": "missing_evidence", "content": retry_message})
                continue
            if audit.requires_rewrite and audit_rewrite_retries < 1:
                audit_rewrite_retries += 1
                retry_message = _audit_retry_message(audit)
                messages.append(
                    {
                        "role": "system",
                        "content": retry_message,
                    }
                )
                transcript_events.append({"kind": "system_retry", "reason": "rewrite", "content": retry_message})
                continue
            final_text = candidate_text
            break

        for tool_call in tool_calls:
            tool_calls_used += 1
            if tool_calls_used > _MAX_TOOL_CALLS:
                result = {"error": f"tool call limit exceeded ({_MAX_TOOL_CALLS})"}
            else:
                result = _validate_chat_tool_arguments(tool_call["name"], tool_call["arguments"])
                if result is None:
                    result = await _dispatch_chat_tool(tool_call["name"], tool_call["arguments"])
            tool_trace.append(
                {
                    "id": tool_call["id"],
                    "type": tool_call["type"],
                    "name": tool_call["name"],
                    "arguments": tool_call["arguments"],
                    "result": result,
                }
            )
            transcript_events.append(
                {
                    "kind": "tool_result",
                    "tool_call": _json_copy(tool_call),
                    "result": _json_copy(result),
                }
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": _serialize_for_prompt(
                        (
                            {
                                "data": _tool_result_for_prompt(tool_call["name"], tool_call["arguments"], result),
                                **({"meta": result.get("meta")} if isinstance(result, dict) and isinstance(result.get("meta"), dict) else {}),
                            }
                            if isinstance(result, dict) and isinstance(result.get("data"), dict)
                            else _tool_result_for_prompt(tool_call["name"], tool_call["arguments"], result)
                        ),
                        max_chars=_MAX_TOOL_RESULT_MESSAGE_CHARS,
                    ),
                }
            )
            if (
                tool_call["name"] == "list_topology_history"
                and topology_history_empty_hints < 1
                and _topology_history_is_empty(result)
            ):
                topology_history_empty_hints += 1
                messages.append(
                    {
                        "role": "system",
                        "content": (
                            "Topology history returned no persisted snapshots for the requested window. "
                            "Do not call get_topology_history_entry with empty arguments as a fallback; that cannot "
                            "recover missing history. Instead, explain that topology-history data is unavailable and "
                            "fall back to current-state or event-based tools such as get_mesh_state, query_history, "
                            "or analyze_node."
                        ),
                    }
                )
                transcript_events.append(
                    {
                        "kind": "system_hint",
                        "reason": "topology_history_empty",
                        "content": messages[-1]["content"],
                    }
                )
        exact_count_response = _build_exact_count_response(context_message, tool_trace)
        if exact_count_response is not None:
            final_text = exact_count_response
            break
        if tool_calls_used > _MAX_TOOL_CALLS:
            final_text = await _force_answer_from_existing_evidence(target, messages)
            break

    if not final_text:
        if tool_trace:
            final_text = await _force_answer_from_existing_evidence(target, messages)
        if not final_text:
            final_text = "I couldn't complete the tool-assisted reasoning loop. Please retry with a narrower request."
    duration_ms = max(0, int((time.perf_counter() - started) * 1000))
    return {
        "conversation_id": conversation_id or f"direct-{uuid.uuid4()}",
        "agent_id": target.agent_id,
        "response": {"text": final_text, "card": None},
        "tool_calls": tool_trace,
        "transcript": {
            "kind": "direct_chat",
            "message": message,
            "rendered_message": context_message,
            "events": transcript_events,
            "final_text": final_text,
        },
        "duration_ms": duration_ms,
        "model": target.model,
        "streaming": False,
    }
