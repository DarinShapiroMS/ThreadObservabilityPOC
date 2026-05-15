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
_MAX_TOOL_DEFERRAL_RETRIES = 1
_MAX_ANSWER_VALIDATION_RETRIES = 1
_MAX_NODE_EVIDENCE_RETRIES = 1
_MAX_HISTORY_COMPARISON_RETRIES = 1
_MAX_COUNTER_GROUNDING_RETRIES = 1
_MAX_TOOL_RESULT_MESSAGE_CHARS = 3500
_MAX_EVIDENCE_MESSAGE_CHARS = 5000
_DEFAULT_SYSTEM_PROMPT = (
    "You are the Thread Observability dashboard troubleshooting assistant. Answer using only the provided "
    "Thread dashboard context, the user's request, and the available diagnostic tools. "
    "Use tools when you need current mesh state, counters, history, or node-specific evidence. "
    "Do not tell the user to run the available diagnostic tools themselves. If a relevant tool exists, call it "
    "yourself before answering. The user does not have direct access to MCP tools, functions, or internal data "
    "services. Never tell the user to call, query, check, inspect, or use those services directly; do that "
    "yourself when possible. Ask the user only for information they uniquely have or for a physical/manual action "
    "you cannot perform from the dashboard. "
    "Use web_search only when outside product or protocol context is actually needed. "
    "Prefer a node's friendly/display name when present; on first mention include its EUI64 only when that helps "
    "disambiguate. Ground conclusions in tool output, clearly separate observed facts from hypotheses, and mention "
    "when evidence is stale or cache-aged before making a strong claim. Use correct Thread terminology: the Leader "
    "is not a mandatory forwarding hop, parent-child attachment matters for end devices, and RouteTable next-hop "
    "semantics are not generic IP routing. This is an interactive troubleshooting conversation: when multiple "
    "explanations fit the evidence, name the top hypotheses and say what tool result would distinguish them. "
    "Gather obvious diagnostic context before asking the user to restate the problem. Prefer concise answers in "
    "this order: what you found, why it matters, and what to do next. Do not tell the user to click or use a "
    "dashboard control unless that control is actually present in the current dashboard UI. In particular, do not "
    "suggest nonexistent actions such as setting an OTBR slug or restarting the pipeline from the dashboard. "
    "Be concise, practical, and explicit about "
    "uncertainty when the available evidence is insufficient."
)
_CHAT_TOOL_EXCLUDE: frozenset[str] = frozenset(
    {
        "get_config",
        "get_recent_logs",
        "ha_get_addon_state",
        "ha_get_addon_logs",
        "ha_get_supervisor_logs",
        "ha_check_for_update",
        "list_otbr_candidates",
    }
)
_WEB_SEARCH_TOOL_NAME = "web_search"
_NODE_EUI64_RE = re.compile(r"\b([0-9a-f]{16})\b", re.IGNORECASE)
_POTENTIAL_NODE_ID_RE = re.compile(r"\b([0-9a-f]{12,16})\b", re.IGNORECASE)
_STRICT_EUI64_RE = re.compile(r"^[0-9a-f]{16}$", re.IGNORECASE)
_NODE_HISTORY_WINDOW = timedelta(hours=2)
_UNSUPPORTED_DASHBOARD_ACTION_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bset\s+otbr\s+slug\b", re.IGNORECASE),
    re.compile(r"\brestart\s+pipeline\b", re.IGNORECASE),
    re.compile(r"\btoggl(?:e|ed)\b.*\bcurrent\b.*\bhistorical\b.*\bview", re.IGNORECASE),
    re.compile(r"\bcurrent\s+and\s+historical\s+views\b", re.IGNORECASE),
    re.compile(r"\bwarning\s+icon\b.*\bgraph\s+diagnostics\b", re.IGNORECASE),
    re.compile(r"\bgraph\s+diagnostics\s+(?:panel|view)\b", re.IGNORECASE),
    re.compile(r"\bweak\s+links?\s+(?:view|panel|details?)\b", re.IGNORECASE),
)
_PAGE_CONTEXT_SINGLE_PARTITION_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:1|one)\s+partition\b", re.IGNORECASE),
    re.compile(r"\bonly\s+one\s+partition\b", re.IGNORECASE),
    re.compile(r"\bdoes\s+not\s+show\s+two\s+partitions\b", re.IGNORECASE),
)
_PAGE_CONTEXT_UNIFIED_NETWORK_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bsingle\s+unified\s+thread\s+network\b", re.IGNORECASE),
    re.compile(r"\bsingle\s+thread\s+network\b", re.IGNORECASE),
)
_PAGE_CONTEXT_NO_OFFLINE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:0|zero)\s+offline\s+nodes?\b", re.IGNORECASE),
    re.compile(r"\bno\s+offline\s+(?:devices|nodes?)\b", re.IGNORECASE),
    re.compile(r"\ball\s+nodes\s+are\s+online\b", re.IGNORECASE),
    re.compile(r"\bfully\s+operational\b", re.IGNORECASE),
)
_PAGE_CONTEXT_NO_STALE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:0|zero)\s+stale\s+nodes?\b", re.IGNORECASE),
    re.compile(r"\bno\s+stale\s+(?:devices|nodes?)\b", re.IGNORECASE),
)


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


def _tool_deferral_retry_budget(target: DirectChatTarget) -> int:
    provider = str(target.provider or "").strip().lower()
    model = str(target.model or "").strip().lower()
    if provider == "cerebras" and ("llama3.1-8b" in model or "llama-3.1-8b" in model or "8b" in model):
        return 2
    return _MAX_TOOL_DEFERRAL_RETRIES


def _tool_deferral_retry_message(attempt: int) -> str:
    if attempt <= 1:
        return (
            "Do not tell me to use the available tools myself. Call the relevant tools now, then answer from the "
            "observed results."
        )
    return (
        "Do not ask me to call internal MCP tools, functions, or data services. You must either call the relevant "
        "tools yourself now or answer explicitly that the currently available evidence is insufficient. Do not punt "
        "tool use back to the user."
    )


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


def _looks_like_tool_deferral(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    patterns = (
        "you can use the",
        "you can use",
        "you can call the",
        "you can call",
        "you can query the",
        "you can check the",
        "we would need to call",
        "i would need to call",
        "use the \"",
        "use the get_",
        "to investigate further, you can use",
        "to proceed, i would like to know",
        "to proceed, i would need to know",
        "it's also a good idea to check",
        "you should use the",
        "i recommend analyzing",
        "i recommend calling",
        "i would recommend analyzing",
        "i would recommend calling",
        "call the \"",
        "calling the \"",
    )
    if any(pattern in normalized for pattern in patterns):
        return True
    if any(
        phrase in normalized
        for phrase in (
            " tool ",
            " tool.",
            " function ",
            " function.",
            " data service",
            " mcp service",
        )
    ):
        return True
    return any(
        tool_name in normalized
        for tool_name in (
            "get_mesh_state",
            "analyze_node",
            "get_counter_series",
            "query_history",
            "get_topology_history_entry",
            "list_topology_history",
            "get_node_history",
        )
    )


def _answer_requests_user_node_selection(candidate_text: str) -> bool:
    normalized = " ".join(str(candidate_text or "").lower().split())
    if not normalized:
        return False
    return any(
        phrase in normalized
        for phrase in (
            "please provide the eui64",
            "provide the eui64 of the node",
            "provide the node's eui64",
            "please provide the node's eui64",
            "which node you would like to investigate",
            "i would need to know which node",
            "need to know which node",
            "selected eui64",
            "selected node eui64",
            "selected node",
            "none of them have a selected eui64",
            "select one of the nodes",
            "please select a node",
            "select a node from the dashboard",
            "provide the eui64 of the node you are interested in",
        )
    )


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


def _looks_like_offline_nodes_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    return "offline node" in normalized or "offline nodes" in normalized


def _looks_like_overall_health_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    return any(
        marker in normalized
        for marker in (
            "overall health of my network",
            "overall health of the network",
            "health of my network right now",
            "how healthy is my network",
            "is my network healthy",
        )
    )


def _looks_like_partition_summary_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    if not any(marker in normalized for marker in ("partition", "thread network", "thread networks", "unified mesh")):
        return False
    return any(
        marker in normalized
        for marker in (
            "why are there two partitions",
            "how many partitions",
            "partition status",
            "partitions right now",
            "single unified",
            "one partition",
            "unified thread network",
            "distinct thread networks",
            "two networks",
            "network split",
        )
    )


def _should_apply_page_context_contradiction_guard(text: str) -> bool:
    return (
        _looks_like_offline_nodes_question(text)
        or _looks_like_overall_health_question(text)
        or _looks_like_partition_summary_question(text)
    )


def _looks_like_history_comparison_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    comparison_markers = (
        "24h ago",
        "24 hours ago",
        "yesterday",
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


def _looks_like_chokepoint_question(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return False
    return any(marker in normalized for marker in ("chokepoint", "chokepoints", "bottleneck", "bottlenecks"))


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


def _has_sufficient_node_evidence(tool_trace: list[dict[str, Any]]) -> bool:
    names = {str(row.get("name") or "") for row in tool_trace}
    if "analyze_node" not in names:
        return False
    return bool(names & {"query_history", "get_mesh_state", "start_triage", "list_all_nodes"})


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


def _result_contains_channel_evidence(value: Any, *, _depth: int = 0) -> bool:
    if _depth > 5:
        return False
    if isinstance(value, dict):
        for key, inner in value.items():
            if str(key).strip().lower() == "channel" and inner not in (None, ""):
                return True
            if _result_contains_channel_evidence(inner, _depth=_depth + 1):
                return True
        return False
    if isinstance(value, list):
        return any(_result_contains_channel_evidence(inner, _depth=_depth + 1) for inner in value)
    return False


def _tool_trace_contains_channel_evidence(tool_trace: list[dict[str, Any]]) -> bool:
    return any(_result_contains_channel_evidence(_tool_result_data(row.get("result"))) for row in tool_trace)


def _history_answer_overclaims_channel_change(message: str, candidate_text: str, tool_trace: list[dict[str, Any]]) -> bool:
    normalized_message = " ".join(str(message or "").lower().split())
    normalized_answer = " ".join(str(candidate_text or "").lower().split())
    if "channel" not in normalized_message:
        return False
    channel_claim_markers = (
        "channel has changed",
        "channel changed",
        "current channel is different",
        "channel did not change",
        "channel has not changed",
        "same channel",
    )
    if not any(marker in normalized_answer for marker in channel_claim_markers):
        return False
    return not _tool_trace_contains_channel_evidence(tool_trace)


def _build_history_insufficient_response(tool_trace: list[dict[str, Any]]) -> str:
    diff_row = next((row for row in reversed(tool_trace) if str(row.get("name") or "") == "diff_topology_history"), None)
    if diff_row is None:
        return (
            "I don't have channel-specific history for the retained comparison anchors, so I can't determine whether "
            "the Thread channel changed in that window."
        )
    diff = _tool_result_data(diff_row.get("result"))
    diff = diff if isinstance(diff, dict) else {}
    summary = diff.get("summary") if isinstance(diff.get("summary"), dict) else {}
    added_nodes = int(summary.get("added_node_count") or len(diff.get("added_nodes") or []))
    removed_nodes = int(summary.get("removed_node_count") or len(diff.get("removed_nodes") or []))
    changed_nodes = int(summary.get("changed_node_count") or len(diff.get("changed_nodes") or []))
    if added_nodes or removed_nodes or changed_nodes:
        return (
            "I can see retained topology changes between the comparison snapshots "
            f"({added_nodes} added nodes, {removed_nodes} removed nodes, {changed_nodes} changed nodes), "
            "but I don't have channel-specific history for those anchors, so I can't determine whether the Thread "
            "channel changed."
        )
    return (
        "I can compare the retained topology snapshots, but they do not include channel-specific history, so I can't "
        "determine whether the Thread channel changed."
    )


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


def _history_snapshot_refs(tool_trace: list[dict[str, Any]]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for row in tool_trace:
        if str(row.get("name") or "") != "get_topology_history_entry":
            continue
        arguments = row.get("arguments") if isinstance(row.get("arguments"), dict) else {}
        for snapshot in _extract_snapshot_summaries(row.get("result")):
            refs.append(
                {
                    "id": snapshot.get("id") or snapshot.get("snapshot_id"),
                    "captured_at": snapshot.get("captured_at") or snapshot.get("ts"),
                    "arguments": arguments,
                }
            )
    return refs


def _history_comparison_is_unreliable(message: str, tool_trace: list[dict[str, Any]]) -> bool:
    if not _looks_like_history_comparison_question(message):
        return False
    if not any(str(row.get("name") or "") == "list_topology_history" for row in tool_trace):
        return True
    refs = _history_snapshot_refs(tool_trace)
    if len(refs) < 2:
        return True
    ids = {int(ref["id"]) for ref in refs if isinstance(ref.get("id"), int)}
    if len(ids) >= 2:
        return False
    captured = {str(ref.get("captured_at") or "") for ref in refs if ref.get("captured_at")}
    return len(captured) < 2


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
            }
            for row in nodes[:20]
            if isinstance(row, dict)
        ],
    }


def _extract_known_node_ids(tool_trace: list[dict[str, Any]]) -> set[str]:
    known: set[str] = set()
    for row in tool_trace:
        name = str(row.get("name") or "")
        arguments = row.get("arguments") if isinstance(row.get("arguments"), dict) else {}
        if name in {"analyze_node", "get_counter_series"}:
            eui64 = str(arguments.get("eui64") or "").strip().lower()
            if eui64:
                known.add(eui64)
        if name == "compare_node_counters":
            for key in ("eui64_a", "eui64_b"):
                eui64 = str(arguments.get(key) or "").strip().lower()
                if eui64:
                    known.add(eui64)
        data = _tool_result_data(row.get("result"))
        if isinstance(data, dict):
            eui64 = str(data.get("eui64") or "").strip().lower()
            if eui64:
                known.add(eui64)
            nodes = data.get("nodes")
            if isinstance(nodes, list):
                for node in nodes:
                    if isinstance(node, dict):
                        eui64 = str(node.get("eui64") or "").strip().lower()
                        if eui64:
                            known.add(eui64)
            for key in ("a", "b"):
                side = data.get(key)
                if isinstance(side, dict):
                    eui64 = str(side.get("eui64") or "").strip().lower()
                    if eui64:
                        known.add(eui64)
    return known


def _response_references_unknown_node(text: str, tool_trace: list[dict[str, Any]]) -> bool:
    refs = {match.group(1).lower() for match in _POTENTIAL_NODE_ID_RE.finditer(str(text or ""))}
    if not refs:
        return False
    known = _extract_known_node_ids(tool_trace)
    if not known:
        return False
    return any(ref not in known for ref in refs)


def _counter_tool_arguments_are_invalid(tool_trace: list[dict[str, Any]]) -> bool:
    for row in tool_trace:
        name = str(row.get("name") or "")
        arguments = row.get("arguments") if isinstance(row.get("arguments"), dict) else {}
        if name in {"get_counter_series", "analyze_node"}:
            eui64 = arguments.get("eui64")
            if not _is_valid_eui64(eui64):
                return True
        if name == "compare_node_counters":
            for key in ("eui64_a", "eui64_b"):
                eui64 = arguments.get(key)
                if not _is_valid_eui64(eui64):
                    return True
    return False


def _counter_evidence_is_empty(tool_trace: list[dict[str, Any]]) -> bool:
    saw_counter_tool = False
    for row in tool_trace:
        name = str(row.get("name") or "")
        data = _tool_result_data(row.get("result"))
        if name == "get_counter_series" and isinstance(data, dict):
            saw_counter_tool = True
            if not isinstance(data.get("series"), list) or len(data.get("series") or []) == 0:
                return True
        if name == "compare_node_counters" and isinstance(data, dict):
            saw_counter_tool = True
            a_series = data.get("a") if isinstance(data.get("a"), dict) else {}
            b_series = data.get("b") if isinstance(data.get("b"), dict) else {}
            if not (a_series.get("series") or b_series.get("series")):
                return True
    return saw_counter_tool and False


def _counter_answer_mentions_unsupported_evidence(candidate_text: str) -> bool:
    normalized = " ".join(str(candidate_text or "").lower().split())
    if not normalized:
        return False
    return any(
        phrase in normalized
        for phrase in (
            "configuration history",
            "config history",
            "reset history",
            "node's configuration",
            "nodes configuration",
            "node that changed channels",
            '"channel_change" counter',
            "get_node_history",
        )
    )


def _counter_answer_is_unreliable(candidate_text: str, tool_trace: list[dict[str, Any]]) -> bool:
    return (
        _counter_tool_arguments_are_invalid(tool_trace)
        or _counter_evidence_is_empty(tool_trace)
        or _response_references_unknown_node(candidate_text, tool_trace)
        or _answer_requests_user_node_selection(candidate_text)
        or _counter_answer_mentions_unsupported_evidence(candidate_text)
    )


def _build_counter_insufficient_response(tool_trace: list[dict[str, Any]]) -> str:
    reasons: list[str] = []
    if _counter_tool_arguments_are_invalid(tool_trace):
        reasons.append("the counter query was not grounded to a real 16-hex EUI64 from the mesh inventory")
    if _counter_evidence_is_empty(tool_trace):
        reasons.append("the returned counter series was empty")
    if not reasons:
        reasons.append("the available counter evidence is insufficient")
    return (
        "I can't determine whether RF conditions caused the channel change from the available evidence because "
        + " and ".join(reasons)
        + "."
    )


def _build_internal_tool_refusal_response(message: str, tool_trace: list[dict[str, Any]]) -> str:
    prefix = "I can't ask you to call internal MCP tools directly. "
    if _looks_like_counter_or_rf_question(message):
        return prefix + _build_counter_insufficient_response(tool_trace)
    return prefix + "The available evidence is insufficient to answer that from the current turn."


def _internal_tool_answer_needs_refusal(message: str, candidate_text: str, tool_trace: list[dict[str, Any]]) -> bool:
    normalized = " ".join(str(candidate_text or "").lower().split())
    if _looks_like_tool_deferral(candidate_text) or _counter_answer_mentions_unsupported_evidence(candidate_text):
        return True
    if _looks_like_counter_or_rf_question(message) and (
        _counter_tool_arguments_are_invalid(tool_trace) or _counter_evidence_is_empty(tool_trace)
    ):
        return True
    return any(
        phrase in normalized
        for phrase in (
            "please provide the friendly name",
            "please provide the eui64",
            "please provide the node's eui64",
            "provide the eui64 of the node",
            "which node you would like to investigate",
            "selected node",
            "selected eui64",
            "selected node eui64",
            "none of them have a selected eui64",
            "select one of the nodes",
            "please select a node",
            "select a node from the dashboard",
            "i need to know which node",
            "i would need to know which node",
            "i do not have any information about which node",
            "i don't have any information about which node",
            "i do not have any information about the node",
            "i don't have any information about the node",
        )
    )


def _apply_deterministic_fallbacks(
    *,
    message: str,
    candidate_text: str,
    tool_trace: list[dict[str, Any]],
    history_comparison_question: bool,
    counter_question: bool,
    internal_tool_request: bool,
) -> str:
    if history_comparison_question and (
        _history_comparison_is_unreliable(message, tool_trace)
        or _history_answer_overclaims_channel_change(message, candidate_text, tool_trace)
    ):
        return _build_history_insufficient_response(tool_trace)
    if _answer_mentions_unsupported_dashboard_action(candidate_text):
        return _build_unsupported_dashboard_action_response(message, candidate_text)
    if _should_apply_page_context_contradiction_guard(message) and _answer_contradicts_page_context(message, candidate_text):
        return _build_page_context_contradiction_response(message)
    if _answer_leaks_internal_tool_names(candidate_text):
        return _build_internal_tool_name_leak_response()
    if internal_tool_request and _internal_tool_answer_needs_refusal(message, candidate_text, tool_trace):
        return _build_internal_tool_refusal_response(message, tool_trace)
    if counter_question and _counter_answer_is_unreliable(candidate_text, tool_trace):
        return _build_counter_insufficient_response(tool_trace)
    return candidate_text


def _answer_mentions_unsupported_dashboard_action(candidate_text: str) -> bool:
    normalized = str(candidate_text or "").strip()
    if not normalized:
        return False
    return any(pattern.search(normalized) for pattern in _UNSUPPORTED_DASHBOARD_ACTION_PATTERNS)


def _build_unsupported_dashboard_action_response(message: str, candidate_text: str) -> str:
    normalized = str(candidate_text or "")
    blocked: list[str] = []
    graph_detail_blocked = False
    if re.search(r"\bset\s+otbr\s+slug\b", normalized, re.IGNORECASE):
        blocked.append("set the OTBR slug")
    if re.search(r"\brestart\s+pipeline\b", normalized, re.IGNORECASE):
        blocked.append("restart the pipeline")
    if re.search(r"\btoggl(?:e|ed)\b.*\bcurrent\b.*\bhistorical\b.*\bview", normalized, re.IGNORECASE) or re.search(
        r"\bcurrent\s+and\s+historical\s+views\b", normalized, re.IGNORECASE
    ):
        blocked.append("toggle between current and historical partition views")
    if re.search(r"\bwarning\s+icon\b.*\bgraph\s+diagnostics\b", normalized, re.IGNORECASE):
        blocked.append("click a warning icon in graph diagnostics")
        graph_detail_blocked = True
    if re.search(r"\bgraph\s+diagnostics\s+(?:panel|view)\b", normalized, re.IGNORECASE):
        blocked.append("open a graph diagnostics panel")
        graph_detail_blocked = True
    if re.search(r"\bweak\s+links?\s+(?:view|panel|details?)\b", normalized, re.IGNORECASE):
        blocked.append("open a weak-links detail view")
        graph_detail_blocked = True
    if graph_detail_blocked and _looks_like_chokepoint_question(message):
        return (
            "The current evidence suggests weak-link or high-error edges are the most likely chokepoints, but the current "
            "dashboard does not expose a graph diagnostics panel or weak-links detail view that names the exact node pairs. "
            "So I can say the chokepoints are in the weak-link set, but I cannot identify the specific edge endpoints from "
            "this turn without inventing UI or evidence that is not present."
        )
    actions = ", ".join(blocked) if blocked else "use that dashboard action"
    return (
        "I can’t point you to that dashboard action because the current UI does not expose a control to "
        f"{actions}. I can still help diagnose the issue from the available Thread evidence and describe any "
        "required manual action in plain terms instead of referring to a nonexistent button or menu."
    )


def _answer_leaks_internal_tool_names(candidate_text: str) -> bool:
    normalized = " ".join(str(candidate_text or "").lower().split())
    if not normalized:
        return False
    if not any(
        phrase in normalized
        for phrase in (
            "use ",
            "call ",
            "query ",
            "check ",
            "recommend ",
            "investigate further",
            "tool ",
            "function ",
            "mcp ",
        )
    ):
        return False
    return _looks_like_tool_deferral(candidate_text)


def _answer_mentions_tool_trace_name(candidate_text: str, tool_trace: list[dict[str, Any]]) -> bool:
    normalized = " ".join(str(candidate_text or "").lower().split())
    if not normalized:
        return False
    for row in tool_trace:
        name = str(row.get("name") or "").strip().lower()
        if name and name in normalized:
            return True
    return False


def _build_internal_tool_name_leak_response() -> str:
    return (
        "I shouldn't send you to internal MCP tools or backend function names directly. I should either use those "
        "tools myself and answer from the evidence, or describe the next diagnostic step in plain operator terms."
    )


def _extract_page_context_from_message(message: str) -> dict[str, Any] | None:
    match = re.search(r"(?:^|\n)Page context:\s*(\{.*\})", str(message or ""))
    if not match:
        return None
    try:
        parsed = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _extract_named_count(candidate_text: str, *labels: str) -> int | None:
    normalized = " ".join(str(candidate_text or "").lower().split())
    if not normalized:
        return None
    for label in labels:
        pattern = re.compile(rf"\b{re.escape(label)}\b[^0-9]{{0,24}}(\d+)\b", re.IGNORECASE)
        match = pattern.search(normalized)
        if match:
            try:
                return int(match.group(1))
            except (TypeError, ValueError):
                continue
    return None


def _answer_contradicts_page_context(message: str, candidate_text: str) -> bool:
    page_context = _extract_page_context_from_message(message)
    if not page_context:
        return False
    summary = page_context.get("snapshot_summary")
    if not isinstance(summary, dict):
        return False
    normalized = str(candidate_text or "").strip()
    if not normalized:
        return False
    try:
        partition_count = int(summary.get("partition_count") or 0)
    except (TypeError, ValueError):
        partition_count = 0
    try:
        distinct_thread_networks = int(summary.get("distinct_thread_networks") or 0)
    except (TypeError, ValueError):
        distinct_thread_networks = 0
    try:
        total_nodes = int(summary.get("total_nodes") or 0)
    except (TypeError, ValueError):
        total_nodes = 0
    try:
        stale_nodes = int(summary.get("stale_nodes") or 0)
    except (TypeError, ValueError):
        stale_nodes = 0
    try:
        offline_nodes = int(summary.get("offline_nodes") or 0)
    except (TypeError, ValueError):
        offline_nodes = 0
    try:
        online_nodes = int(summary.get("online_nodes") or 0)
    except (TypeError, ValueError):
        online_nodes = 0
    if partition_count > 1 and any(pattern.search(normalized) for pattern in _PAGE_CONTEXT_SINGLE_PARTITION_PATTERNS):
        return True
    if distinct_thread_networks > 1 and any(pattern.search(normalized) for pattern in _PAGE_CONTEXT_UNIFIED_NETWORK_PATTERNS):
        return True
    if offline_nodes > 0 and any(pattern.search(normalized) for pattern in _PAGE_CONTEXT_NO_OFFLINE_PATTERNS):
        return True
    if stale_nodes > 0 and any(pattern.search(normalized) for pattern in _PAGE_CONTEXT_NO_STALE_PATTERNS):
        return True
    claimed_total = _extract_named_count(normalized, "total nodes", "nodes total")
    if total_nodes > 0 and claimed_total is not None and claimed_total != total_nodes:
        return True
    claimed_offline = _extract_named_count(normalized, "offline nodes", "offline devices", "offline")
    if offline_nodes >= 0 and claimed_offline is not None and claimed_offline != offline_nodes:
        return True
    claimed_online = _extract_named_count(normalized, "online nodes", "healthy nodes", "online")
    if online_nodes > 0 and claimed_online is not None and claimed_online != online_nodes:
        return True
    claimed_stale = _extract_named_count(normalized, "stale nodes", "stale")
    if stale_nodes >= 0 and claimed_stale is not None and claimed_stale != stale_nodes:
        return True
    return False


def _build_page_context_contradiction_response(message: str) -> str:
    page_context = _extract_page_context_from_message(message) or {}
    summary = page_context.get("snapshot_summary") if isinstance(page_context, dict) else {}
    summary = summary if isinstance(summary, dict) else {}
    visible_offline_nodes = page_context.get("visible_offline_nodes") if isinstance(page_context, dict) else []
    visible_offline_nodes = visible_offline_nodes if isinstance(visible_offline_nodes, list) else []
    try:
        partition_count = int(summary.get("partition_count") or 0)
    except (TypeError, ValueError):
        partition_count = 0
    try:
        distinct_thread_networks = int(summary.get("distinct_thread_networks") or 0)
    except (TypeError, ValueError):
        distinct_thread_networks = 0
    try:
        total_nodes = int(summary.get("total_nodes") or 0)
    except (TypeError, ValueError):
        total_nodes = 0
    try:
        online_nodes = int(summary.get("online_nodes") or 0)
    except (TypeError, ValueError):
        online_nodes = 0
    try:
        offline_nodes = int(summary.get("offline_nodes") or 0)
    except (TypeError, ValueError):
        offline_nodes = 0
    try:
        stale_nodes = int(summary.get("stale_nodes") or 0)
    except (TypeError, ValueError):
        stale_nodes = 0
    try:
        active_issue_count = int(summary.get("active_issue_count") or 0)
    except (TypeError, ValueError):
        active_issue_count = 0

    if _looks_like_offline_nodes_question(message):
        if visible_offline_nodes:
            names = []
            for row in visible_offline_nodes[:3]:
                if not isinstance(row, dict):
                    continue
                name = str(row.get("friendly_name") or row.get("name") or row.get("eui64") or "").strip()
                if name:
                    names.append(name)
            listed = ", ".join(names) if names else "the visible offline node"
            return (
                f"The dashboard currently shows {offline_nodes} offline node{'s' if offline_nodes != 1 else ''} out of "
                f"{total_nodes or (online_nodes + offline_nodes)} total. The offline node to investigate first is {listed}. "
                "I am not trusting the contradictory backend summary that claimed there were no offline nodes, because it "
                "does not match the live UI context for this turn."
            )
        return (
            f"The dashboard currently shows {offline_nodes} offline node{'s' if offline_nodes != 1 else ''} out of "
            f"{total_nodes or (online_nodes + offline_nodes)} total, so I should not claim there are none. The live UI and "
            "the gathered backend evidence disagree, and the visible offline node count should win for this turn."
        )

    if _looks_like_overall_health_question(message):
        health_label = "mixed" if (offline_nodes > 0 or distinct_thread_networks > 1 or active_issue_count > 0) else "good"
        concerns: list[str] = []
        if offline_nodes > 0:
            concerns.append(f"{offline_nodes} offline node{'s' if offline_nodes != 1 else ''}")
        if distinct_thread_networks > 1:
            concerns.append(f"{distinct_thread_networks} distinct Thread networks")
        if stale_nodes > 0:
            concerns.append(f"{stale_nodes} stale node{'s' if stale_nodes != 1 else ''}")
        if active_issue_count > 0:
            concerns.append(f"{active_issue_count} active issue{'s' if active_issue_count != 1 else ''}")
        concern_text = ", ".join(concerns) if concerns else "no immediate health alarms"
        return (
            f"Overall health looks {health_label}, not fully clean. The live dashboard currently shows {online_nodes} online / "
            f"{offline_nodes} offline of {total_nodes or (online_nodes + offline_nodes)}, {partition_count} partition"
            f"{'s' if partition_count != 1 else ''}, and {distinct_thread_networks} distinct Thread network"
            f"{'s' if distinct_thread_networks != 1 else ''}. The main concerns right now are {concern_text}. I am using "
            "the live page context here because the gathered backend summary contradicted those visible counts."
        )

    details = []
    if total_nodes > 0:
        details.append(f"{total_nodes} total node{'s' if total_nodes != 1 else ''}")
    if online_nodes > 0 or offline_nodes > 0:
        details.append(f"{online_nodes} online / {offline_nodes} offline")
    if stale_nodes > 0:
        details.append(f"{stale_nodes} stale node{'s' if stale_nodes != 1 else ''}")
    if partition_count > 0:
        details.append(f"{partition_count} partition{'s' if partition_count != 1 else ''}")
    if distinct_thread_networks > 0:
        details.append(f"{distinct_thread_networks} distinct Thread network{'s' if distinct_thread_networks != 1 else ''}")
    details_text = ", ".join(details) if details else "a conflicting dashboard state"
    return (
        "I can't flatten this into a single unified mesh because the current dashboard page context already shows "
        f"{details_text}. The safer conclusion is that the visible UI and the gathered evidence disagree, so this should be "
        "treated as an active discrepancy to investigate rather than claiming there is only one partition."
    )


def _validate_chat_tool_arguments(name: str, arguments: dict[str, Any]) -> dict[str, Any] | None:
    if name in {"get_counter_series", "analyze_node"}:
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
    node_question: bool,
) -> list[str]:
    policies = [
        "Stay grounded in the gathered evidence from this turn; do not invent facts, fields, or timestamps.",
        "If the evidence is insufficient, say so explicitly and name the missing evidence instead of guessing.",
        "Do not tell the user to call internal MCP tools, functions, or backend services themselves.",
        "Do not suggest dashboard controls or clicks that do not exist in the current UI. Avoid nonexistent actions such as setting an OTBR slug or restarting the pipeline from the dashboard.",
        "When the prompt includes Page context, do not contradict its visible counts or status summaries unless you explicitly explain the source disagreement.",
    ]
    if internal_tool_request:
        policies.append("For internal-tool questions, either answer from gathered evidence or refuse clearly; never punt internal tool usage back to the user.")
    if counter_question:
        policies.append("Do not invent node IDs or RF/channel conclusions from empty counter series or missing node inventory evidence.")
    if history_comparison_question:
        policies.append("Do not claim a historical change unless the gathered evidence actually distinguishes the current and historical anchors.")
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
        "gathered in this turn. Do not ask the user to call internal tools or services. If the evidence is insufficient, "
        "say that directly instead of guessing. Keep the answer focused and operator-facing.\n\n"
        f"Audit critique: {critique}"
    )


def _audit_missing_evidence_message(verdict: AuditVerdict) -> str:
    missing_tools = ", ".join(verdict.missing_tool_opportunities) or "the missing evidence bundle"
    critique = verdict.critique or "The first answer did not gather the right evidence for this question."
    return (
        "The first answer skipped needed evidence. Gather one additional evidence bundle now before answering again. "
        f"Prefer these tools if they are available and relevant: {missing_tools}. After that, answer directly from the "
        "observed results. If the evidence is still insufficient, say so plainly instead of guessing.\n\n"
        f"Audit critique: {critique}"
    )


def _answer_review_retry_message(review: AnswerReview) -> str:
    critique = review.critique or "The prior answer was not sufficiently grounded in the gathered evidence."
    return (
        "Revise the prior answer once using the evaluator feedback below. Keep the answer grounded in the evidence already "
        "gathered in this turn. Do not ask the user to call internal tools or services. If the evidence is insufficient, "
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
    node_question: bool,
) -> AnswerReview:
    review_target = _answer_review_target(target)
    body = {
        "model": review_target.model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a strict reviewer for a Thread diagnostics assistant. Review the candidate answer against the "
                    "user request, gathered evidence, and policy bundle. Return JSON only with this shape: "
                    '{"verdict":"pass"|"fail","critique":"short guidance for one retry"}. '
                    "Use verdict fail only when the answer is materially ungrounded, punts internal tool usage back to the "
                    "user, or ignores clear insufficiency in the evidence."
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
    node_question: bool,
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
                    "question, rendered page context, available tool catalog, actual tool trace, and candidate answer. "
                    "Return JSON only with this exact shape: "
                    '{"answered_question":true,"grounded_in_evidence":true,"hallucinated_ui_or_actions":false,'
                    '"tool_choice_ok":true,"missing_tool_opportunities":[],"contains_extraneous_content":false,'
                    '"rewrite_needed":false,"repair_action":"accept"|"rewrite_once"|"gather_missing_evidence_once",'
                    '"critique":"short guidance"}. '
                    "Use gather_missing_evidence_once only when the answer skipped an obviously better available tool or "
                    "missing evidence bundle. Use rewrite_once when the existing tools were sufficient but the answer needs "
                    "to be rewritten to answer directly, stay grounded, or remove extraneous content."
                ),
            },
            {
                "role": "user",
                "content": (
                    "System prompt:\n"
                    f"{system_prompt}\n\n"
                    "User question:\n"
                    f"{user_message}\n\n"
                    "Rendered turn context:\n"
                    f"{context_message}\n\n"
                    "Candidate answer:\n"
                    f"{candidate_text}\n\n"
                    "Policy bundle:\n"
                    + "\n".join(f"- {policy}" for policy in _answer_review_policies(
                        internal_tool_request=internal_tool_request,
                        counter_question=counter_question,
                        history_comparison_question=history_comparison_question,
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
    return _parse_audit_verdict(payload)


def _force_answer_retry_message() -> str:
    return (
        "Answer now from the evidence already gathered. Do not call more tools. If the available evidence is still "
        "insufficient, say that explicitly and name the missing evidence instead of guessing."
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


async def _repair_internal_tool_leak_from_existing_evidence(
    target: DirectChatTarget,
    messages: list[dict[str, Any]],
) -> str:
    body = {
        "model": target.model,
        "messages": [
            *messages,
            {
                "role": "system",
                "content": (
                    "The prior answer leaked internal MCP tools, function names, or backend services to the operator. "
                    "Rewrite the answer once using only the evidence already gathered in this turn. Do not mention "
                    "tools, MCP, functions, or backend services. Answer directly in plain operator language. If the "
                    "evidence is insufficient, say that plainly instead of punting the operator to internal tooling."
                ),
            },
        ],
        "temperature": target.temperature,
        "stream": False,
    }
    payload = await _post_chat_completions(target, body)
    return _extract_message_text(payload)


async def _finalize_candidate_text(
    *,
    target: DirectChatTarget,
    messages: list[dict[str, Any]],
    message: str,
    candidate_text: str,
    tool_trace: list[dict[str, Any]],
    history_comparison_question: bool,
    counter_question: bool,
    internal_tool_request: bool,
) -> str:
    final_candidate = candidate_text
    if tool_trace and not internal_tool_request and not _wants_exact_count_response(message) and (
        _answer_leaks_internal_tool_names(final_candidate)
        or _answer_mentions_tool_trace_name(final_candidate, tool_trace)
    ):
        repaired = await _repair_internal_tool_leak_from_existing_evidence(target, messages)
        if repaired:
            final_candidate = repaired
    return _apply_deterministic_fallbacks(
        message=message,
        candidate_text=final_candidate,
        tool_trace=tool_trace,
        history_comparison_question=history_comparison_question,
        counter_question=counter_question,
        internal_tool_request=internal_tool_request,
    )


async def _gather_backend_history_comparison_evidence(
    tool_trace: list[dict[str, Any]],
) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    anchor_at = now - timedelta(hours=24)
    list_arguments = {
        "since": (now - timedelta(hours=48)).isoformat(),
        "until": now.isoformat(),
        "limit": 200,
    }
    list_result = await _dispatch_chat_tool("list_topology_history", list_arguments)
    tool_trace.append(
        {
            "id": f"backend-{uuid.uuid4()}",
            "type": "function",
            "name": "list_topology_history",
            "arguments": list_arguments,
            "result": list_result,
        }
    )
    snapshots = _extract_snapshot_summaries(list_result)
    newest = snapshots[0] if snapshots else None
    older = None
    for row in snapshots[1:]:
        captured_at = _parse_iso8601(row.get("captured_at") or row.get("ts"))
        if captured_at is not None and captured_at <= anchor_at:
            older = row
            break
    if not newest or not older:
        return {
            "status": "insufficient_history",
            "reason": "No distinct retained snapshot was available for the comparison anchor.",
            "available_snapshots": snapshots[:10],
        }
    if newest.get("id") == older.get("id"):
        return {
            "status": "insufficient_history",
            "reason": "The current and historical anchors resolved to the same snapshot.",
            "available_snapshots": snapshots[:10],
        }
    diff_arguments = {
        "snapshot_id_a": older.get("id"),
        "snapshot_id_b": newest.get("id"),
    }
    diff_result = await _dispatch_chat_tool("diff_topology_history", diff_arguments)
    tool_trace.append(
        {
            "id": f"backend-{uuid.uuid4()}",
            "type": "function",
            "name": "diff_topology_history",
            "arguments": diff_arguments,
            "result": diff_result,
        }
    )
    return {
        "status": "ok",
        "current_snapshot": newest,
        "historical_snapshot": older,
        "diff": _compact_topology_diff(_tool_result_data(diff_result) if isinstance(_tool_result_data(diff_result), dict) else {}),
    }


async def _gather_backend_counter_grounding_evidence(
    tool_trace: list[dict[str, Any]],
) -> dict[str, Any]:
    list_arguments = {"limit": 40}
    list_result = await _dispatch_chat_tool("list_all_nodes", list_arguments)
    tool_trace.append(
        {
            "id": f"backend-{uuid.uuid4()}",
            "type": "function",
            "name": "list_all_nodes",
            "arguments": list_arguments,
            "result": list_result,
        }
    )
    return _compact_node_inventory(list_result)


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


async def _dispatch_chat_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    from ..api import mcp_tools

    if name == _WEB_SEARCH_TOOL_NAME:
        return await web_search.search_web(
            str(arguments.get("query") or ""),
            max_results=int(arguments.get("max_results", 5)),
        )
    if name not in mcp_tools._READ_TOOLS or name in _CHAT_TOOL_EXCLUDE:
        return {"error": f"tool not allowed for chat: {name}"}
    return await mcp_tools._dispatch_and_wrap(name, arguments)


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
    tool_deferral_retry_budget = _tool_deferral_retry_budget(target)
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": _DEFAULT_SYSTEM_PROMPT},
        {"role": "user", "content": context_message},
    ]
    tools = _chat_tools()
    tool_trace: list[dict[str, Any]] = []
    tool_calls_used = 0
    tool_deferral_retries = 0
    audit_rewrite_retries = 0
    audit_evidence_retries = 0
    node_evidence_retries = 0
    history_comparison_retries = 0
    counter_grounding_retries = 0
    topology_history_empty_hints = 0
    final_text = ""
    node_question = _looks_like_node_question(message)
    history_comparison_question = _looks_like_history_comparison_question(message)
    counter_question = _looks_like_counter_or_rf_question(message)
    internal_tool_request = _looks_like_internal_tool_request(message)

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
            if tool_deferral_retries < tool_deferral_retry_budget and _looks_like_tool_deferral(candidate_text):
                tool_deferral_retries += 1
                messages.append(
                    {
                        "role": "user",
                        "content": _tool_deferral_retry_message(tool_deferral_retries),
                    }
                )
                continue
            if (
                history_comparison_question
                and history_comparison_retries < _MAX_HISTORY_COMPARISON_RETRIES
                and _history_comparison_is_unreliable(message, tool_trace)
            ):
                history_comparison_retries += 1
                evidence = await _gather_backend_history_comparison_evidence(tool_trace)
                messages.append(
                    {
                        "role": "system",
                        "content": (
                            "This question compares current state with an older retained topology snapshot. Use the "
                            "authoritative backend evidence below before answering. Do not invent snapshot timestamps, "
                            "and do not treat the same snapshot as both the current and historical anchor. If there is "
                            "no distinct older snapshot, say the retained history is insufficient for that comparison. "
                            "If the question asks about channel changes, do not claim a channel change unless the gathered "
                            "evidence includes channel-specific data. A topology diff alone does not prove a channel change.\n\n"
                            + _serialize_for_prompt(evidence, max_chars=_MAX_EVIDENCE_MESSAGE_CHARS)
                        ),
                    }
                )
                continue
            if (
                counter_question
                and counter_grounding_retries < _MAX_COUNTER_GROUNDING_RETRIES
                and _counter_answer_is_unreliable(candidate_text, tool_trace)
            ):
                counter_grounding_retries += 1
                evidence = await _gather_backend_counter_grounding_evidence(tool_trace)
                messages.append(
                    {
                        "role": "system",
                        "content": (
                            "Counter-based answers must stay grounded in real nodes from the current mesh inventory. Do not "
                            "invent node IDs, do not use placeholder EUI64 values, and do not infer RF or channel-root-cause "
                            "conclusions from empty counter series. Do not recommend config-history or reset-history evidence "
                            "unless that evidence was actually gathered in this turn. "
                            "Use only the current mesh inventory below, or answer that the evidence is insufficient.\n\n"
                            + _serialize_for_prompt(evidence, max_chars=_MAX_EVIDENCE_MESSAGE_CHARS)
                        ),
                    }
                )
                continue
            if node_question and node_evidence_retries < _MAX_NODE_EVIDENCE_RETRIES and not _has_sufficient_node_evidence(tool_trace):
                node_evidence_retries += 1
                evidence = await _gather_backend_node_evidence(message, tool_trace)
                evidence_message = (
                    "This is a node-specific troubleshooting question. Use the authoritative backend evidence "
                    "below before answering. Do not describe the node as long-stable if the recent evidence "
                    "shows a fresh attach, recommission, parent change, partition transition, or other recent "
                    "state change. Explicitly call out recent-change evidence when present.\n\n"
                    + _serialize_for_prompt(evidence, max_chars=_MAX_EVIDENCE_MESSAGE_CHARS)
                    if evidence is not None
                    else "This is a node-specific troubleshooting question. Gather node-specific and recent-change "
                    "evidence yourself before answering."
                )
                messages.append(
                    {
                        "role": "system",
                        "content": evidence_message,
                    }
                )
                continue
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
                node_question=node_question,
            )
            )
            if audit.requires_missing_evidence and audit_evidence_retries < 1:
                audit_evidence_retries += 1
                messages.append(
                    {
                        "role": "system",
                        "content": _audit_missing_evidence_message(audit),
                    }
                )
                continue
            if audit.requires_rewrite and audit_rewrite_retries < 1:
                audit_rewrite_retries += 1
                messages.append(
                    {
                        "role": "system",
                        "content": _audit_retry_message(audit),
                    }
                )
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
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": _serialize_for_prompt(result, max_chars=_MAX_TOOL_RESULT_MESSAGE_CHARS),
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
                            "analyze_node, or start_triage."
                        ),
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
    final_text = await _finalize_candidate_text(
        target=target,
        messages=messages,
        message=context_message,
        candidate_text=final_text,
        tool_trace=tool_trace,
        history_comparison_question=history_comparison_question,
        counter_question=counter_question,
        internal_tool_request=internal_tool_request,
    )
    duration_ms = max(0, int((time.perf_counter() - started) * 1000))
    return {
        "conversation_id": conversation_id or f"direct-{uuid.uuid4()}",
        "agent_id": target.agent_id,
        "response": {"text": final_text, "card": None},
        "tool_calls": tool_trace,
        "duration_ms": duration_ms,
        "model": target.model,
        "streaming": False,
    }