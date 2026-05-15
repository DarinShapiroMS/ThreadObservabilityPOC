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
_DEFAULT_SYSTEM_PROMPT = (
    "You are the Thread Observability Thread-network troubleshooting assistant. Answer using only the user's "
    "request, evidence gathered from available diagnostic tools, and retained backend conversation context. "
    "Use tools proactively when current mesh state, counters, history, routing, health, or node-specific evidence is relevant. "
    "For multi-part questions, gather evidence for each requested dimension that the available tools can cover before answering; "
    "do not stop at a partial answer just because one slice is already grounded. "
    "Do not tell the user to run the available diagnostic tools themselves. If a relevant tool exists, call it "
    "yourself before answering. The user does not have direct access to MCP tools, functions, or internal data "
    "services. Never tell the user to call, query, check, inspect, or use those services directly; do that "
    "yourself when possible. Ask the user only for information they uniquely have or for a physical/manual action "
    "you cannot perform from the available backend evidence. "
    "Use web_search when current Matter or Thread specifications, vendor documentation, or other external protocol context "
    "is relevant and the answer is not available from backend evidence alone. "
    "Prefer a node's friendly/display name when present; on first mention include its EUI64 only when that helps "
    "disambiguate. Ground conclusions in tool output, clearly separate observed facts from hypotheses, and mention "
    "when evidence is stale or cache-aged before making a strong claim. Use correct Thread terminology: the Leader "
    "is not a mandatory forwarding hop, parent-child attachment matters for end devices, and RouteTable next-hop "
    "semantics are not generic IP routing. This is an interactive troubleshooting conversation: when multiple "
    "explanations fit the evidence, name the top hypotheses and say what tool result would distinguish them. "
    "Gather obvious diagnostic context before asking the user to restate the problem. Once you have gathered the relevant evidence, "
    "answer concisely in this order: what you found, why it matters, and what to do next. Do not trade completeness for brevity "
    "when the user asks for multiple dimensions of analysis. Do not reason from UI controls, page state, "
    "or view-specific labels. If a claim is not present in backend evidence, do not use it. "
    "Be concise, practical, and explicit about uncertainty only after using the relevant available tools and only when the remaining gap "
    "cannot be closed from the available evidence."
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
            }
            for row in nodes[:20]
            if isinstance(row, dict)
        ],
    }


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
        "Do not reason from UI controls, page state, or display labels. Use backend evidence only.",
        "Do not translate missing evidence into interface advice or invented operator workflows.",
        "Do not answer with self-referential meta commentary about what you should or should not do; answer the user's question directly from evidence.",
        "Do not infer network improvement, better routing, or a better path to OTBR from node counts, link counts, or generic topology diffs alone; require explicit path, parent, route, or OTBR-role evidence before making that claim.",
        "Do not imply that the retained evidence covers the full requested history window when it only spans a shorter interval; state the actual observed coverage and the missing earlier history instead.",
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
                    "question, backend turn context, available tool catalog, actual tool trace, and candidate answer. "
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
                    "Turn context:\n"
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