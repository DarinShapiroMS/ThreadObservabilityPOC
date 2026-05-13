"""Direct model chat client for dashboard requests.

Uses an OpenAI-compatible chat-completions API so the add-on can talk
directly to providers like Cerebras or OpenAI without going through
Home Assistant's Assist agent layer.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from ..config import AIConfig
from . import web_search

_DIRECT_AGENT_PREFIX = "direct:"
_MAX_TOOL_ROUNDS = 4
_MAX_TOOL_CALLS = 8
_MAX_TOOL_DEFERRAL_RETRIES = 1
_MAX_NODE_EVIDENCE_RETRIES = 1
_MAX_TOOL_RESULT_MESSAGE_CHARS = 3500
_MAX_EVIDENCE_MESSAGE_CHARS = 5000
_DEFAULT_SYSTEM_PROMPT = (
    "You are the Thread Observability dashboard troubleshooting assistant. Answer using only the provided "
    "Thread dashboard context, the user's request, and the available diagnostic tools. "
    "Use tools when you need current mesh state, counters, history, or node-specific evidence. "
    "Do not tell the user to run the available diagnostic tools themselves. If a relevant tool exists, call it "
    "yourself before answering. "
    "Use web_search only when outside product or protocol context is actually needed. "
    "Prefer a node's friendly/display name when present; on first mention include its EUI64 only when that helps "
    "disambiguate. Ground conclusions in tool output, clearly separate observed facts from hypotheses, and mention "
    "when evidence is stale or cache-aged before making a strong claim. Use correct Thread terminology: the Leader "
    "is not a mandatory forwarding hop, parent-child attachment matters for end devices, and RouteTable next-hop "
    "semantics are not generic IP routing. This is an interactive troubleshooting conversation: when multiple "
    "explanations fit the evidence, name the top hypotheses and say what tool result would distinguish them. "
    "Gather obvious diagnostic context before asking the user to restate the problem. Prefer concise answers in "
    "this order: what you found, why it matters, and what to do next. Be concise, practical, and explicit about "
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
        "use the \"",
        "use the get_",
        "to investigate further, you can use",
        "it's also a good idea to check",
        "you should use the",
    )
    if any(pattern in normalized for pattern in patterns):
        return True
    return any(tool_name in normalized for tool_name in ("get_mesh_state", "analyze_node", "get_counter_series", "query_history"))


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
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": _DEFAULT_SYSTEM_PROMPT},
        {"role": "user", "content": rendered_message or message},
    ]
    tools = _chat_tools()
    tool_trace: list[dict[str, Any]] = []
    tool_calls_used = 0
    tool_deferral_retries = 0
    node_evidence_retries = 0
    topology_history_empty_hints = 0
    final_text = ""
    node_question = _looks_like_node_question(message)

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
            if tool_deferral_retries < _MAX_TOOL_DEFERRAL_RETRIES and _looks_like_tool_deferral(candidate_text):
                tool_deferral_retries += 1
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "Do not tell me to use the available tools myself. Call the relevant tools now, then "
                            "answer from the observed results."
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
            final_text = candidate_text
            break

        for tool_call in tool_calls:
            tool_calls_used += 1
            if tool_calls_used > _MAX_TOOL_CALLS:
                result = {"error": f"tool call limit exceeded ({_MAX_TOOL_CALLS})"}
            else:
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
        exact_count_response = _build_exact_count_response(message, tool_trace)
        if exact_count_response is not None:
            final_text = exact_count_response
            break
        if tool_calls_used >= _MAX_TOOL_CALLS:
            final_text = "I hit the current tool-call limit while gathering evidence. Please narrow the question."
            break

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