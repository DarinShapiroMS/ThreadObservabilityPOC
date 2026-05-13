"""Direct model chat client for dashboard requests.

Uses an OpenAI-compatible chat-completions API so the add-on can talk
directly to providers like Cerebras or OpenAI without going through
Home Assistant's Assist agent layer.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

from ..config import AIConfig
from . import web_search

_DIRECT_AGENT_PREFIX = "direct:"
_MAX_TOOL_ROUNDS = 4
_MAX_TOOL_CALLS = 8
_MAX_TOOL_DEFERRAL_RETRIES = 1
_MAX_NODE_EVIDENCE_RETRIES = 1
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
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "This is a node-specific troubleshooting question. Before answering, gather node-specific "
                            "and recent-change evidence yourself. Call analyze_node for the node, plus at least one "
                            "history or topology tool such as query_history, get_mesh_state, or start_triage, then "
                            "answer from those observed results."
                        ),
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
                    "content": json.dumps(result, separators=(",", ":"), ensure_ascii=True),
                }
            )
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