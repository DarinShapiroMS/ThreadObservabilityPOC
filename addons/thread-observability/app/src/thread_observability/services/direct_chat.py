"""Direct model chat client for dashboard requests.

Uses an OpenAI-compatible chat-completions API so the add-on can talk
directly to providers like Cerebras or OpenAI without going through
Home Assistant's Assist agent layer.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

from ..config import AIConfig

_DIRECT_AGENT_PREFIX = "direct:"
_DEFAULT_SYSTEM_PROMPT = (
    "You are the Thread Observability dashboard assistant. Answer using only the provided "
    "Thread dashboard context and the user's request. Be concise, practical, and explicit "
    "about uncertainty when the page context is insufficient."
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
    if not ai.enabled:
        return None
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


async def direct_chat_turn(
    *,
    target: DirectChatTarget,
    message: str,
    rendered_message: str,
    conversation_id: str | None,
) -> dict[str, Any]:
    started = time.perf_counter()
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if target.api_key:
        headers["Authorization"] = f"Bearer {target.api_key}"
    body = {
        "model": target.model,
        "messages": [
            {"role": "system", "content": _DEFAULT_SYSTEM_PROMPT},
            {"role": "user", "content": rendered_message or message},
        ],
        "temperature": target.temperature,
        "stream": False,
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(f"{target.base_url.rstrip('/')}/chat/completions", headers=headers, json=body)
        resp.raise_for_status()
        payload = resp.json()
    duration_ms = max(0, int((time.perf_counter() - started) * 1000))
    text = _extract_message_text(payload if isinstance(payload, dict) else {})
    return {
        "conversation_id": conversation_id or f"direct-{uuid.uuid4()}",
        "agent_id": target.agent_id,
        "response": {"text": text, "card": None},
        "tool_calls": [],
        "duration_ms": duration_ms,
        "model": target.model,
        "streaming": False,
    }