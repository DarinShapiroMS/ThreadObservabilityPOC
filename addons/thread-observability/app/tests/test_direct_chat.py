"""Unit tests for direct-model chat orchestration."""

from __future__ import annotations

import asyncio
import json

from thread_observability.services import direct_chat


def test_direct_chat_turn_executes_mcp_tool_and_returns_trace(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama-4-scout",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )
    calls: list[dict[str, object]] = []

    async def fake_post_chat_completions(target, body):  # noqa: ANN001
        calls.append(json.loads(json.dumps(body)))
        if len(calls) == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {
                                        "name": "get_health_snapshot",
                                        "arguments": "{}",
                                    },
                                }
                            ],
                        }
                    }
                ]
            }
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "The mesh looks healthy based on the latest snapshot.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        assert name == "get_health_snapshot"
        assert arguments == {}
        return {"data": {"status": "healthy"}, "meta": {"tool": name}}

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="What looks wrong?",
            rendered_message="User message: What looks wrong?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == "The mesh looks healthy based on the latest snapshot."
    assert len(result["tool_calls"]) == 1
    assert result["tool_calls"][0]["name"] == "get_health_snapshot"
    assert result["tool_calls"][0]["result"]["data"]["status"] == "healthy"
    assert len(calls) == 2
    assert calls[0]["tools"]
    tool_message = next(msg for msg in calls[1]["messages"] if msg.get("role") == "tool")
    assert tool_message["role"] == "tool"
    assert json.loads(tool_message["content"])["data"]["status"] == "healthy"


def test_parse_tool_arguments_accepts_json_string() -> None:
    assert direct_chat._parse_tool_arguments('{"eui64":"AA"}') == {"eui64": "AA"}


def test_dispatch_chat_tool_rejects_non_whitelisted_tool() -> None:
    result = asyncio.run(direct_chat._dispatch_chat_tool("ha_restart_addon", {}))
    assert result == {"error": "tool not allowed for chat: ha_restart_addon"}