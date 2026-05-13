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


def test_chat_tools_include_web_search_and_safe_read_tools() -> None:
    tools = direct_chat._chat_tools()
    names = {row["function"]["name"] for row in tools}
    assert "get_health_snapshot" in names
    assert "query_history" in names
    assert "web_search" in names
    assert "get_config" not in names
    assert "ha_get_addon_logs" not in names


def test_dispatch_chat_tool_rejects_non_whitelisted_tool() -> None:
    result = asyncio.run(direct_chat._dispatch_chat_tool("ha_restart_addon", {}))
    assert result == {"error": "tool not allowed for chat: ha_restart_addon"}


def test_dispatch_chat_tool_routes_web_search(monkeypatch) -> None:
    async def fake_search(query: str, *, max_results: int = 5) -> dict[str, object]:
        assert query == "matter over thread error 15"
        assert max_results == 3
        return {"query": query, "count": 1, "results": [{"title": "doc", "url": "https://example.com"}]}

    monkeypatch.setattr(direct_chat.web_search, "search_web", fake_search)
    result = asyncio.run(
        direct_chat._dispatch_chat_tool(
            "web_search",
            {"query": "matter over thread error 15", "max_results": 3},
        )
    )
    assert result["count"] == 1


def test_dispatch_chat_tool_allows_other_safe_read_tool(monkeypatch) -> None:
    from thread_observability.api import mcp_tools

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        assert name == "get_timeseries_health"
        return {"data": {"backend": "sqlite"}, "meta": {"tool": name}}

    monkeypatch.setattr(mcp_tools, "_dispatch_and_wrap", fake_dispatch)
    result = asyncio.run(direct_chat._dispatch_chat_tool("get_timeseries_health", {}))
    assert result["data"]["backend"] == "sqlite"


def test_looks_like_tool_deferral_detects_punting_response() -> None:
    text = (
        "To investigate further, you can use the get_counter_series tool and the analyze_node tool. "
        "It is also a good idea to check the mesh state using get_mesh_state."
    )
    assert direct_chat._looks_like_tool_deferral(text) is True


def test_direct_chat_turn_retries_once_when_model_defers_tool_use(monkeypatch) -> None:
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
                            "content": "You can use the get_health_snapshot tool to inspect the current mesh health.",
                        }
                    }
                ]
            }
        if len(calls) == 2:
            retry_message = calls[1]["messages"][-1]
            assert retry_message["role"] == "user"
            assert "Do not tell me to use the available tools myself" in retry_message["content"]
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "call-2",
                                    "type": "function",
                                    "function": {"name": "get_health_snapshot", "arguments": "{}"},
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
                        "content": "I checked the current health snapshot and found no active mesh-wide degradation.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        assert name == "get_health_snapshot"
        return {"data": {"status": "healthy"}, "meta": {"tool": name}}

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Which offline nodes look most suspicious right now?",
            rendered_message="User message: Which offline nodes look most suspicious right now?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == "I checked the current health snapshot and found no active mesh-wide degradation."
    assert len(result["tool_calls"]) == 1
    assert result["tool_calls"][0]["name"] == "get_health_snapshot"


def test_direct_chat_turn_retries_for_node_question_without_history_context(monkeypatch) -> None:
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
                            "content": "The node looks online and stable right now.",
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {"name": "analyze_node", "arguments": '{"eui64":"e6684b9903e8970f"}'},
                                }
                            ],
                        }
                    }
                ]
            }
        if len(calls) == 2:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "The node looks online and stable right now.",
                        }
                    }
                ]
            }
        if len(calls) == 3:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "The node is online now, but recent history shows a very recent re-attach or partition-related change that matters for troubleshooting.",
                        }
                    }
                ]
            }

        assert len(calls) == 3
        return {
            "choices": []
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        if name == "analyze_node":
            return {
                "eui64": arguments["eui64"],
                "node": {
                    "eui64": arguments["eui64"],
                    "friendly_name": "Family Room Track Lights",
                    "partition_id": 1846206278,
                    "attach_attempt_count": 1,
                    "partition_id_change_count": 1,
                },
                "timeline": [{"ts": "2026-05-13T15:00:00Z", "kind": "re_attached_node"}],
                "open_issues": [],
                "recent_issues": [],
                "physical_identity": None,
            }
        if name == "query_history":
            assert arguments["eui64"] == "e6684b9903e8970f"
            assert "since" in arguments
            return [{"ts": "2026-05-13T15:00:00Z", "kind": "re_attached_node", "details": {"partition_id": 2107240925}}]
        if name == "get_mesh_state":
            assert arguments["freshness_minutes"] == 120
            return {
                "computed_at": "2026-05-13T15:05:00Z",
                "partition_id": 1846206278,
                "nodes": [
                    {
                        "eui64": "e6684b9903e8970f",
                        "partition_id": 1846206278,
                        "status": "online",
                    }
                ],
                "links": [],
            }
        raise AssertionError(f"unexpected tool {name}")

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Tell me what is going on with node e6684b9903e8970f.",
            rendered_message="User message: Tell me what is going on with node e6684b9903e8970f.",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == "The node is online now, but recent history shows a very recent re-attach or partition-related change that matters for troubleshooting."
    assert [row["name"] for row in result["tool_calls"]] == ["analyze_node", "query_history", "get_mesh_state"]