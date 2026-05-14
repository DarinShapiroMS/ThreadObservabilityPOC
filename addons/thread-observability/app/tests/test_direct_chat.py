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


def test_direct_chat_turn_compacts_large_tool_results_for_prompt(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama-4-scout",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )
    calls: list[dict[str, object]] = []
    large_result = {
        "rows": [
            {
                "eui64": f"node-{index:04d}",
                "notes": "x" * 400,
                "metrics": {"rssi": -70, "lqi": 3, "status": "online"},
            }
            for index in range(80)
        ]
    }

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
                                    "id": "call-big",
                                    "type": "function",
                                    "function": {
                                        "name": "list_all_nodes",
                                        "arguments": "{}",
                                    },
                                }
                            ],
                        }
                    }
                ]
            }
        tool_message = next(msg for msg in body["messages"] if msg.get("role") == "tool")
        assert len(tool_message["content"]) <= direct_chat._MAX_TOOL_RESULT_MESSAGE_CHARS
        assert "_truncated_items" in tool_message["content"] or "[truncated" in tool_message["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "I reviewed the node inventory.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        assert name == "list_all_nodes"
        assert arguments == {}
        return large_result

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="List the current nodes.",
            rendered_message="User message: List the current nodes.",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == "I reviewed the node inventory."
    assert len(result["tool_calls"]) == 1
    assert len(result["tool_calls"][0]["result"]["rows"]) == 80


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


def test_looks_like_tool_deferral_detects_internal_service_recommendation() -> None:
    text = (
        "I would recommend calling the get_topology_history_entry function. "
        "The user can query that internal data service directly to investigate further."
    )
    assert direct_chat._looks_like_tool_deferral(text) is True


def test_tool_deferral_retry_budget_is_model_aware() -> None:
    default_target = direct_chat.DirectChatTarget(
        provider="openai",
        model="gpt-5.4",
        base_url="https://api.openai.com/v1",
        api_key="secret",
        temperature=0.2,
    )
    smaller_target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama3.1-8b",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )

    assert direct_chat._tool_deferral_retry_budget(default_target) == 1
    assert direct_chat._tool_deferral_retry_budget(smaller_target) == 2


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


def test_direct_chat_turn_retries_when_model_tells_user_to_call_internal_service(monkeypatch) -> None:
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
                            "content": (
                                "I would recommend calling the get_topology_history_entry function directly. "
                                "You can query that internal data service to investigate further."
                            ),
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
                            "content": "I checked the available evidence and the retained topology history is insufficient to answer that directly.",
                        }
                    }
                ]
            }
        raise AssertionError(f"unexpected call count {len(calls)}")

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="When did the channel change?",
            rendered_message="User message: When did the channel change?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == "I checked the available evidence and the retained topology history is insufficient to answer that directly."
    assert result["tool_calls"] == []


def test_direct_chat_turn_uses_second_retry_for_model_profile(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama3.1-8b",
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
                            "content": "I would recommend calling the get_health_snapshot function directly.",
                        }
                    }
                ]
            }
        if len(calls) == 3:
            retry_message = calls[2]["messages"][-1]
            assert retry_message["role"] == "user"
            assert "Do not ask me to call internal MCP tools" in retry_message["content"]
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
        raise AssertionError(f"unexpected call count {len(calls)}")

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Which offline nodes look most suspicious right now?",
            rendered_message="User message: Which offline nodes look most suspicious right now?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == "I checked the current health snapshot and found no active mesh-wide degradation."
    assert result["tool_calls"] == []


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


def test_direct_chat_turn_guides_away_from_empty_topology_history_fallback(monkeypatch) -> None:
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
                                    "id": "call-topo-1",
                                    "type": "function",
                                    "function": {"name": "list_topology_history", "arguments": '{"limit": 10}'},
                                }
                            ],
                        }
                    }
                ]
            }
        if len(calls) == 2:
            note = next(
                msg for msg in body["messages"]
                if msg.get("role") == "system" and "Topology history returned no persisted snapshots" in str(msg.get("content") or "")
            )
            assert "Do not call get_topology_history_entry with empty arguments" in note["content"]
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "call-topo-2",
                                    "type": "function",
                                    "function": {"name": "get_mesh_state", "arguments": "{}"},
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
                        "content": "There are no persisted topology snapshots for that window, so I used current mesh state instead.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        if name == "list_topology_history":
            return {"snapshots": [], "count": 0}
        if name == "get_mesh_state":
            return {"partition_id": 1846206278, "nodes": [], "links": [], "node_count": 0, "link_count": 0}
        raise AssertionError(f"unexpected tool {name}")

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Which devices recently changed partitions?",
            rendered_message="User message: Which devices recently changed partitions?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == "There are no persisted topology snapshots for that window, so I used current mesh state instead."
    assert [row["name"] for row in result["tool_calls"]] == ["list_topology_history", "get_mesh_state"]


def test_direct_chat_turn_returns_exact_counts_from_tool_results(monkeypatch) -> None:
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
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "call-topo",
                                "type": "function",
                                "function": {"name": "list_topology_history", "arguments": '{"limit": 100}'},
                            },
                            {
                                "id": "call-stats",
                                "type": "function",
                                "function": {"name": "get_storage_stats", "arguments": "{}"},
                            },
                        ],
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        if name == "list_topology_history":
            return {"data": {"snapshots": [{"id": 71}], "count": 71}, "meta": {"tool": name}}
        if name == "get_storage_stats":
            return {
                "data": {
                    "sqlite": {"row_counts": {"topology_snapshots": 71}},
                },
                "meta": {"tool": name},
            }
        raise AssertionError(f"unexpected tool {name}")

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Call list_topology_history and get_storage_stats, then answer with just the two counts.",
            rendered_message="User message: Call list_topology_history and get_storage_stats, then answer with just the two counts.",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == (
        "list_topology_history.count=71; "
        "get_storage_stats.sqlite.row_counts.topology_snapshots=71"
    )
    assert [row["name"] for row in result["tool_calls"]] == ["list_topology_history", "get_storage_stats"]
    assert len(calls) == 1


def test_direct_chat_turn_retries_when_history_comparison_uses_same_snapshot(monkeypatch) -> None:
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
                                    "id": "call-now",
                                    "type": "function",
                                    "function": {"name": "get_topology_history_entry", "arguments": '{"at":"2026-05-13T17:00:00Z"}'},
                                },
                                {
                                    "id": "call-24h",
                                    "type": "function",
                                    "function": {"name": "get_topology_history_entry", "arguments": '{"at":"2026-05-12T17:00:00Z"}'},
                                },
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
                            "content": "The channel did not change over the last 24 hours.",
                        }
                    }
                ]
            }
        history_note = next(
            msg for msg in calls[2]["messages"]
            if msg.get("role") == "system" and "same snapshot as both the current and historical anchor" in str(msg.get("content") or "")
        )
        assert "retained history is insufficient" in history_note["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "I found only one distinct retained snapshot for that comparison window, so I can't determine whether the channel changed in the last 24 hours.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        if name == "get_topology_history_entry":
            return {
                "data": {"id": 71, "captured_at": "2026-05-13T17:00:00Z", "partition_id": 1846206278},
                "meta": {"tool": name},
            }
        if name == "list_topology_history":
            return {
                "data": {
                    "snapshots": [
                        {"id": 71, "captured_at": "2026-05-13T17:00:00Z", "partition_id": 1846206278, "node_count": 18, "link_count": 122},
                    ],
                    "count": 1,
                },
                "meta": {"tool": name},
            }
        raise AssertionError(f"unexpected tool {name}")

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Did the channel change between now and 24h ago?",
            rendered_message="User message: Did the channel change between now and 24h ago?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == (
        "I don't have channel-specific history for the retained comparison anchors, so I can't determine whether the Thread channel changed in that window."
    )
    assert [row["name"] for row in result["tool_calls"]] == [
        "get_topology_history_entry",
        "get_topology_history_entry",
        "list_topology_history",
    ]


def test_direct_chat_turn_retries_when_counter_series_is_empty_and_answer_invents_node(monkeypatch) -> None:
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
                                    "id": "call-counters",
                                    "type": "function",
                                    "function": {"name": "get_counter_series", "arguments": '{"eui64":"0004a30b86a40000"}'},
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
                            "content": "Node 0004a30b86a40000 shows RF retries that likely drove the channel change.",
                        }
                    }
                ]
            }
        note = next(
            msg for msg in calls[2]["messages"]
            if msg.get("role") == "system" and "Do not invent node IDs" in str(msg.get("content") or "")
        )
        assert "empty counter series" in note["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "I can't attribute the channel change to RF on a specific node because the counter series was empty and that node does not appear in the current mesh inventory.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        if name == "get_counter_series":
            return {
                "data": {"eui64": arguments["eui64"], "series": [], "deltas": {}},
                "meta": {"tool": name},
            }
        if name == "list_all_nodes":
            return {
                "data": {
                    "count": 1,
                    "nodes": [
                        {
                            "eui64": "e6684b9903e8970f",
                            "friendly_name": "Family Room Track Lights",
                            "status": "online",
                            "partition_id": 1846206278,
                        }
                    ],
                },
                "meta": {"tool": name},
            }
        raise AssertionError(f"unexpected tool {name}")

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Did RF conditions cause the channel change?",
            rendered_message="User message: Did RF conditions cause the channel change?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == (
        "I can't determine whether RF conditions caused the channel change from the available evidence because the returned counter series was empty."
    )
    assert [row["name"] for row in result["tool_calls"]] == ["get_counter_series", "list_all_nodes"]


def test_direct_chat_turn_forces_final_answer_after_tool_limit(monkeypatch) -> None:
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
                                    "id": "call-mesh",
                                    "type": "function",
                                    "function": {"name": "get_mesh_state", "arguments": "{}"},
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
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "call-issues",
                                    "type": "function",
                                    "function": {"name": "list_active_issues", "arguments": "{}"},
                                }
                            ],
                        }
                    }
                ]
            }
        assert "tools" not in calls[2]
        note = calls[2]["messages"][-1]
        assert note["role"] == "system"
        assert "Do not call more tools" in note["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "Based on the evidence gathered so far, the likely choke point is the single upstream partition path; I don't need more tools to say that confidently.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        assert name == "get_mesh_state"
        return {"data": {"nodes": [], "links": []}, "meta": {"tool": name}}

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)
    monkeypatch.setattr(direct_chat, "_MAX_TOOL_CALLS", 1)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="What is the likely choke point in this mesh, and would placement changes help?",
            rendered_message="User message: What is the likely choke point in this mesh, and would placement changes help?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == (
        "Based on the evidence gathered so far, the likely choke point is the single upstream partition path; I don't need more tools to say that confidently."
    )
    assert [row["name"] for row in result["tool_calls"]] == ["get_mesh_state", "list_active_issues"]
    assert result["tool_calls"][1]["result"] == {"error": "tool call limit exceeded (1)"}


def test_direct_chat_turn_does_not_map_topology_diff_to_channel_change(monkeypatch) -> None:
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
                                    "id": "call-now",
                                    "type": "function",
                                    "function": {"name": "get_topology_history_entry", "arguments": '{"at":"now"}'},
                                },
                                {
                                    "id": "call-24h",
                                    "type": "function",
                                    "function": {"name": "get_topology_history_entry", "arguments": '{"at":"24h ago"}'},
                                },
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
                        "content": "The channel has changed between now and 24h ago. The current channel is different from the one 24 hours ago.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        if name == "get_topology_history_entry":
            return {
                "data": {"id": 73, "captured_at": "2026-05-14T01:04:46Z", "partition_id": 1846206278},
                "meta": {"tool": name},
            }
        if name == "list_topology_history":
            return {
                "data": {
                    "snapshots": [
                        {"id": 73, "captured_at": "2026-05-14T01:04:46Z", "partition_id": 1846206278, "node_count": 19, "link_count": 59},
                        {"id": 44, "captured_at": "2026-05-13T00:53:46Z", "partition_id": 2107240925, "node_count": 15, "link_count": 48},
                    ],
                    "count": 2,
                },
                "meta": {"tool": name},
            }
        if name == "diff_topology_history":
            return {
                "data": {
                    "snapshot_id_a": 44,
                    "snapshot_id_b": 73,
                    "added_nodes": [{"eui64": "e6684b9903e8970f"}],
                    "removed_nodes": [],
                    "changed_nodes": [],
                    "added_links": [],
                    "removed_links": [],
                    "summary": {"added_node_count": 1, "removed_node_count": 0, "changed_node_count": 0},
                },
                "meta": {"tool": name},
            }
        raise AssertionError(f"unexpected tool {name}")

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Did the channel change between now and 24h ago?",
            rendered_message="User message: Did the channel change between now and 24h ago?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == (
        "I can see retained topology changes between the comparison snapshots (1 added nodes, 0 removed nodes, 0 changed nodes), "
        "but I don't have channel-specific history for those anchors, so I can't determine whether the Thread channel changed."
    )
    assert [row["name"] for row in result["tool_calls"]] == [
        "get_topology_history_entry",
        "get_topology_history_entry",
        "list_topology_history",
        "diff_topology_history",
    ]
    assert len(calls) == 3


def test_direct_chat_turn_rejects_placeholder_counter_node_and_unsupported_follow_up(monkeypatch) -> None:
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
                                    "id": "call-counters",
                                    "type": "function",
                                    "function": {"name": "get_counter_series", "arguments": '{"eui64":"your_node_eui64","counter_names":["channel_change"]}'},
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
                            "content": "The node that changed channels shows no increments on the \"channel_change\" counter, so this was likely a config change or reset. Check configuration history and reset history next.",
                        }
                    }
                ]
            }
        note = next(
            msg for msg in calls[2]["messages"]
            if msg.get("role") == "system" and "Do not recommend config-history or reset-history evidence" in str(msg.get("content") or "")
        )
        assert "placeholder EUI64 values" in note["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "The node that changed channels shows no increments on the \"channel_change\" counter, so this was likely a config change or reset. Check configuration history and reset history next.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        if name == "list_all_nodes":
            return {
                "data": {
                    "count": 1,
                    "nodes": [
                        {
                            "eui64": "e6684b9903e8970f",
                            "friendly_name": "Family Room Track Lights",
                            "status": "online",
                            "partition_id": 1846206278,
                        }
                    ],
                },
                "meta": {"tool": name},
            }
        raise AssertionError(f"unexpected tool {name}")

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Did RF conditions cause the channel change?",
            rendered_message="User message: Did RF conditions cause the channel change?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == (
        "I can't determine whether RF conditions caused the channel change from the available evidence because the counter query was not grounded to a real 16-hex EUI64 from the mesh inventory and the returned counter series was empty."
    )
    assert result["tool_calls"][0]["name"] == "get_counter_series"
    assert result["tool_calls"][0]["result"] == {"error": "invalid eui64 argument: expected 16 hex characters"}
    assert result["tool_calls"][1]["name"] == "list_all_nodes"
    assert len(calls) == 3