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

    async def fake_audit(*args, **kwargs):  # noqa: ANN002, ANN003
        return direct_chat.AuditVerdict()

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)
    monkeypatch.setattr(direct_chat, "_audit_answer_candidate", fake_audit)

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
    assert result["transcript"]["kind"] == "direct_chat"
    assert any(event["kind"] == "assistant_completion" for event in result["transcript"]["events"])
    assert any(event["kind"] == "tool_result" for event in result["transcript"]["events"])


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


def test_direct_chat_turn_preserves_signal_strength_fields_in_node_inventory_prompt(monkeypatch) -> None:
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
                                    "id": "call-signal",
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
        tool_payload = json.loads(tool_message["content"])
        first_node = tool_payload["nodes"][0]
        assert first_node["friendly_name"] == "Bedroom Sensor"
        assert first_node["signal_strength"]["rssi"] == -88
        assert first_node["signal_strength"]["lqi"] == 90
        assert first_node["signal_strength"]["strongest_available_rssi"] == -88
        assert first_node["signal_strength"]["best_reporter_name"] == "Hallway Router"
        assert first_node["signal_strength"]["best_reporter_eui64"] == "8899aabbccddeeff"
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "Bedroom Sensor has the weakest reported signal in the current node inventory.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        assert name == "list_all_nodes"
        assert arguments == {}
        return {
            "count": 1,
            "nodes": [
                {
                    "eui64": "0011223344556677",
                    "friendly_name": "Bedroom Sensor",
                    "status": "healthy",
                    "partition_id": 1234,
                    "signal_strength": {
                        "rssi": -88,
                        "lqi": 90,
                        "strongest_available_rssi": -88,
                        "strongest_available_lqi": 90,
                        "best_reporter": {
                            "eui64": "8899aabbccddeeff",
                            "name": "Hallway Router",
                            "rssi": -88,
                            "lqi": 90,
                            "is_child": True,
                        },
                        "source": "links",
                    },
                }
            ],
        }

    async def fake_audit(*args, **kwargs):  # noqa: ANN002, ANN003
        return direct_chat.AuditVerdict()

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)
    monkeypatch.setattr(direct_chat, "_audit_answer_candidate", fake_audit)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Which of my devices have the lowest signal quality?",
            rendered_message="User message: Which of my devices have the lowest signal quality?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == "Bedroom Sensor has the weakest reported signal in the current node inventory."
    assert [row["name"] for row in result["tool_calls"]] == ["list_all_nodes"]


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


def test_audit_answer_candidate_uses_isolated_evaluator_model(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="openai",
        model="gpt-4.1",
        base_url="https://api.openai.com/v1",
        api_key="secret",
        temperature=0.2,
    )
    seen: list[tuple[str, dict[str, object]]] = []

    async def fake_post_chat_completions(target, body):  # noqa: ANN001
        seen.append((target.model, json.loads(json.dumps(body))))
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": (
                            '{"answered_question":false,"grounded_in_evidence":false,'
                            '"hallucinated_ui_or_actions":false,"tool_choice_ok":true,'
                            '"missing_tool_opportunities":[],"contains_extraneous_content":false,'
                            '"rewrite_needed":true,"repair_action":"rewrite_once",'
                            '"critique":"The answer claims stability without evidence."}'
                        ),
                    }
                }
            ]
        }

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)

    review = asyncio.run(
        direct_chat._audit_answer_candidate(
            target,
            system_prompt=direct_chat._DEFAULT_SYSTEM_PROMPT,
            user_message="What changed recently?",
            context_message="User message: What changed recently?",
            candidate_text="Everything is stable.",
            available_tools=direct_chat._chat_tools(),
            tool_trace=[{"name": "get_health_snapshot", "arguments": {}, "result": {"data": {"status": "stale"}}}],
            internal_tool_request=False,
            counter_question=False,
            history_comparison_question=False,
            node_question=False,
        )
    )

    assert review.failed is True
    assert "stability" in review.critique.lower()
    assert seen[0][0] == "gpt-4o-mini"
    assert "Policy bundle" in str(seen[0][1]["messages"][1]["content"])
    assert "Available tool catalog" in str(seen[0][1]["messages"][1]["content"])
    assert "Actual tool trace" in str(seen[0][1]["messages"][1]["content"])


def test_direct_chat_turn_retries_once_when_audit_requests_rewrite(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama-4-scout",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )
    calls: list[dict[str, object]] = []
    reviews = [
        direct_chat.AuditVerdict(
            answered_question=False,
            grounded_in_evidence=False,
            rewrite_needed=True,
            repair_action="rewrite_once",
            critique="The answer overstates certainty; say the evidence is insufficient.",
        ),
        direct_chat.AuditVerdict(),
    ]

    async def fake_post_chat_completions(target, body):  # noqa: ANN001
        calls.append(json.loads(json.dumps(body)))
        if len(calls) == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "The network is definitely stable.",
                        }
                    }
                ]
            }
        critique = next(
            msg for msg in body["messages"]
            if msg.get("role") == "system" and "Audit critique:" in str(msg.get("content") or "")
        )
        assert "overstates certainty" in critique["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "The current evidence is insufficient to confirm that the network is stable.",
                    }
                }
            ]
        }

    async def fake_audit(*args, **kwargs):  # noqa: ANN002, ANN003
        return reviews.pop(0)

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_audit_answer_candidate", fake_audit)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Is the mesh stable right now?",
            rendered_message="User message: Is the mesh stable right now?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == "The current evidence is insufficient to confirm that the network is stable."
    assert len(calls) == 2


def test_direct_chat_turn_gathers_missing_evidence_once_when_audit_requests_it(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama-4-scout",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )
    calls: list[dict[str, object]] = []
    audits = [
        direct_chat.AuditVerdict(
            answered_question=False,
            grounded_in_evidence=False,
            tool_choice_ok=False,
            missing_tool_opportunities=["get_health_snapshot"],
            repair_action="gather_missing_evidence_once",
            critique="The answer skipped the current health snapshot.",
        ),
        direct_chat.AuditVerdict(),
    ]

    async def fake_post_chat_completions(target, body):  # noqa: ANN001
        calls.append(json.loads(json.dumps(body)))
        if len(calls) == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "The network seems fine.",
                        }
                    }
                ]
            }
        if len(calls) == 2:
            audit_note = next(
                msg for msg in body["messages"]
                if msg.get("role") == "system" and "Gather one additional evidence bundle now" in str(msg.get("content") or "")
            )
            assert "get_health_snapshot" in audit_note["content"]
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "call-health",
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
                        "content": "The latest health snapshot shows one offline node, so the network is not fully healthy.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        assert name == "get_health_snapshot"
        assert arguments == {}
        return {"data": {"status": "degraded", "offline_nodes": 1}, "meta": {"tool": name}}

    async def fake_audit(*args, **kwargs):  # noqa: ANN002, ANN003
        return audits.pop(0)

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)
    monkeypatch.setattr(direct_chat, "_audit_answer_candidate", fake_audit)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="What is the overall health of my network right now?",
            rendered_message="User message: What is the overall health of my network right now?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == "The latest health snapshot shows one offline node, so the network is not fully healthy."
    assert [row["name"] for row in result["tool_calls"]] == ["get_health_snapshot"]
    assert len(calls) == 3


def _obsolete_test_direct_chat_turn_repairs_internal_tool_leak_from_existing_evidence(monkeypatch) -> None:
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
                            "content": "Use get_mesh_state and list_all_nodes to inspect the weak_link edges around Family Room Track Lights and Front Porch Lights.",
                        }
                    }
                ]
            }
        if len(calls) == 3:
            retry_prompt = calls[2]["messages"][-1]
            assert retry_prompt["role"] == "user"
            assert "Do not tell me to use the available tools myself" in retry_prompt["content"]
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "The evidence from get_mesh_state and list_all_nodes points to the weak_link corridor around Family Room Track Lights and Front Porch Lights.",
                        }
                    }
                ]
            }
        repair_prompt = calls[3]["messages"][-1]
        assert repair_prompt["role"] == "system"
        assert "Do not mention tools, MCP, functions, or backend services" in repair_prompt["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "The weakest links right now are around Family Room Track Lights and Front Porch Lights, so that corridor is the likeliest chokepoint area.",
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        assert name == "get_mesh_state"
        return {
            "data": {
                "nodes": [{"eui64": "e6684b9903e8970f", "friendly_name": "Family Room Track Lights"}],
                "links": [{"from": "e6684b9903e8970f", "to": "42a51d07266062b5", "tags": ["weak_link"]}],
            },
            "meta": {"tool": name},
        }

    async def fake_audit(*args, **kwargs):  # noqa: ANN002, ANN003
        return direct_chat.AuditVerdict()

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)
    monkeypatch.setattr(direct_chat, "_audit_answer_candidate", fake_audit)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="What are the chokepoints in my network right now?",
            rendered_message="User message: What are the chokepoints in my network right now?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == (
        "The weakest links right now are around Family Room Track Lights and Front Porch Lights, so that corridor is the likeliest chokepoint area."
    )
    assert [row["name"] for row in result["tool_calls"]] == ["get_mesh_state"]
    assert len(calls) == 4


def _obsolete_test_direct_chat_turn_retries_when_model_tells_user_to_call_internal_service(monkeypatch) -> None:
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


def _obsolete_test_direct_chat_turn_uses_second_retry_for_model_profile(monkeypatch) -> None:
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


def _obsolete_test_direct_chat_turn_retries_for_node_question_without_history_context(monkeypatch) -> None:
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


def _obsolete_test_direct_chat_turn_retries_when_history_comparison_uses_same_snapshot(monkeypatch) -> None:
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


def _obsolete_test_direct_chat_turn_retries_when_counter_series_is_empty_and_answer_invents_node(monkeypatch) -> None:
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


def _obsolete_test_direct_chat_turn_does_not_map_topology_diff_to_channel_change(monkeypatch) -> None:
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

    async def fake_audit(*args, **kwargs):  # noqa: ANN002, ANN003
        return direct_chat.AuditVerdict()

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)
    monkeypatch.setattr(direct_chat, "_audit_answer_candidate", fake_audit)

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


def _obsolete_test_direct_chat_turn_rejects_placeholder_counter_node_and_unsupported_follow_up(monkeypatch) -> None:
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

    async def fake_audit(*args, **kwargs):  # noqa: ANN002, ANN003
        return direct_chat.AuditVerdict()

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)
    monkeypatch.setattr(direct_chat, "_audit_answer_candidate", fake_audit)

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


def _obsolete_test_direct_chat_turn_does_not_map_topology_diff_to_channel_stability_claim(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama-4-scout",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )

    async def fake_post_chat_completions(target, body):  # noqa: ANN001
        if not hasattr(fake_post_chat_completions, "calls"):
            fake_post_chat_completions.calls = []
        fake_post_chat_completions.calls.append(json.loads(json.dumps(body)))
        if len(fake_post_chat_completions.calls) == 1:
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
                        "content": "The channel did not change between now and 24h ago. It is the same channel in both retained snapshots.",
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


def _obsolete_test_direct_chat_turn_refuses_internal_tool_request_when_counter_call_uses_null_eui64(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama3.1-8b",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )

    async def fake_post_chat_completions(target, body):  # noqa: ANN001
        if not hasattr(fake_post_chat_completions, "calls"):
            fake_post_chat_completions.calls = []
        fake_post_chat_completions.calls.append(json.loads(json.dumps(body)))
        if len(fake_post_chat_completions.calls) == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "call-counters-1",
                                    "type": "function",
                                    "function": {
                                        "name": "get_counter_series",
                                        "arguments": '{"eui64":null,"counter_names":["tx_retry","tx_err_cca"]}',
                                    },
                                }
                            ],
                        }
                    }
                ]
            }
        if len(fake_post_chat_completions.calls) >= 2:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": (
                                "Based on the current evidence, the selected node EUI64 is still null. This means that I do "
                                "not have any information about the node that is experiencing the issue. Therefore, I do not "
                                "have enough evidence to determine whether RF caused the channel change. To proceed, please "
                                "select a node from the dashboard."
                            ),
                        }
                    }
                ]
            }
        raise AssertionError("unexpected model call")

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
            message="What internal MCP tool should I call to verify whether RF caused the channel change?",
            rendered_message="User message: What internal MCP tool should I call to verify whether RF caused the channel change?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == (
        "I can't ask you to call internal MCP tools directly. I can't determine whether RF conditions caused the channel "
        "change from the available evidence because the counter query was not grounded to a real 16-hex EUI64 from the mesh "
        "inventory and the returned counter series was empty."
    )
    assert result["tool_calls"][0]["result"] == {"error": "invalid eui64 argument: expected 16 hex characters"}
    assert result["tool_calls"][-1]["name"] == "list_all_nodes"


def _obsolete_test_direct_chat_turn_uses_history_insufficient_fallback_when_only_current_mesh_state_is_available(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama3.1-8b",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )

    async def fake_post_chat_completions(target, body):  # noqa: ANN001
        if not hasattr(fake_post_chat_completions, "calls"):
            fake_post_chat_completions.calls = []
        fake_post_chat_completions.calls.append(json.loads(json.dumps(body)))
        if len(fake_post_chat_completions.calls) == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "call-mesh-1",
                                    "type": "function",
                                    "function": {"name": "get_mesh_state", "arguments": "{}"},
                                }
                            ],
                        }
                    }
                ]
            }
        if len(fake_post_chat_completions.calls) >= 2:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": (
                                "The current channel is 11. However, the available evidence is insufficient to determine if "
                                "the channel changed between now and 24h ago."
                            ),
                        }
                    }
                ]
            }
        raise AssertionError("unexpected model call")

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        if name == "get_mesh_state":
            return {
                "data": {
                    "channel": 11,
                    "node_count": 19,
                    "partition_id": 1846206278,
                },
                "meta": {"tool": name},
            }
        if name == "list_topology_history":
            return {
                "data": {
                    "count": 0,
                    "snapshots": [],
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
        "I don't have channel-specific history for the retained comparison anchors, so I can't determine whether the "
        "Thread channel changed in that window."
    )
    assert [row["name"] for row in result["tool_calls"]] == ["get_mesh_state", "list_topology_history"]


def _obsolete_test_direct_chat_turn_falls_back_when_rf_answer_invents_get_node_history_and_requests_eui64(monkeypatch) -> None:
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
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "call-counters-1",
                                    "type": "function",
                                    "function": {
                                        "name": "get_counter_series",
                                        "arguments": '{"eui64":null,"counter_names":["tx_retry","tx_err_cca"]}',
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
                        "content": (
                            "Unfortunately, the available evidence is insufficient to determine if RF conditions caused the "
                            "channel change. We don't have information about the node's EUI64, which is required to retrieve "
                            "its counter series. To gather more evidence, please provide the EUI64 of the node you are interested "
                            "in, or select one of the nodes from the current mesh inventory."
                        ),
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
        "I can't determine whether RF conditions caused the channel change from the available evidence because the counter "
        "query was not grounded to a real 16-hex EUI64 from the mesh inventory and the returned counter series was empty."
    )
    assert result["tool_calls"][0]["result"] == {"error": "invalid eui64 argument: expected 16 hex characters"}
    assert result["tool_calls"][-1]["name"] == "list_all_nodes"


def _obsolete_test_direct_chat_turn_clamps_forced_history_answer_without_history_tools(monkeypatch) -> None:
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
        if len(calls) <= 2:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": f"call-mesh-{len(calls)}",
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
                        "content": (
                            "Based on the available evidence, the current channel is 11, but 24 hours ago, the channel was not "
                            "available in the mesh state. This suggests that the channel may have changed between now and 24 hours ago."
                        ),
                    }
                }
            ]
        }

    async def fake_dispatch(name: str, arguments: dict[str, object]) -> dict[str, object]:
        assert name == "get_mesh_state"
        return {
            "data": {"channel": 11, "node_count": 19, "partition_id": 1846206278},
            "meta": {"tool": name},
        }

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_dispatch_chat_tool", fake_dispatch)
    monkeypatch.setattr(direct_chat, "_MAX_TOOL_ROUNDS", 1)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="Did the channel change between now and 24h ago?",
            rendered_message="User message: Did the channel change between now and 24h ago?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == (
        "I don't have channel-specific history for the retained comparison anchors, so I can't determine whether the "
        "Thread channel changed in that window."
    )
    assert [row["name"] for row in result["tool_calls"]] == ["get_mesh_state", "get_mesh_state"]


def _obsolete_test_direct_chat_turn_clamps_forced_internal_tool_answer_that_requests_node_selection(monkeypatch) -> None:
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
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "call-counters-1",
                                    "type": "function",
                                    "function": {
                                        "name": "get_counter_series",
                                        "arguments": '{"eui64":null,"counter_names":["tx_retry","tx_err_cca"]}',
                                    },
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
                                    "id": "call-counters-2",
                                    "type": "function",
                                    "function": {
                                        "name": "get_counter_series",
                                        "arguments": '{"eui64":null,"counter_names":["tx_retry","tx_err_cca"]}',
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
                        "content": (
                            "Based on the current evidence, the selected node EUI64 is still null. This means that I do not have "
                            "enough evidence to determine whether RF caused the channel change. To proceed, please select a node "
                            "from the dashboard."
                        ),
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
    monkeypatch.setattr(direct_chat, "_MAX_TOOL_ROUNDS", 1)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="What internal MCP tool should I call to verify whether RF caused the channel change?",
            rendered_message="User message: What internal MCP tool should I call to verify whether RF caused the channel change?",
            conversation_id=None,
        )
    )

    assert result["response"]["text"] == (
        "I can't ask you to call internal MCP tools directly. I can't determine whether RF conditions caused the channel "
        "change from the available evidence because the counter query was not grounded to a real 16-hex EUI64 from the mesh "
        "inventory and the returned counter series was empty."
    )
    assert [row["name"] for row in result["tool_calls"]] == ["get_counter_series", "get_counter_series"]


def test_answer_review_policies_block_nonexistent_dashboard_actions() -> None:
    policies = direct_chat._answer_review_policies(
        internal_tool_request=False,
        counter_question=False,
        history_comparison_question=False,
        node_question=False,
    )

    assert any("UI controls" in policy for policy in policies)
    assert any("backend evidence only" in policy for policy in policies)
    assert any("interface advice" in policy for policy in policies)
    assert any("routing-table checks" in policy for policy in policies)
    assert any("self-referential meta commentary" in policy for policy in policies)
    assert any("better path to OTBR" in policy for policy in policies)
    assert any("signal quality improved" in policy for policy in policies)
    assert any("full requested history window" in policy for policy in policies)


def test_direct_chat_turn_rewrites_duplicate_device_meta_response_via_audit(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama-4-scout",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )
    calls: list[dict[str, object]] = []
    audits = [
        direct_chat.AuditVerdict(
            answered_question=False,
            grounded_in_evidence=True,
            contains_extraneous_content=True,
            rewrite_needed=True,
            repair_action="rewrite_once",
            critique="Answer the duplicate-device question directly. Do not respond with meta commentary about internal tools.",
        ),
        direct_chat.AuditVerdict(),
    ]

    async def fake_post_chat_completions(target, body):  # noqa: ANN001
        calls.append(json.loads(json.dumps(body)))
        if len(calls) == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": (
                                "I shouldn't send you to internal MCP tools or backend function names directly. I should either "
                                "use those tools myself and answer from the evidence, or describe the next diagnostic step in "
                                "plain operator terms."
                            ),
                        }
                    }
                ]
            }
        critique = next(
            msg for msg in body["messages"]
            if msg.get("role") == "system" and "Audit critique:" in str(msg.get("content") or "")
        )
        assert "duplicate-device question directly" in critique["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": (
                            "A duplicate physical device group means the backend has multiple EUI64 rows that appear to belong "
                            "to the same real device, usually because an older commissioning record or stale registration was "
                            "retained after the device rejoined with a new identity."
                        ),
                    }
                }
            ]
        }

    async def fake_audit(*args, **kwargs):  # noqa: ANN002, ANN003
        return audits.pop(0)

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_audit_answer_candidate", fake_audit)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="What does the duplicate physical device group mean?",
            rendered_message="User message: What does the duplicate physical device group mean?",
            conversation_id=None,
        )
    )

    assert "multiple EUI64 rows" in result["response"]["text"]
    assert "same real device" in result["response"]["text"]
    assert len(calls) == 2


def test_direct_chat_turn_rewrites_ungrounded_otbr_path_improvement_claim_via_audit(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama-4-scout",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )
    calls: list[dict[str, object]] = []
    audits = [
        direct_chat.AuditVerdict(
            answered_question=False,
            grounded_in_evidence=False,
            contains_extraneous_content=False,
            rewrite_needed=True,
            repair_action="rewrite_once",
            critique=(
                "Do not infer route improvement or a better OTBR path from node and link-count deltas alone. "
                "Answer with the actual evidence and state what is missing."
            ),
        ),
        direct_chat.AuditVerdict(),
    ]

    async def fake_post_chat_completions(target, body):  # noqa: ANN001
        calls.append(json.loads(json.dumps(body)))
        if len(calls) == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": (
                                "A single device was added and the three new links mean it improved the network by giving "
                                "existing devices a better path to the OTBR."
                            ),
                        }
                    }
                ]
            }
        critique = next(
            msg for msg in body["messages"]
            if msg.get("role") == "system" and "Audit critique:" in str(msg.get("content") or "")
        )
        assert "better OTBR path" in critique["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": (
                            "The retained history supports only that one node and three links appeared in that window. "
                            "I cannot tell from those counts alone whether any device found a better path to the OTBR, "
                            "because that would require explicit route, parent, or OTBR-role evidence for the affected devices."
                        ),
                    }
                }
            ]
        }

    async def fake_audit(*args, **kwargs):  # noqa: ANN002, ANN003
        return audits.pop(0)

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_audit_answer_candidate", fake_audit)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message=(
                "Looking at the past 3 days, tell me about the most recent devices added to the network and whether "
                "any device found a new and better path to the OTBR."
            ),
            rendered_message=(
                "User message: Looking at the past 3 days, tell me about the most recent devices added to the network "
                "and whether any device found a new and better path to the OTBR."
            ),
            conversation_id=None,
        )
    )

    assert "one node and three links appeared" in result["response"]["text"]
    assert "cannot tell from those counts alone" in result["response"]["text"]
    assert "better path to the OTBR" in result["response"]["text"]
    assert len(calls) == 2


def test_direct_chat_turn_rewrites_sparse_history_window_overclaim_via_audit(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama-4-scout",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )
    calls: list[dict[str, object]] = []
    audits = [
        direct_chat.AuditVerdict(
            answered_question=False,
            grounded_in_evidence=False,
            rewrite_needed=True,
            repair_action="rewrite_once",
            critique=(
                "Do not present a same-day retained snapshot span as if it covered the full requested 3-day window. "
                "State the actual retained coverage and the missing earlier history."
            ),
        ),
        direct_chat.AuditVerdict(),
    ]

    async def fake_post_chat_completions(target, body):  # noqa: ANN001
        calls.append(json.loads(json.dumps(body)))
        if len(calls) == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": (
                                "Over the past 3 days, the network grew from 16 nodes to 17 nodes and from 62 links to 65 links, "
                                "with no other topology changes."
                            ),
                        }
                    }
                ]
            }
        critique = next(
            msg for msg in body["messages"]
            if msg.get("role") == "system" and "Audit critique:" in str(msg.get("content") or "")
        )
        assert "requested 3-day window" in critique["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": (
                            "The retained topology history I can see here only spans a shorter same-day window, not the full requested 3 days. "
                            "Within that retained span, the network changed from 16 nodes and 62 links to 17 nodes and 65 links. "
                            "I cannot say from the current evidence whether that was the only change across the full 3-day window because the earlier history is missing."
                        ),
                    }
                }
            ]
        }

    async def fake_audit(*args, **kwargs):  # noqa: ANN002, ANN003
        return audits.pop(0)

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_audit_answer_candidate", fake_audit)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="How has my network changed in the past 3 days?",
            rendered_message="User message: How has my network changed in the past 3 days?",
            conversation_id=None,
        )
    )

    assert "only spans a shorter same-day window" in result["response"]["text"]
    assert "not the full requested 3 days" in result["response"]["text"]
    assert "earlier history is missing" in result["response"]["text"]
    assert len(calls) == 2


def test_direct_chat_turn_rewrites_speculative_signal_quality_improvement_claim_via_audit(monkeypatch) -> None:
    target = direct_chat.DirectChatTarget(
        provider="cerebras",
        model="llama-4-scout",
        base_url="https://api.cerebras.ai/v1",
        api_key="secret",
        temperature=0.2,
    )
    calls: list[dict[str, object]] = []
    audits = [
        direct_chat.AuditVerdict(
            answered_question=False,
            grounded_in_evidence=False,
            contains_extraneous_content=True,
            rewrite_needed=True,
            repair_action="rewrite_once",
            critique=(
                "Do not infer that signal quality improved for any device from a new REED/router and a few new links alone. "
                "Do not prescribe routing-table or node-health checks as next steps when you did not run them. "
                "State the actual retained history coverage, the observed topology change, and the missing before/after signal or route evidence."
            ),
        ),
        direct_chat.AuditVerdict(),
    ]

    async def fake_post_chat_completions(target, body):  # noqa: ANN001
        calls.append(json.loads(json.dumps(body)))
        if len(calls) == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": (
                                "Over the past 3 days the network added a new REED with three links, which likely improved signal quality for some devices. "
                                "Check the routing tables and node-health metrics next to confirm which devices benefited."
                            ),
                        }
                    }
                ]
            }
        critique = next(
            msg for msg in body["messages"]
            if msg.get("role") == "system" and "Audit critique:" in str(msg.get("content") or "")
        )
        assert "signal quality improved" in critique["content"]
        assert "routing-table" in critique["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": (
                            "The retained history visible here shows a shorter same-day span, not the full requested 3 days. "
                            "Within that retained span, one node and three links were added. I cannot tell from that topology diff alone "
                            "whether signal quality improved for any device, because I do not have explicit before/after RSSI, LQI, attachment, or route evidence for the affected devices."
                        ),
                    }
                }
            ]
        }

    async def fake_audit(*args, **kwargs):  # noqa: ANN002, ANN003
        return audits.pop(0)

    monkeypatch.setattr(direct_chat, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(direct_chat, "_audit_answer_candidate", fake_audit)

    result = asyncio.run(
        direct_chat.direct_chat_turn(
            target=target,
            message="How as has my network changed over the past 3 days? What did that change do for signal quality, did it improve for some devices?",
            rendered_message="User message: How as has my network changed over the past 3 days? What did that change do for signal quality, did it improve for some devices?",
            conversation_id=None,
        )
    )

    assert "shorter same-day span" in result["response"]["text"]
    assert "one node and three links were added" in result["response"]["text"]
    assert "cannot tell from that topology diff alone" in result["response"]["text"]
    assert "before/after RSSI, LQI, attachment, or route evidence" in result["response"]["text"]
    assert len(calls) == 2
