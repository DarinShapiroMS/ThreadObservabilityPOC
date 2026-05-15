"""Tests for the HA conversation proxy endpoints (#10)."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from thread_observability.api import supervisor_client
from thread_observability.api.http_api import create_core_app
from thread_observability.config import AIConfig, ChatConfig, RetentionConfig, ThreadObsConfig
from thread_observability.services import chat_memory
from thread_observability.services import direct_chat
from thread_observability.storage.sqlite_store import SQLiteStore, get_store, reset_store_for_tests


@pytest.fixture(autouse=True)
def reset_chat_memory_store(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    reset_store_for_tests(store)
    chat_memory.reset()
    yield
    chat_memory.reset()
    reset_store_for_tests(None)
    store.close()


@pytest.fixture(autouse=True)
def enable_chat_http_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: ThreadObsConfig(chat=ChatConfig(enabled=True)))


def _chat_enabled_config(
    *,
    ai: AIConfig | None = None,
    chat: ChatConfig | None = None,
    retention: RetentionConfig | None = None,
) -> ThreadObsConfig:
    return ThreadObsConfig(
        ai=ai or AIConfig(),
        chat=chat or ChatConfig(enabled=True),
        retention=retention or RetentionConfig(),
    )


def test_chat_agents_endpoint_returns_agent_list(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_list_agents() -> dict[str, object]:
        return {
            "count": 1,
            "source": "ws",
            "agents": [{"agent_id": "conversation.claude", "name": "Claude", "source": "ws", "tool_names": ["get_health_snapshot"]}],
        }

    monkeypatch.setattr(supervisor_client, "list_conversation_agents", fake_list_agents)
    client = TestClient(create_core_app())

    response = client.get("/v1/chat/agents")
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["agents"][0]["agent_id"] == "conversation.claude"
    assert body["thread_tools_connected"] is True
    assert body["mcp_connect_url"].endswith("/mcp/sse")
    assert body["starter_prompts"]


def test_chat_agents_endpoint_includes_direct_agent_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_list_agents() -> dict[str, object]:
        return {
            "count": 1,
            "source": "ws",
            "agents": [{"agent_id": "conversation.claude", "name": "Claude", "source": "ws"}],
        }

    cfg = _chat_enabled_config(
        ai=AIConfig(
            enabled=True,
            provider="cerebras",
            chat_backend="auto",
            model="llama-4-scout",
            api_key="secret",
        )
    )
    monkeypatch.setattr(supervisor_client, "list_conversation_agents", fake_list_agents)
    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    client = TestClient(create_core_app())

    response = client.get("/v1/chat/agents")
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert body["agents"][0]["agent_id"] == "direct:cerebras"
    assert body["default_backend"] == "direct"
    assert body["default_label"].startswith("Auto (Direct Cerebras")
    assert body["thread_tools_connected"] is True


def test_chat_agents_endpoint_includes_direct_agent_even_if_ai_enabled_false(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_list_agents() -> dict[str, object]:
        return {"count": 0, "source": "ws", "agents": []}

    cfg = _chat_enabled_config(
        ai=AIConfig(
            enabled=False,
            provider="cerebras",
            chat_backend="direct",
            model="llama3.1-8b",
            api_key="secret",
        )
    )
    monkeypatch.setattr(supervisor_client, "list_conversation_agents", fake_list_agents)
    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    client = TestClient(create_core_app())

    response = client.get("/v1/chat/agents")
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["agents"][0]["agent_id"] == "direct:cerebras"
    assert body["default_backend"] == "direct"
    assert body["thread_tools_connected"] is True


def test_chat_turn_success_shapes_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_process(*, text: str, conversation_id: str | None = None, agent_id: str | None = None) -> dict[str, object]:
        assert "Page context:" not in text
        assert text.endswith("User message: Why are there two partitions right now?")
        assert conversation_id == "conv-1"
        assert agent_id == "conversation.claude"
        return {
            "conversation_id": "conv-1",
            "agent_id": "conversation.claude",
            "response": {
                "speech": {"plain": {"speech": "Two partitions are present."}},
                "data": {
                    "tool_calls": [{"name": "start_triage"}],
                    "model": "claude-sonnet-4.5",
                },
            },
        }

    monkeypatch.setattr(supervisor_client, "conversation_process", fake_process)
    client = TestClient(create_core_app())

    response = client.post(
        "/v1/chat/turn",
        json={
            "message": "Why are there two partitions right now?",
            "conversation_id": "conv-1",
            "agent_id": "conversation.claude",
            "page_context": {"page": "dashboard"},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["conversation_id"] == "conv-1"
    assert body["response"]["text"] == "Two partitions are present."
    assert body["tool_calls"][0]["name"] == "start_triage"
    assert body["model"] == "claude-sonnet-4.5"
    assert body["streaming"] is False


def test_chat_turn_uses_direct_model_when_auto_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _chat_enabled_config(
        ai=AIConfig(
            enabled=True,
            provider="cerebras",
            chat_backend="auto",
            model="llama-4-scout",
            api_key="secret",
        )
    )

    async def fake_direct_turn(*, target, message: str, rendered_message: str, conversation_id: str | None):  # noqa: ANN001
        assert target.provider == "cerebras"
        assert message == "hello"
        assert "User message: hello" in rendered_message
        assert conversation_id is not None
        return {
            "conversation_id": str(conversation_id),
            "agent_id": target.agent_id,
            "response": {"text": "direct reply", "card": None},
            "tool_calls": [],
            "duration_ms": 9,
            "model": target.model,
            "streaming": False,
        }

    async def fail_ha_process(**kwargs):  # noqa: ARG001
        raise AssertionError("HA path should not be called")

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(direct_chat, "direct_chat_turn", fake_direct_turn)
    monkeypatch.setattr(supervisor_client, "conversation_process", fail_ha_process)
    client = TestClient(create_core_app())

    response = client.post("/v1/chat/turn", json={"message": "hello", "page_context": {"page": "dashboard"}})
    assert response.status_code == 200
    body = response.json()
    assert body["agent_id"] == "direct:cerebras"
    assert body["response"]["text"] == "direct reply"
    assert body["model"] == "llama-4-scout"
    assert str(body["conversation_id"]).startswith("direct:") is False


def test_chat_turn_uses_direct_model_even_if_ai_enabled_false(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _chat_enabled_config(
        ai=AIConfig(
            enabled=False,
            provider="cerebras",
            chat_backend="direct",
            model="llama3.1-8b",
            api_key="secret",
        )
    )

    async def fake_direct_turn(*, target, message: str, rendered_message: str, conversation_id: str | None):  # noqa: ANN001
        assert target.provider == "cerebras"
        return {
            "conversation_id": "direct-1",
            "agent_id": target.agent_id,
            "response": {"text": "direct reply", "card": None},
            "tool_calls": [],
            "duration_ms": 7,
            "model": target.model,
            "streaming": False,
        }

    async def fail_ha_process(**kwargs):  # noqa: ARG001
        raise AssertionError("HA path should not be called")

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(direct_chat, "direct_chat_turn", fake_direct_turn)
    monkeypatch.setattr(supervisor_client, "conversation_process", fail_ha_process)
    client = TestClient(create_core_app())

    response = client.post("/v1/chat/turn", json={"message": "hello", "page_context": {"page": "dashboard"}})
    assert response.status_code == 200
    body = response.json()
    assert body["agent_id"] == "direct:cerebras"
    assert body["response"]["text"] == "direct reply"


def test_chat_turn_injects_session_memory_on_direct_followup(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _chat_enabled_config(
        ai=AIConfig(
            enabled=True,
            provider="cerebras",
            chat_backend="direct",
            model="llama-4-scout",
            api_key="secret",
        ),
        chat=ChatConfig(enabled=True, persist_transcripts=True),
    )
    rendered_messages: list[str] = []

    async def fake_direct_turn(*, target, message: str, rendered_message: str, conversation_id: str | None):  # noqa: ANN001
        rendered_messages.append(rendered_message)
        assert conversation_id is not None
        if len(rendered_messages) == 1:
            assert "Session memory:" not in rendered_message
            return {
                "conversation_id": str(conversation_id),
                "agent_id": target.agent_id,
                "response": {"text": "First reply", "card": None},
                "tool_calls": [
                    {
                        "name": "analyze_node",
                        "result": {
                            "eui64": "e6684b9903e8970f",
                            "node": {
                                "eui64": "e6684b9903e8970f",
                                "friendly_name": "Family Room Track Lights",
                                "status": "online",
                                "partition_id": 1846206278,
                            },
                            "timeline": [{"kind": "re_attached_node"}],
                        },
                    },
                    {
                        "name": "get_mesh_state",
                        "result": {"all_partitions": [1846206278, 2107240925]},
                    },
                ],
                "duration_ms": 7,
                "model": target.model,
                "streaming": False,
            }
        assert "Session memory:" in rendered_message
        assert "Family Room Track Lights" in rendered_message
        assert "Current mesh state shows 2 active partitions." in rendered_message
        assert "Recent node timeline includes: re_attached_node." in rendered_message
        assert "hypotheses" in rendered_message
        assert "Partition split or stale Thread dataset may explain the observed behavior." in rendered_message
        assert "pending_questions" not in rendered_message
        return {
            "conversation_id": str(conversation_id),
            "agent_id": target.agent_id,
            "response": {"text": "Second reply", "card": None},
            "tool_calls": [],
            "duration_ms": 8,
            "model": target.model,
            "streaming": False,
        }

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(direct_chat, "direct_chat_turn", fake_direct_turn)
    client = TestClient(create_core_app())

    first = client.post(
        "/v1/chat/turn",
        json={
            "message": "What is going on with node e6684b9903e8970f?",
            "page_context": {
                "page": "dashboard",
                "selected_node_eui64": "e6684b9903e8970f",
                "snapshot_summary": {"partition_count": 2, "distinct_thread_networks": 2},
            },
        },
    )
    assert first.status_code == 200
    conversation_id = first.json()["conversation_id"]
    chat_memory.reset()

    second = client.post(
        "/v1/chat/turn",
        json={
            "message": "What changed recently?",
            "conversation_id": conversation_id,
            "page_context": {"page": "dashboard", "selected_node_eui64": "e6684b9903e8970f"},
        },
    )
    assert second.status_code == 200
    assert second.json()["response"]["text"] == "Second reply"


def test_chat_turn_does_not_inject_graph_diagnostics_into_rendered_message(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _chat_enabled_config(
        ai=AIConfig(
            enabled=True,
            provider="cerebras",
            chat_backend="direct",
            model="llama-4-scout",
            api_key="secret",
        )
    )
    store = get_store()
    a = "aa" * 8
    b = "bb" * 8
    c = "cc" * 8
    d = "dd" * 8
    e = "ee" * 8
    store.upsert_node_metadata(eui64=a, friendly_name="Leader-A")
    store.upsert_node_metadata(eui64=b, friendly_name="Router-B")
    store.upsert_node_metadata(eui64=c, friendly_name="Leader-C")
    store.upsert_node_metadata(eui64=d, friendly_name="Child-D")
    store.upsert_node_metadata(eui64=e, friendly_name="Child-E")
    store.set_node_diagnostics(a, partition_id=1111, routing_role="leader")
    store.set_node_diagnostics(b, partition_id=1111, routing_role="router")
    store.set_node_diagnostics(c, partition_id=2222, routing_role="leader")
    store.replace_links_for_reporter(a, "neighbor_table", [
        {"neighbor_eui64": b, "rssi_avg": -90, "rssi_last": -92,
         "lqi_in": 40, "lqi_out": None, "is_child": 0,
         "age_seconds": 5, "frame_error_rate": 12, "message_error_rate": 0,
         "path_cost": None},
        {"neighbor_eui64": d, "rssi_avg": -65, "rssi_last": -66,
         "lqi_in": 180, "lqi_out": None, "is_child": 1,
         "age_seconds": 5, "frame_error_rate": 0, "message_error_rate": 0,
         "path_cost": None},
        {"neighbor_eui64": e, "rssi_avg": -68, "rssi_last": -68,
         "lqi_in": 170, "lqi_out": None, "is_child": 1,
         "age_seconds": 5, "frame_error_rate": 0, "message_error_rate": 0,
         "path_cost": None},
    ])
    store.replace_links_for_reporter(b, "neighbor_table", [
        {"neighbor_eui64": a, "rssi_avg": -50, "rssi_last": -50,
         "lqi_in": 240, "lqi_out": None, "is_child": 0,
         "age_seconds": 5, "frame_error_rate": 0, "message_error_rate": 0,
         "path_cost": None},
    ])

    async def fake_direct_turn(*, target, message: str, rendered_message: str, conversation_id: str | None):  # noqa: ANN001
        assert "graph_diagnostics" not in rendered_message
        assert "split_mesh" not in rendered_message
        assert "weak_links" not in rendered_message
        assert "subtree_dependency" not in rendered_message
        assert rendered_message.endswith("User message: What is the likely choke point in this mesh, and would placement changes help?")
        return {
            "conversation_id": str(conversation_id),
            "agent_id": target.agent_id,
            "response": {"text": "direct reply", "card": None},
            "tool_calls": [],
            "duration_ms": 1,
            "model": target.model,
            "streaming": False,
        }

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(direct_chat, "direct_chat_turn", fake_direct_turn)
    client = TestClient(create_core_app())

    response = client.post(
        "/v1/chat/turn",
        json={
            "message": "What is the likely choke point in this mesh, and would placement changes help?",
            "page_context": {"page": "dashboard", "active_tab": "graph"},
        },
    )

    assert response.status_code == 200
    assert response.json()["response"]["text"] == "direct reply"


def test_chat_turn_keeps_pending_question_when_reply_is_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _chat_enabled_config(
        ai=AIConfig(
            enabled=True,
            provider="cerebras",
            chat_backend="direct",
            model="llama-4-scout",
            api_key="secret",
        ),
        chat=ChatConfig(enabled=True, persist_transcripts=True),
    )
    rendered_messages: list[str] = []

    async def fake_direct_turn(*, target, message: str, rendered_message: str, conversation_id: str | None):  # noqa: ANN001
        rendered_messages.append(rendered_message)
        if len(rendered_messages) == 1:
            return {
                "conversation_id": str(conversation_id),
                "agent_id": target.agent_id,
                "response": {"text": "I couldn't complete the tool-assisted reasoning loop. Please retry with a narrower request.", "card": None},
                "tool_calls": [],
                "duration_ms": 6,
                "model": target.model,
                "streaming": False,
            }
        assert "pending_questions" in rendered_message
        assert "Which offline nodes look most suspicious right now?" in rendered_message
        return {
            "conversation_id": str(conversation_id),
            "agent_id": target.agent_id,
            "response": {"text": "Follow-up reply", "card": None},
            "tool_calls": [],
            "duration_ms": 6,
            "model": target.model,
            "streaming": False,
        }

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(direct_chat, "direct_chat_turn", fake_direct_turn)
    client = TestClient(create_core_app())

    first = client.post(
        "/v1/chat/turn",
        json={"message": "Which offline nodes look most suspicious right now?"},
    )
    assert first.status_code == 200
    conversation_id = first.json()["conversation_id"]
    chat_memory.reset()

    second = client.post(
        "/v1/chat/turn",
        json={"message": "Try again.", "conversation_id": conversation_id},
    )
    assert second.status_code == 200


def test_chat_turn_promotes_curated_hypothesis_from_reply_text(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _chat_enabled_config(
        ai=AIConfig(
            enabled=True,
            provider="cerebras",
            chat_backend="direct",
            model="llama-4-scout",
            api_key="secret",
        ),
        chat=ChatConfig(enabled=True, persist_transcripts=True),
    )
    rendered_messages: list[str] = []

    async def fake_direct_turn(*, target, message: str, rendered_message: str, conversation_id: str | None):  # noqa: ANN001
        rendered_messages.append(rendered_message)
        if len(rendered_messages) == 1:
            return {
                "conversation_id": str(conversation_id),
                "agent_id": target.agent_id,
                "response": {"text": "A stale Thread dataset is still plausible here.", "card": None},
                "tool_calls": [],
                "duration_ms": 5,
                "model": target.model,
                "streaming": False,
            }
        assert "hypotheses" in rendered_message
        assert "Stale Thread dataset or credentials mismatch may explain the observed behavior." in rendered_message
        return {
            "conversation_id": str(conversation_id),
            "agent_id": target.agent_id,
            "response": {"text": "Follow-up reply", "card": None},
            "tool_calls": [],
            "duration_ms": 5,
            "model": target.model,
            "streaming": False,
        }

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(direct_chat, "direct_chat_turn", fake_direct_turn)
    client = TestClient(create_core_app())

    first = client.post("/v1/chat/turn", json={"message": "What could cause this partition split?"})
    assert first.status_code == 200
    conversation_id = first.json()["conversation_id"]
    chat_memory.reset()

    second = client.post(
        "/v1/chat/turn",
        json={"message": "What should I verify next?", "conversation_id": conversation_id},
    )
    assert second.status_code == 200


def test_chat_turn_explicit_ha_agent_overrides_direct_default(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _chat_enabled_config(
        ai=AIConfig(
            enabled=True,
            provider="cerebras",
            chat_backend="auto",
            model="llama-4-scout",
            api_key="secret",
        )
    )

    async def fake_process(*, text: str, conversation_id: str | None = None, agent_id: str | None = None) -> dict[str, object]:
        assert agent_id == "conversation.claude"
        return {
            "conversation_id": "conv-1",
            "agent_id": agent_id,
            "response": {
                "speech": {"plain": {"speech": "routed through HA"}},
                "data": {"tool_calls": []},
            },
        }

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(supervisor_client, "conversation_process", fake_process)
    client = TestClient(create_core_app())

    response = client.post("/v1/chat/turn", json={"message": "hello", "agent_id": "conversation.claude"})
    assert response.status_code == 200
    assert response.json()["response"]["text"] == "routed through HA"


def test_chat_turn_returns_412_when_no_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_process(*, text: str, conversation_id: str | None = None, agent_id: str | None = None) -> dict[str, object]:  # noqa: ARG001
        raise supervisor_client.NoConversationAgentConfigured("No default agent configured")

    monkeypatch.setattr(supervisor_client, "conversation_process", fake_process)
    client = TestClient(create_core_app())

    response = client.post("/v1/chat/turn", json={"message": "hello"})
    assert response.status_code == 412


def test_chat_turn_returns_502_for_upstream_http_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    request = httpx.Request("POST", "http://supervisor/core/api/conversation/process")
    upstream = httpx.Response(500, request=request, text="agent crashed")

    async def fake_process(*, text: str, conversation_id: str | None = None, agent_id: str | None = None) -> dict[str, object]:  # noqa: ARG001
        raise httpx.HTTPStatusError("boom", request=request, response=upstream)

    monkeypatch.setattr(supervisor_client, "conversation_process", fake_process)
    client = TestClient(create_core_app())

    response = client.post("/v1/chat/turn", json={"message": "hello"})
    assert response.status_code == 502


def test_chat_turn_rejects_streaming_for_now() -> None:
    client = TestClient(create_core_app())
    response = client.post(
        "/v1/chat/turn",
        json={"message": "hello", "streaming": True},
    )
    assert response.status_code == 501


def test_chat_turn_rewrites_builtin_fallback_without_model(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_process(*, text: str, conversation_id: str | None = None, agent_id: str | None = None) -> dict[str, object]:  # noqa: ARG001
        return {
            "conversation_id": "conv-1",
            "agent_id": "conversation.home_assistant",
            "response": {
                "speech": {"plain": {"speech": "Sorry, I couldn't understand that"}},
                "data": {},
            },
        }

    monkeypatch.setattr(supervisor_client, "conversation_process", fake_process)
    client = TestClient(create_core_app())

    response = client.post("/v1/chat/turn", json={"message": "hello"})
    assert response.status_code == 200
    body = response.json()
    assert "not an LLM-backed Assist agent" in body["response"]["text"]
    assert "conversation.home_assistant" in body["response"]["text"]


def test_chat_turn_keeps_builtin_text_when_model_is_present(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_process(*, text: str, conversation_id: str | None = None, agent_id: str | None = None) -> dict[str, object]:  # noqa: ARG001
        return {
            "conversation_id": "conv-1",
            "agent_id": "conversation.claude",
            "response": {
                "speech": {"plain": {"speech": "Sorry, I couldn't understand that"}},
                "data": {"model": "claude-sonnet-4.5"},
            },
        }

    monkeypatch.setattr(supervisor_client, "conversation_process", fake_process)
    client = TestClient(create_core_app())

    response = client.post("/v1/chat/turn", json={"message": "hello"})
    assert response.status_code == 200
    body = response.json()
    assert body["response"]["text"] == "Sorry, I couldn't understand that"


def test_chat_option_defaults_match_issue_14() -> None:
    cfg = ThreadObsConfig()

    assert cfg.chat.enabled is False
    assert cfg.chat.default_agent_id == ""
    assert cfg.chat.send_page_context is True
    assert cfg.chat.persist_transcripts is False
    assert cfg.retention.chat_days == 14


def test_chat_agents_endpoint_reports_disabled_state(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ThreadObsConfig(chat=ChatConfig(enabled=False, default_agent_id="conversation.claude"))
    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    client = TestClient(create_core_app())

    response = client.get("/v1/chat/agents")

    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is False
    assert body["count"] == 0
    assert body["agents"] == []
    assert body["default_agent_id"] == "conversation.claude"
    assert body["send_page_context"] is True


def test_chat_turn_rejects_when_chat_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ThreadObsConfig(chat=ChatConfig(enabled=False))
    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    client = TestClient(create_core_app())

    response = client.post("/v1/chat/turn", json={"message": "hello"})

    assert response.status_code == 403


def test_chat_turn_uses_configured_default_agent_when_request_omits_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _chat_enabled_config(chat=ChatConfig(enabled=True, default_agent_id="conversation.claude"))

    async def fake_process(*, text: str, conversation_id: str | None = None, agent_id: str | None = None) -> dict[str, object]:  # noqa: ARG001
        assert agent_id == "conversation.claude"
        return {
            "conversation_id": "conv-1",
            "agent_id": agent_id,
            "response": {
                "speech": {"plain": {"speech": "default agent used"}},
                "data": {},
            },
        }

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(supervisor_client, "conversation_process", fake_process)
    client = TestClient(create_core_app())

    response = client.post("/v1/chat/turn", json={"message": "hello"})

    assert response.status_code == 200
    assert response.json()["agent_id"] == "conversation.claude"


def test_chat_turn_omits_page_context_when_disabled_in_options(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _chat_enabled_config(chat=ChatConfig(enabled=True, send_page_context=False))

    async def fake_process(*, text: str, conversation_id: str | None = None, agent_id: str | None = None) -> dict[str, object]:  # noqa: ARG001
        assert "Page context:" not in text
        return {
            "conversation_id": "conv-1",
            "agent_id": "conversation.claude",
            "response": {
                "speech": {"plain": {"speech": "context suppressed"}},
                "data": {},
            },
        }

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(supervisor_client, "conversation_process", fake_process)
    client = TestClient(create_core_app())

    response = client.post(
        "/v1/chat/turn",
        json={"message": "hello", "page_context": {"page": "dashboard", "active_tab": "graph"}},
    )

    assert response.status_code == 200
    assert response.json()["response"]["text"] == "context suppressed"


def test_chat_turn_skips_persisted_transcripts_when_disabled(monkeypatch: pytest.MonkeyPatch, store: SQLiteStore) -> None:
    cfg = _chat_enabled_config(
        chat=ChatConfig(enabled=True, persist_transcripts=False),
        retention=RetentionConfig(chat_days=30),
    )

    async def fake_process(*, text: str, conversation_id: str | None = None, agent_id: str | None = None) -> dict[str, object]:  # noqa: ARG001
        return {
            "conversation_id": "conv-1",
            "agent_id": "conversation.claude",
            "response": {
                "speech": {"plain": {"speech": "hello from HA"}},
                "data": {},
            },
        }

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(supervisor_client, "conversation_process", fake_process)
    client = TestClient(create_core_app())

    response = client.post("/v1/chat/turn", json={"message": "hello"})

    assert response.status_code == 200
    assert store.get_chat_session_memory("conv-1") is None


def test_chat_turn_persists_and_returns_direct_transcript(monkeypatch: pytest.MonkeyPatch, store: SQLiteStore) -> None:
    cfg = _chat_enabled_config(
        ai=AIConfig(
            enabled=True,
            provider="cerebras",
            chat_backend="direct",
            model="llama-4-scout",
            api_key="secret",
        ),
        chat=ChatConfig(enabled=True, persist_transcripts=True),
    )

    async def fake_direct_turn(*, target, message: str, rendered_message: str, conversation_id: str | None):  # noqa: ANN001
        return {
            "conversation_id": str(conversation_id or "conv-direct"),
            "agent_id": target.agent_id,
            "response": {"text": "direct reply", "card": None},
            "tool_calls": [{"name": "get_health_snapshot", "arguments": {}, "result": {"ok": True}}],
            "transcript": {
                "kind": "direct_chat",
                "rendered_message": rendered_message,
                "events": [
                    {
                        "kind": "assistant_completion",
                        "request": {"messages": [{"role": "user", "content": rendered_message}]},
                        "response": {"choices": [{"message": {"content": "direct reply"}}]},
                    },
                    {"kind": "audit_review", "request": {"messages": []}, "response": {"choices": []}},
                ],
                "final_text": "direct reply",
            },
            "duration_ms": 4,
            "model": target.model,
            "streaming": False,
        }

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(direct_chat, "direct_chat_turn", fake_direct_turn)
    client = TestClient(create_core_app())

    turn = client.post("/v1/chat/turn", json={"message": "hello", "conversation_id": "conv-direct"})

    assert turn.status_code == 200
    assert "transcript" not in turn.json()
    persisted = store.get_chat_session_memory("conv-direct")
    assert persisted is not None
    turns = persisted["payload"]["transcript_turns"]
    assert turns[0]["backend"] == "direct"
    assert turns[0]["transcript"]["events"][0]["kind"] == "assistant_completion"

    transcript = client.get("/v1/chat/transcript/conv-direct")

    assert transcript.status_code == 200
    body = transcript.json()
    assert body["conversation_id"] == "conv-direct"
    assert body["turn_count"] == 1
    assert body["transcript_turns"][0]["response_text"] == "direct reply"
    assert body["transcript_turns"][0]["transcript"]["events"][1]["kind"] == "audit_review"


def test_chat_turn_persists_ha_proxy_exchange_and_exposes_transcript(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _chat_enabled_config(chat=ChatConfig(enabled=True, persist_transcripts=True))

    async def fake_process(*, text: str, conversation_id: str | None = None, agent_id: str | None = None) -> dict[str, object]:
        return {
            "conversation_id": "conv-ha",
            "agent_id": agent_id or "conversation.claude",
            "response": {
                "speech": {"plain": {"speech": "hello from HA"}},
                "data": {"tool_calls": [{"name": "start_triage"}]},
            },
        }

    import thread_observability.api.http_api as http_api

    monkeypatch.setattr(http_api, "get_config", lambda: cfg)
    monkeypatch.setattr(supervisor_client, "conversation_process", fake_process)
    client = TestClient(create_core_app())

    response = client.post("/v1/chat/turn", json={"message": "hello", "conversation_id": "conv-ha"})
    assert response.status_code == 200

    transcript = client.get("/v1/chat/transcript/conv-ha")
    assert transcript.status_code == 200
    body = transcript.json()
    assert body["transcript_turns"][0]["backend"] == "ha"
    assert body["transcript_turns"][0]["transcript"]["kind"] == "ha_conversation_proxy"
    assert body["transcript_turns"][0]["transcript"]["rendered_message"].endswith("User message: hello")
