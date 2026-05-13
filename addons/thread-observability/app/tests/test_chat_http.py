"""Tests for the HA conversation proxy endpoints (#10)."""

from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from thread_observability.api import supervisor_client
from thread_observability.api.http_api import create_core_app
from thread_observability.config import AIConfig, ThreadObsConfig
from thread_observability.services import direct_chat


def test_chat_agents_endpoint_returns_agent_list(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_list_agents() -> dict[str, object]:
        return {
            "count": 1,
            "source": "ws",
            "agents": [{"agent_id": "conversation.claude", "name": "Claude", "source": "ws"}],
        }

    monkeypatch.setattr(supervisor_client, "list_conversation_agents", fake_list_agents)
    client = TestClient(create_core_app())

    response = client.get("/v1/chat/agents")
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["agents"][0]["agent_id"] == "conversation.claude"


def test_chat_agents_endpoint_includes_direct_agent_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_list_agents() -> dict[str, object]:
        return {
            "count": 1,
            "source": "ws",
            "agents": [{"agent_id": "conversation.claude", "name": "Claude", "source": "ws"}],
        }

    cfg = ThreadObsConfig(
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


def test_chat_turn_success_shapes_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_process(*, text: str, conversation_id: str | None = None, agent_id: str | None = None) -> dict[str, object]:
        assert "Page context:" in text
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
    cfg = ThreadObsConfig(
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
        assert conversation_id is None
        return {
            "conversation_id": "direct-1",
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


def test_chat_turn_explicit_ha_agent_overrides_direct_default(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ThreadObsConfig(
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