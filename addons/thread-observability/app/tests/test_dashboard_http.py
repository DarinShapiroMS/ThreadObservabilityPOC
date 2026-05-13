"""Dashboard smoke tests for the static HTML shell."""

from __future__ import annotations

from fastapi.testclient import TestClient

from thread_observability.api.http_api import create_core_app


def test_dashboard_serves_assessment_and_chat_shell() -> None:
    client = TestClient(create_core_app())

    response = client.get("/")

    assert response.status_code == 200
    html = response.text
    assert 'id="assessment-chip"' in html
    assert 'id="assessment-banner"' in html
    assert 'id="assessment-run-now-btn"' in html
    assert 'id="chat-card"' in html
    assert 'id="chat-agent-select"' in html
    assert 'id="chat-send-btn"' in html
    assert 'chat-copy-btn' in html


def test_dashboard_wires_expected_dashboard_endpoints() -> None:
    client = TestClient(create_core_app())

    html = client.get("/").text

    assert "v1/chat/agents" in html
    assert "v1/chat/turn" in html
    assert "v1/assessment/state" in html
    assert "v1/assessment/findings?state=open&limit=10" in html
    assert "v1/assessment/history?limit=" in html
    assert "v1/assessment/run-now" in html
