"""Dashboard smoke tests for the static HTML shell."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

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
    assert 'id="graph-risk-strip"' in html
    assert 'id="graph-link-density"' in html
    assert 'id="graph-area-filter"' in html
    assert 'id="graph-area-recommendations"' in html
    assert 'id="graph-group-duplicates"' in html
    assert 'id="graph-overlay-recent-changes"' in html
    assert 'id="graph-history-summary"' in html
    assert 'id="graph-overlay-weak-links"' in html
    assert 'id="graph-overlay-unstable"' in html
    assert 'id="graph-inspector"' in html
    assert 'chat-copy-btn' in html
    assert html.count('id="chat-card"') == 1
    assert html.index('id="tab-diagnostics"') < html.index('id="chat-card"')


def test_dashboard_wires_expected_dashboard_endpoints() -> None:
    client = TestClient(create_core_app())

    html = client.get("/").text

    assert "v1/chat/agents" in html
    assert "v1/chat/turn" in html
    assert "v1/assessment/state" in html
    assert "v1/assessment/findings?state=open&limit=10" in html
    assert "v1/assessment/history?limit=" in html
    assert "v1/assessment/run-now" in html


def test_dashboard_renders_assistant_markdown_safely() -> None:
    client = TestClient(create_core_app())
    html = client.get("/").text

    assert "markdown-it" in html
    assert "dompurify" in html.lower()
    assert "renderAssistantMarkdown" in html
    assert "DOMPurify.sanitize" in html


def test_dashboard_uses_home_assistant_theme_tokens() -> None:
    client = TestClient(create_core_app())
    html = client.get("/").text

    assert "--primary-background-color" in html
    assert "--ha-card-background" in html
    assert "--primary-text-color" in html
    assert "--secondary-text-color" in html
    assert "--accent-color" in html


def test_node_analysis_endpoint_exposes_peer_comparison(store) -> None:
    subject = "11" * 8
    peer = "22" * 8
    now = datetime(2026, 5, 12, 12, 0, 0, tzinfo=UTC)
    store.upsert_node_metadata(eui64=subject, friendly_name="Subject", role="router")
    store.upsert_node_metadata(eui64=peer, friendly_name="Peer", role="router")
    store.set_node_diagnostics(subject, partition_id=42, routing_role="router")
    store.set_node_diagnostics(peer, partition_id=42, routing_role="router")
    for offset_days in (1, 2):
        store.insert_event(
            eui64=subject,
            type="parent_change",
            ts=(now - timedelta(days=offset_days)).isoformat(),
        )

    client = TestClient(create_core_app())
    response = client.get(f"/v1/nodes/{subject}/analysis")

    assert response.status_code == 200
    payload = response.json()
    assert payload["node"]["eui64"] == subject
    assert payload["peer_comparison"]["partition_id"] == 42
    assert payload["peer_comparison"]["peer_count"] == 1
