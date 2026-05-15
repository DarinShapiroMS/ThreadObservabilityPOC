from __future__ import annotations

import argparse
import json
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

from fastapi.testclient import TestClient

from thread_observability.api import supervisor_client
from thread_observability.api.http_api import create_core_app
from thread_observability.config import AssessmentConfig
from thread_observability.config import AIConfig, ChatConfig, RetentionConfig, ThreadObsConfig
from thread_observability.pipeline import otbr_adapter
from thread_observability.pipeline import runner as pipeline_runner
from thread_observability.pipeline import topology_snapshot as topology_snapshot_mod
from thread_observability.services import chat_memory
from thread_observability.services import direct_chat
from thread_observability.storage import influx_store as ts_store
from thread_observability.storage.sqlite_store import SQLiteStore, reset_store_for_tests


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROMPTS = REPO_ROOT / "addons" / "thread-observability" / "app" / "tests" / "fixtures" / "chat_prompt_regression.json"


def _build_config() -> ThreadObsConfig:
    return ThreadObsConfig(
        ai=AIConfig(
            enabled=True,
            provider="cerebras",
            chat_backend="direct",
            model="llama-4-scout",
            api_key="local-smoke",
        ),
        chat=ChatConfig(enabled=True),
        retention=RetentionConfig(),
        assessment=AssessmentConfig(enabled=True),
    )


def _load_prompts(path: Path) -> list[dict[str, str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    prompts = payload.get("prompts")
    if not isinstance(prompts, list):
        raise ValueError(f"prompt fixture at {path} is missing a prompts list")
    return [row for row in prompts if isinstance(row, dict)]


def _seed_store(store: SQLiteStore) -> None:
    now_dt = datetime.now(tz=UTC)
    now = now_dt.isoformat()
    router = "11" * 8
    sleepy = "22" * 8
    otbr = "33" * 8
    stale_neighbor = "44" * 8

    store.upsert_node_metadata(eui64=router, friendly_name="Office Router", device_id="router-1")
    store.upsert_node_metadata(eui64=sleepy, friendly_name="Bedroom Shade", device_id="shade-1")
    store.upsert_node_metadata(eui64=otbr, friendly_name="Main Border Router", device_id="otbr-1", role="border_router")
    store.set_node_diagnostics(router, routing_role="router", partition_id=1234, leader_router_id=2, channel=15)
    store.set_node_diagnostics(sleepy, routing_role="sleepy_end_device", partition_id=1234)
    store.set_node_diagnostics(otbr, routing_role="leader", partition_id=1234, leader_router_id=2, channel=15)
    store.set_node_router_id(router, 1)
    store.set_node_router_id(otbr, 2)
    store.insert_event(eui64=router, type="attach", ts=now)
    store.insert_event(eui64=sleepy, type="attach", ts=now)
    store.insert_event(eui64=otbr, type="attach", ts=now)
    store.apply_availability([
        (router, True, "ha_entity"),
        (sleepy, False, "ha_entity"),
        (otbr, True, "ha_entity"),
    ])
    store.replace_links_for_reporter(
        router,
        "neighbor_table",
        [
            {
                "neighbor_eui64": sleepy,
                "rssi_avg": -66,
                "lqi_in": 3,
                "is_child": True,
            },
            {
                "neighbor_eui64": otbr,
                "rssi_avg": -52,
                "lqi_in": 3,
            },
            {
                "neighbor_eui64": stale_neighbor,
                "rssi_avg": -81,
                "lqi_in": 1,
            },
        ],
        partition_id=1234,
    )
    store.replace_links_for_reporter(
        router,
        "route_table",
        [
            {
                "neighbor_eui64": otbr,
                "path_cost": 1,
                "next_hop_router_id": 2,
                "lqi_in": 3,
                "lqi_out": 3,
                "link_established": True,
            },
        ],
        partition_id=1234,
    )
    store.replace_links_for_reporter(
        otbr,
        "neighbor_table",
        [
            {
                "neighbor_eui64": router,
                "rssi_avg": -52,
                "lqi_in": 3,
            },
        ],
        partition_id=1234,
    )
    store.upsert_network_data(
        partition_id=1234,
        otbr_eui64=otbr,
        pan_id="0x1234",
        extended_pan_id="fedcba9876543210",
        network_name="TestMesh",
        channel=15,
        mesh_local_prefix="fd11:2233:4455::/64",
        on_mesh_prefixes=[{"prefix": "fd11:2233:4455::/64", "preferred": True}],
        br_servers=[{"server16": "0x2000"}],
        active_timestamp="1",
    )
    store.insert_topology_snapshot(
        snapshot={
            "computed_at": (now_dt - timedelta(hours=1)).isoformat(),
            "node_count": 2,
            "link_count": 1,
            "nodes": [
                {"eui64": router, "role": "router", "routing_role": "router", "partition_id": 1234, "parent_eui64": otbr},
                {"eui64": otbr, "role": "border_router", "routing_role": "leader", "partition_id": 1234, "parent_eui64": None},
            ],
            "links": [
                {"from": router, "to": otbr, "source": "route_table", "is_child": False},
            ],
            "partitions": [{"partition_id": 1234, "leader_eui64": otbr, "member_count": 2}],
        },
        snapshot_hash="snapshot-a",
        captured_at=(now_dt - timedelta(hours=1)).isoformat(),
    )
    current_snapshot = {
        "computed_at": now,
        "nodes": [
            {"eui64": sleepy, "role": None, "routing_role": "sleepy_end_device", "partition_id": 1234, "parent_eui64": router},
            {"eui64": router, "role": "router", "routing_role": "router", "partition_id": 1234, "parent_eui64": otbr},
            {"eui64": otbr, "role": "border_router", "routing_role": "leader", "partition_id": 1234, "parent_eui64": None},
        ],
        "links": [
            {"from": router, "to": sleepy, "source": "neighbor_table", "is_child": True},
            {"from": router, "to": otbr, "source": "route_table", "is_child": False},
        ],
        "partitions": [{"partition_id": 1234, "leader_eui64": otbr, "member_count": 3}],
    }
    store.insert_topology_snapshot(
        snapshot=current_snapshot,
        snapshot_hash=topology_snapshot_mod._canonicalize_snapshot_for_hash(current_snapshot),
        captured_at=now,
    )
    store.record_chat_turn_stat(
        conversation_id="seeded-1",
        recorded_at=now,
        backend="direct",
        agent_id="direct:cerebras",
        model_name="llama-4-scout",
        status="ok",
        error_kind=None,
        duration_ms=42,
        tool_call_count=1,
        had_page_context=False,
        selected_node_eui64=router,
        active_tab="network",
    )
    store.upsert_assessment_finding(
        finding_id="finding-1",
        finding_key="weak-link:router",
        verdict="warn",
        severity="medium",
        confidence=0.8,
        headline="Weak router uplink",
        evidence=[{"kind": "link", "eui64": router}],
        suggested_starter_prompt="Which links look weak or error-prone right now?",
        node_eui64=router,
        finding_type="weak_link",
    )
    store.record_assessment_run(
        verdict="warn",
        severity="medium",
        confidence=0.8,
        headline="Weak router uplink",
        finding_key="weak-link:router",
        finding_id="finding-1",
        finding_type="weak_link",
        node_eui64=router,
        model_name="local-smoke",
    )
    store.record_assessment_run(
        verdict="ok",
        severity="watch",
        confidence=0.4,
        headline="Mesh stable",
        model_name="local-smoke",
    )
    store.upsert_assessment_schedule(
        {
            "state": "engaged",
            "reason": "local smoke seeded schedule",
            "current_interval_seconds": 900,
            "next_assessment_at": (now_dt + timedelta(minutes=15)).isoformat(),
            "last_assessment_at": now,
            "consecutive_ok": 1,
            "budget_calls_used": 1,
        }
    )
    store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)


@contextmanager
def _patched_runtime(store: SQLiteStore, cfg: ThreadObsConfig) -> Iterator[dict[str, Any]]:
    import thread_observability.api.http_api as http_api

    originals = {
        "http_api_get_config": http_api.get_config,
        "http_api_get_store": http_api.get_store,
        "direct_chat_turn": direct_chat.direct_chat_turn,
        "supervisor_list_agents": supervisor_client.list_conversation_agents,
        "supervisor_get_addon_info": supervisor_client.get_addon_info,
        "timeseries_health": ts_store.timeseries_health,
        "otbr_get_state": otbr_adapter.get_state,
        "pipeline_get_runner_state": pipeline_runner.get_runner_state,
    }
    seen_messages: list[tuple[str, str]] = []

    async def fake_direct_turn(*, target, message: str, rendered_message: str, conversation_id: str | None):  # noqa: ANN001
        seen_messages.append((message, rendered_message))
        return {
            "conversation_id": conversation_id or "direct-local",
            "agent_id": target.agent_id,
            "response": {"text": f"local-smoke::{message}", "card": None},
            "tool_calls": [],
            "duration_ms": 1,
            "model": target.model,
            "streaming": False,
        }

    async def fake_list_agents() -> dict[str, object]:
        return {"count": 0, "source": "local", "agents": []}

    async def fake_addon_info() -> dict[str, object]:
        return {"slug": "thread-observability", "version": "local", "state": "started"}

    async def fake_timeseries_health() -> dict[str, object]:
        return {"ok": True, "backend": "sqlite"}

    http_api.get_config = lambda: cfg
    http_api.get_store = lambda: store
    direct_chat.direct_chat_turn = fake_direct_turn
    supervisor_client.list_conversation_agents = fake_list_agents
    supervisor_client.get_addon_info = fake_addon_info
    ts_store.timeseries_health = fake_timeseries_health
    otbr_adapter.get_state = lambda: {"slug": "local-otbr", "last_run_at": None}
    pipeline_runner.get_runner_state = lambda: {"running": False, "tick_count": 0, "stages": {}}
    try:
        yield {"seen_messages": seen_messages}
    finally:
        http_api.get_config = originals["http_api_get_config"]
        http_api.get_store = originals["http_api_get_store"]
        direct_chat.direct_chat_turn = originals["direct_chat_turn"]
        supervisor_client.list_conversation_agents = originals["supervisor_list_agents"]
        supervisor_client.get_addon_info = originals["supervisor_get_addon_info"]
        ts_store.timeseries_health = originals["timeseries_health"]
        otbr_adapter.get_state = originals["otbr_get_state"]
        pipeline_runner.get_runner_state = originals["pipeline_get_runner_state"]


def _assert_status(label: str, response, expected_status: int = 200) -> dict[str, Any]:  # noqa: ANN001
    if response.status_code != expected_status:
        raise AssertionError(f"{label} returned {response.status_code}: {response.text}")
    return response.json()


def run_smoke(prompts_path: Path) -> None:
    prompts = _load_prompts(prompts_path)
    cfg = _build_config()

    with tempfile.TemporaryDirectory() as tmp_dir:
        store = SQLiteStore(Path(tmp_dir) / "state.db")
        reset_store_for_tests(store)
        chat_memory.reset()
        _seed_store(store)

        try:
            with _patched_runtime(store, cfg) as runtime:
                client = TestClient(create_core_app())

                health = _assert_status("/health", client.get("/health"))
                snapshot = _assert_status("/v1/health/snapshot", client.get("/v1/health/snapshot"))
                status = _assert_status("/v1/dev/status", client.get("/v1/dev/status"))
                agents = _assert_status("/v1/chat/agents", client.get("/v1/chat/agents"))
                topology = _assert_status("/v1/topology", client.get("/v1/topology"))
                history = _assert_status("/v1/topology/history", client.get("/v1/topology/history?limit=5"))
                diff = _assert_status("/v1/topology/history/diff", client.get("/v1/topology/history/diff?snapshot_id_a=1&snapshot_id_b=2"))
                partitions = _assert_status("/v1/partitions", client.get("/v1/partitions"))
                neighbors = _assert_status("/v1/neighbors/1111111111111111", client.get("/v1/neighbors/1111111111111111"))
                children = _assert_status("/v1/children/1111111111111111", client.get("/v1/children/1111111111111111"))
                route = _assert_status("/v1/routes/1111111111111111", client.get("/v1/routes/1111111111111111"))
                stale = _assert_status("/v1/links/stale", client.get("/v1/links/stale"))
                network_data = _assert_status("/v1/network-data", client.get("/v1/network-data"))
                network_row = _assert_status("/v1/network-data/1234", client.get("/v1/network-data/1234"))
                node_analysis = _assert_status("/v1/nodes/1111111111111111/analysis", client.get("/v1/nodes/1111111111111111/analysis"))
                chat_stats = _assert_status("/v1/chat/stats", client.get("/v1/chat/stats"))
                assessment_state = _assert_status("/v1/assessment/state", client.get("/v1/assessment/state"))
                assessment_findings = _assert_status("/v1/assessment/findings", client.get("/v1/assessment/findings?state=open&limit=10"))
                assessment_history = _assert_status("/v1/assessment/history", client.get("/v1/assessment/history?limit=10&offset=0"))

                assert health["status"] == "ok"
                summary = snapshot.get("summary") or {}
                assert summary.get("online_nodes") == 2
                assert summary.get("sleeping_nodes") == 1
                assert summary.get("total_nodes") == 3

                node_counts = status.get("node_counts") or {}
                assert node_counts.get("online") == 2
                assert node_counts.get("sleeping") == 1
                assert node_counts.get("total") == 3
                assert agents.get("default_backend") == "direct"
                assert len(topology.get("nodes") or []) == 3
                assert history.get("count") == 2
                assert diff.get("summary", {}).get("added_node_count") == 1
                assert partitions.get("partition_count") == 1
                assert (status.get("partitions") or {}).get("summary") == "single partition"
                assert len(neighbors.get("neighbors") or []) >= 2
                assert len(children.get("children") or []) == 1
                assert route.get("complete") is True
                assert route.get("hop_count") == 2
                assert stale.get("count") == 1
                assert network_data.get("count") == 1
                assert network_row.get("partition_id") == 1234
                assert node_analysis.get("node", {}).get("eui64") == "1111111111111111"
                assert chat_stats.get("total_turns") == 1
                assert assessment_state.get("enabled") is True
                assert assessment_findings.get("count") == 1
                assert assessment_history.get("count") == 2

                for case in prompts:
                    prompt = str(case.get("prompt") or "").strip()
                    case_id = str(case.get("id") or "unknown")
                    body = _assert_status(
                        f"/v1/chat/turn [{case_id}]",
                        client.post("/v1/chat/turn", json={"message": prompt}),
                    )
                    text = ((body.get("response") or {}).get("text") if isinstance(body.get("response"), dict) else "")
                    assert text == f"local-smoke::{prompt}"

                for prompt, rendered_message in runtime["seen_messages"]:
                    assert prompt in rendered_message
                    assert "Page context:" not in rendered_message
                    assert "graph_diagnostics" not in rendered_message
                    assert "active_tab" not in rendered_message

                print(f"API smoke passed: 16 endpoints + {len(prompts)} chat prompts")
        finally:
            chat_memory.reset()
            reset_store_for_tests(None)
            store.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run in-process API smoke checks without a Home Assistant deployment.")
    parser.add_argument("--prompts", type=Path, default=DEFAULT_PROMPTS, help="Path to the chat prompt regression fixture JSON.")
    args = parser.parse_args()
    run_smoke(args.prompts.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())