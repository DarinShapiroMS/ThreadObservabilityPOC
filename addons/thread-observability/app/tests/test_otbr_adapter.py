"""Integration tests for the OTBR adapter (Supervisor mocked via monkeypatch)."""

from __future__ import annotations

import asyncio

import pytest

from thread_observability.pipeline import otbr_adapter


SAMPLE_LOG = """\
2026-05-11T20:14:07Z [N] Mle: Attach attempt 1, AnyPartition
2026-05-11T20:14:08Z [N] Mle: Attach succeeded
2026-05-11T20:14:09Z [I] Mle: Parent response from 1234567890abcdef rss:-60 lqi:200
2026-05-11T20:14:10Z [N] ChildTable: Child added ext_addr=abcdef0011223344
unrelated noise line
2026-05-11T20:14:12Z [W] Mle: AttachState ParentRequest -> Idle (attach failed)
"""


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture()
def patched(monkeypatch: pytest.MonkeyPatch) -> dict:
    state = {"lines": SAMPLE_LOG.splitlines()}
    otbr_adapter._MEM_STATE.clear()

    async def fake_fetch(slug: str, *, max_bytes: int = 256_000) -> list[str]:
        state["last_slug"] = slug
        return list(state["lines"])

    monkeypatch.setattr(otbr_adapter, "fetch_logs", fake_fetch)
    return state


def test_ingest_without_slug_returns_error(store, patched) -> None:
    res = _run(otbr_adapter.ingest_once(store=store))
    assert res["error"] and "slug" in res["error"].lower()
    assert res["events_inserted"] == 0


def test_ingest_inserts_recognised_events(store, patched) -> None:
    otbr_adapter.set_slug("core_openthread_border_router", store=store)
    res = _run(otbr_adapter.ingest_once(store=store))
    assert res["error"] is None
    assert res["events_inserted"] == 5
    assert res["lines_seen"] == len(SAMPLE_LOG.splitlines())

    events = store.query_events(limit=100)
    types = sorted(e["type"] for e in events)
    assert types == sorted([
        "attach_attempt", "attach", "parent_response", "child_added", "attach_failed",
    ])


def test_ingest_is_idempotent_when_no_new_lines(store, patched) -> None:
    otbr_adapter.set_slug("core_openthread_border_router", store=store)
    first = _run(otbr_adapter.ingest_once(store=store))
    assert first["events_inserted"] == 5

    second = _run(otbr_adapter.ingest_once(store=store))
    assert second["error"] is None
    assert second["lines_new"] == 0
    assert second["events_inserted"] == 0
    assert len(store.query_events(limit=100)) == 5


def test_ingest_resumes_after_new_lines_appended(store, patched) -> None:
    otbr_adapter.set_slug("core_openthread_border_router", store=store)
    _run(otbr_adapter.ingest_once(store=store))

    patched["lines"].extend([
        "2026-05-11T20:15:00Z [N] Mle: Detached from parent 1111222233334444",
        "noise",
    ])

    res = _run(otbr_adapter.ingest_once(store=store))
    assert res["lines_new"] == 2
    assert res["events_inserted"] == 1
    types = [e["type"] for e in store.query_events(limit=10)]
    assert types[0] == "detach"


def test_get_state_reflects_progress(store, patched) -> None:
    otbr_adapter.set_slug("core_openthread_border_router", store=store)
    _run(otbr_adapter.ingest_once(store=store))
    state = otbr_adapter.get_state(store=store)
    assert state["slug"] == "core_openthread_border_router"
    assert state["events_total"] == 5
    assert state["last_event_ts"] is not None
    assert state["last_error"] is None


def test_ingest_failure_is_recorded(store, monkeypatch) -> None:
    otbr_adapter._MEM_STATE.clear()
    otbr_adapter.set_slug("core_openthread_border_router", store=store)

    async def boom(slug: str, *, max_bytes: int = 256_000) -> list[str]:
        raise RuntimeError("supervisor unreachable")

    monkeypatch.setattr(otbr_adapter, "fetch_logs", boom)
    res = _run(otbr_adapter.ingest_once(store=store))
    assert res["error"] and "supervisor unreachable" in res["error"]
    assert res["events_inserted"] == 0

    state = otbr_adapter.get_state(store=store)
    assert state["last_error"] and "supervisor unreachable" in state["last_error"]
