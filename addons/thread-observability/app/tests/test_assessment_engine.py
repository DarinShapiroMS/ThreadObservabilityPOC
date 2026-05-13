"""Tests for the assessment engine envelope + dedup + degradation (#19)."""

from __future__ import annotations

import asyncio
import json

import pytest

from thread_observability.services.assessment.engine import (
    AssessmentEngine,
    EnvelopeParseError,
    VerdictEnvelope,
    finding_key_for,
    parse_envelope,
)
from thread_observability.storage.sqlite_store import SQLiteStore


def _run(coro):
    return asyncio.run(coro)


def test_parse_envelope_happy_path() -> None:
    raw = json.dumps(
        {
            "verdict": "investigate",
            "severity": "investigate",
            "confidence": 0.82,
            "headline": "Eve Door has flapped parents 4 times",
            "evidence": [
                {"tool": "analyze_node", "key_finding": "4 parent changes/1h"},
                {"tool": "get_counter_series", "key_finding": "tx_retry +120"},
            ],
            "suggested_starter_prompt": "Tell me what's wrong with Eve Door.",
            "node_eui64": "EE3F4567ABCDEF12",
            "finding_type": "parent_flapping",
        }
    )
    env = parse_envelope(raw)
    assert env.verdict == "investigate"
    assert env.confidence == 0.82
    assert env.node_eui64 == "EE3F4567ABCDEF12"
    assert len(env.evidence) == 2


def test_parse_envelope_strips_markdown_fence() -> None:
    fenced = (
        "```json\n"
        '{"verdict":"ok","severity":"watch","confidence":0.9,'
        '"headline":"all good","evidence":[]}\n'
        "```"
    )
    env = parse_envelope(fenced)
    assert env.verdict == "ok"


def test_parse_envelope_rejects_investigate_with_one_evidence() -> None:
    with pytest.raises(EnvelopeParseError):
        parse_envelope(
            {
                "verdict": "investigate",
                "severity": "investigate",
                "confidence": 0.9,
                "headline": "bad",
                "evidence": [{"tool": "x", "key_finding": "y"}],
            }
        )


def test_parse_envelope_rejects_bad_verdict() -> None:
    with pytest.raises(EnvelopeParseError):
        parse_envelope({"verdict": "panic", "headline": "x", "evidence": []})


def test_parse_envelope_caps_headline() -> None:
    env = parse_envelope(
        {
            "verdict": "ok",
            "severity": "watch",
            "confidence": 0.5,
            "headline": "x" * 500,
            "evidence": [],
        }
    )
    assert len(env.headline) <= 120


def test_finding_key_stable_per_node_and_type() -> None:
    a = VerdictEnvelope(
        verdict="investigate",
        severity="investigate",
        confidence=0.9,
        headline="parent flap",
        evidence=[],
        node_eui64="AA",
        finding_type="parent_flapping",
    )
    b = VerdictEnvelope(
        verdict="investigate",
        severity="investigate",
        confidence=0.7,
        headline="parent flap again",
        evidence=[],
        node_eui64="AA",
        finding_type="parent_flapping",
    )
    assert finding_key_for(a) == finding_key_for(b)


def test_engine_no_agent_degrades_to_ok(store: SQLiteStore) -> None:
    eng = AssessmentEngine(agent=None, store=store)
    res = _run(eng.run_once())
    assert res.envelope.verdict == "ok"
    assert res.finding_id is None


def test_engine_persists_investigate_finding(store: SQLiteStore) -> None:
    async def agent(*, prompt, context):  # noqa: ARG001
        return json.dumps(
            {
                "verdict": "investigate",
                "severity": "investigate",
                "confidence": 0.8,
                "headline": "node x partition changes",
                "evidence": [
                    {"tool": "a", "key_finding": "b"},
                    {"tool": "c", "key_finding": "d"},
                ],
                "node_eui64": "AA",
                "finding_type": "partition_anomaly",
            }
        )

    eng = AssessmentEngine(agent=agent, store=store)
    res = _run(eng.run_once())
    assert res.finding_id is not None
    assert res.envelope.verdict == "investigate"
    rows = store.list_assessment_findings()
    assert len(rows) == 1


def test_engine_dedup_on_same_key(store: SQLiteStore) -> None:
    async def agent(*, prompt, context):  # noqa: ARG001
        return json.dumps(
            {
                "verdict": "investigate",
                "severity": "investigate",
                "confidence": 0.6,
                "headline": "x",
                "evidence": [
                    {"tool": "a", "key_finding": "b"},
                    {"tool": "c", "key_finding": "d"},
                ],
                "node_eui64": "BB",
                "finding_type": "parent_flapping",
            }
        )

    eng = AssessmentEngine(agent=agent, store=store)
    r1 = _run(eng.run_once())
    r2 = _run(eng.run_once())
    assert r2.dedup_hit is True
    assert r1.finding_id == r2.finding_id


def test_engine_ok_clears_open_finding(store: SQLiteStore) -> None:
    async def inv(*, prompt, context):  # noqa: ARG001
        return json.dumps(
            {
                "verdict": "investigate",
                "severity": "investigate",
                "confidence": 0.6,
                "headline": "x",
                "evidence": [
                    {"tool": "a", "key_finding": "b"},
                    {"tool": "c", "key_finding": "d"},
                ],
                "node_eui64": "CC",
                "finding_type": "parent_flapping",
            }
        )

    async def ok(*, prompt, context):  # noqa: ARG001
        return json.dumps(
            {
                "verdict": "ok",
                "severity": "watch",
                "confidence": 0.95,
                "headline": "all good",
                "evidence": [],
                "node_eui64": "CC",
                "finding_type": "parent_flapping",
            }
        )

    eng_inv = AssessmentEngine(agent=inv, store=store)
    _run(eng_inv.run_once())
    eng_ok = AssessmentEngine(agent=ok, store=store)
    res = _run(eng_ok.run_once())
    assert res.cleared_count == 1
    open_rows = store.list_assessment_findings(state="open")
    assert open_rows == []


def test_engine_parse_failure_retries_then_degrades(store: SQLiteStore) -> None:
    calls = {"n": 0}

    async def flaky(*, prompt, context):  # noqa: ARG001
        calls["n"] += 1
        return "not json at all"

    eng = AssessmentEngine(agent=flaky, store=store)
    res = _run(eng.run_once())
    assert calls["n"] == 2
    assert res.envelope.verdict == "ok"
    assert res.envelope.finding_type == "parse_failure"


def test_engine_respects_suppression(store: SQLiteStore) -> None:
    async def inv(*, prompt, context):  # noqa: ARG001
        return json.dumps(
            {
                "verdict": "investigate",
                "severity": "investigate",
                "confidence": 0.6,
                "headline": "x",
                "evidence": [
                    {"tool": "a", "key_finding": "b"},
                    {"tool": "c", "key_finding": "d"},
                ],
                "node_eui64": "DD",
                "finding_type": "parent_flapping",
            }
        )

    eng = AssessmentEngine(agent=inv, store=store)
    r1 = _run(eng.run_once())
    store.dismiss_assessment_finding(r1.finding_id, suppress_seconds=3600)
    r2 = _run(eng.run_once())
    assert r2.suppressed is True
    assert r2.finding_id is None
