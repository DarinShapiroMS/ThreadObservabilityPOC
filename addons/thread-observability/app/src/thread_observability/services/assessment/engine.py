"""Assessment engine — produces verdict envelopes from Thread state (#19).

Architecture
------------

The engine is intentionally **dumb about the LLM**. It:

1. Calls ``start_triage`` to collect the situation snapshot.
2. Optionally walks the ``recommended_next`` tool chain (capped depth)
   for fresher evidence.
3. Hands the structured context to an injectable ``VerdictAgent``
   protocol — in production this points at HA's ``conversation.process``;
   in tests this is a stub that returns a canned envelope.
4. Validates the returned JSON envelope. On parse / shape failure it
   retries once, then degrades to a synthetic ``verdict: "ok"`` so we
   never surface "the AI broke" to the user.
5. Persists / dedups the resulting finding via the store.

The actual conversation-agent wiring (HA REST → ``conversation.process``)
will land with #10. For now we accept any callable implementing
``VerdictAgent``; the runner in ``services.core_service`` decides which
one to inject. Tests pass a stub.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol

from ...storage.sqlite_store import SQLiteStore, get_store

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Verdict envelope shape
# ---------------------------------------------------------------------------

VALID_VERDICTS = {"ok", "watch", "investigate"}
VALID_SEVERITIES = {"watch", "investigate", "critical"}
MAX_HEADLINE = 120
MAX_STARTER_PROMPT = 200
MAX_EVIDENCE_ITEMS = 6


@dataclass(slots=True)
class VerdictEnvelope:
    verdict: str
    severity: str
    confidence: float
    headline: str
    evidence: list[dict[str, Any]]
    suggested_starter_prompt: str | None = None
    node_eui64: str | None = None
    finding_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "severity": self.severity,
            "confidence": self.confidence,
            "headline": self.headline,
            "evidence": self.evidence,
            "suggested_starter_prompt": self.suggested_starter_prompt,
            "node_eui64": self.node_eui64,
            "finding_type": self.finding_type,
        }


class EnvelopeParseError(ValueError):
    """Raised when the agent's reply doesn't conform to the envelope."""


def parse_envelope(raw: str | dict[str, Any]) -> VerdictEnvelope:
    """Parse + validate a verdict envelope from raw agent output.

    Accepts either an already-decoded dict or a JSON string. Tolerates
    ```json fenced blocks. Caps fields to documented limits.
    """
    if isinstance(raw, str):
        text = raw.strip()
        if text.startswith("```"):
            # strip markdown fence
            text = text.strip("`")
            if text.lower().startswith("json"):
                text = text[4:]
            text = text.strip()
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise EnvelopeParseError(f"invalid JSON: {exc}") from exc
    else:
        data = raw

    if not isinstance(data, dict):
        raise EnvelopeParseError("envelope must be a JSON object")

    verdict = str(data.get("verdict") or "").lower()
    if verdict not in VALID_VERDICTS:
        raise EnvelopeParseError(f"verdict must be one of {VALID_VERDICTS}")

    severity = str(data.get("severity") or "").lower() or "watch"
    if severity not in VALID_SEVERITIES:
        raise EnvelopeParseError(f"severity must be one of {VALID_SEVERITIES}")

    try:
        confidence = float(data.get("confidence", 0.0))
    except (TypeError, ValueError) as exc:
        raise EnvelopeParseError(f"confidence not numeric: {exc}") from exc
    confidence = max(0.0, min(1.0, confidence))

    headline = str(data.get("headline") or "").strip()
    if not headline:
        raise EnvelopeParseError("headline is required")
    if len(headline) > MAX_HEADLINE:
        headline = headline[: MAX_HEADLINE - 1] + "…"

    raw_evidence = data.get("evidence") or []
    if not isinstance(raw_evidence, list):
        raise EnvelopeParseError("evidence must be a list")
    evidence: list[dict[str, Any]] = []
    for item in raw_evidence[:MAX_EVIDENCE_ITEMS]:
        if not isinstance(item, dict):
            continue
        tool = str(item.get("tool") or "").strip()
        key_finding = str(item.get("key_finding") or "").strip()
        if not tool or not key_finding:
            continue
        evidence.append({"tool": tool, "key_finding": key_finding})

    # Guardrail: investigate verdicts require at least 2 pieces of evidence.
    if verdict == "investigate" and len(evidence) < 2:
        raise EnvelopeParseError("investigate verdict requires >=2 evidence items")

    starter = data.get("suggested_starter_prompt")
    if starter is not None:
        starter = str(starter).strip()[:MAX_STARTER_PROMPT] or None

    eui64 = data.get("node_eui64")
    if eui64 is not None:
        eui64 = str(eui64).strip().upper() or None

    finding_type = data.get("finding_type")
    if finding_type is not None:
        finding_type = str(finding_type).strip().lower() or None

    return VerdictEnvelope(
        verdict=verdict,
        severity=severity,
        confidence=confidence,
        headline=headline,
        evidence=evidence,
        suggested_starter_prompt=starter,
        node_eui64=eui64,
        finding_type=finding_type,
    )


def finding_key_for(envelope: VerdictEnvelope) -> str:
    """Stable dedup key: hash of (eui64 or '*') + finding_type."""
    parts = [
        envelope.node_eui64 or "*",
        envelope.finding_type or _infer_finding_type(envelope.headline),
    ]
    blob = "|".join(parts).lower()
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


def _infer_finding_type(headline: str) -> str:
    """Cheap keyword-based fallback when the agent omits finding_type."""
    h = headline.lower()
    if "parent" in h and ("flap" in h or "chang" in h):
        return "parent_flapping"
    if "partition" in h:
        return "partition_anomaly"
    if "lqi" in h or "rssi" in h or "link" in h:
        return "weak_link"
    if "retry" in h or "retries" in h:
        return "tx_retry_spike"
    if "phantom" in h or "stale" in h:
        return "stale_or_phantom"
    return "generic"


# ---------------------------------------------------------------------------
# Agent protocol
# ---------------------------------------------------------------------------


class VerdictAgent(Protocol):
    """Callable that turns prompt + context into raw envelope text."""

    async def __call__(self, *, prompt: str, context: dict[str, Any]) -> str: ...


SYSTEM_PROMPT = """You are the Background Diagnostics agent for a Thread mesh observability tool.

You receive a snapshot of the current Thread network and recent context.
Your job: decide whether anything needs the user's attention.

You MUST respond with a single JSON object — no prose, no markdown fence
unless required by the channel — matching this schema:

{
  "verdict": "ok" | "watch" | "investigate",
  "severity": "watch" | "investigate" | "critical",
  "confidence": 0.0-1.0,
  "headline": "<=120 char user-facing summary",
  "evidence": [
    {"tool": "<tool name>", "key_finding": "<one-line observation>"}
  ],
  "suggested_starter_prompt": "<=200 char chat opener",
  "node_eui64": "EUI-64 hex if node-scoped, else omit",
  "finding_type": "short snake_case slug like parent_flapping"
}

Rules:

* Prefer "ok" if the network is healthy or you lack solid evidence.
  Do not invent problems.
* "investigate" requires at least 2 distinct pieces of evidence.
* If unsure, return "watch" — that records a soft signal without
  pestering the user.
* Headline is direct, neutral, free of marketing words. No emoji.
* Cite only tools that are actually present in the context payload.
* No PII. No internal IDs beyond EUI-64.
"""


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AssessmentResult:
    envelope: VerdictEnvelope
    finding_id: str | None
    finding_key: str
    dedup_hit: bool
    parse_attempts: int
    duration_seconds: float
    cleared_count: int = 0
    suppressed: bool = False


class AssessmentEngine:
    """Wraps the verdict workflow against an injected agent."""

    def __init__(
        self,
        *,
        agent: VerdictAgent | None = None,
        triage_fn: Callable[[], Awaitable[dict[str, Any]]] | None = None,
        store: SQLiteStore | None = None,
        system_prompt: str = SYSTEM_PROMPT,
    ) -> None:
        self._agent = agent
        self._triage_fn = triage_fn
        self._store = store or get_store()
        self._system_prompt = system_prompt

    async def run_once(
        self,
        *,
        extra_context: dict[str, Any] | None = None,
    ) -> AssessmentResult:
        """Perform one assessment and persist the resulting finding."""
        start = time.monotonic()
        context = await self._collect_context(extra=extra_context)
        envelope, attempts = await self._ask_agent(context)
        result = self._persist(envelope)
        result.parse_attempts = attempts
        result.duration_seconds = time.monotonic() - start
        return result

    # ----- internals --------------------------------------------------

    async def _collect_context(
        self,
        *,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        triage: dict[str, Any] = {}
        if self._triage_fn is not None:
            try:
                triage = await self._triage_fn()
            except Exception as exc:  # noqa: BLE001
                log.warning("triage call failed during assessment: %s", exc)
                triage = {"error": str(exc)}
        context = {"triage": triage}
        if extra:
            context["extra"] = extra
        return context

    async def _ask_agent(
        self,
        context: dict[str, Any],
    ) -> tuple[VerdictEnvelope, int]:
        attempts = 0
        last_err: Exception | None = None
        if self._agent is None:
            # No agent wired — degrade gracefully.
            return (
                VerdictEnvelope(
                    verdict="ok",
                    severity="watch",
                    confidence=0.0,
                    headline="Background Diagnostics: no agent configured",
                    evidence=[],
                    finding_type="no_agent",
                ),
                0,
            )
        for _attempt in range(2):
            attempts += 1
            try:
                raw = await self._agent(prompt=self._system_prompt, context=context)
                return parse_envelope(raw), attempts
            except EnvelopeParseError as exc:
                last_err = exc
                log.warning(
                    "assessment envelope parse failed (attempt %d): %s",
                    attempts,
                    exc,
                )
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                log.warning("assessment agent call failed (attempt %d): %s", attempts, exc)
        log.info(
            "assessment giving up after %d attempts (%s); treating as ok",
            attempts,
            last_err,
        )
        return (
            VerdictEnvelope(
                verdict="ok",
                severity="watch",
                confidence=0.0,
                headline="Background Diagnostics: agent reply unparseable",
                evidence=[],
                finding_type="parse_failure",
            ),
            attempts,
        )

    def _persist(self, envelope: VerdictEnvelope) -> AssessmentResult:
        key = finding_key_for(envelope)

        if envelope.verdict == "ok":
            cleared = self._store.clear_assessment_findings_by_key(
                key, cleared_by="assessment"
            )
            return AssessmentResult(
                envelope=envelope,
                finding_id=None,
                finding_key=key,
                dedup_hit=False,
                parse_attempts=0,
                duration_seconds=0.0,
                cleared_count=cleared,
            )

        if self._store.is_finding_key_suppressed(key):
            return AssessmentResult(
                envelope=envelope,
                finding_id=None,
                finding_key=key,
                dedup_hit=False,
                parse_attempts=0,
                duration_seconds=0.0,
                suppressed=True,
            )

        finding_id = f"evid-{uuid.uuid4().hex[:12]}"
        before = self._store.get_assessment_finding(finding_id)  # always None
        row = self._store.upsert_assessment_finding(
            finding_id=finding_id,
            finding_key=key,
            verdict=envelope.verdict,
            severity=envelope.severity,
            confidence=envelope.confidence,
            headline=envelope.headline,
            evidence=envelope.evidence,
            suggested_starter_prompt=envelope.suggested_starter_prompt,
            node_eui64=envelope.node_eui64,
            finding_type=envelope.finding_type,
        )
        # If dedup hit, upsert returns the existing row (finding_id != our generated one).
        actual_id = row.get("finding_id", finding_id)
        return AssessmentResult(
            envelope=envelope,
            finding_id=actual_id,
            finding_key=key,
            dedup_hit=actual_id != finding_id,
            parse_attempts=0,
            duration_seconds=0.0,
        )
