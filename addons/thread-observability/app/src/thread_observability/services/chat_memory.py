from __future__ import annotations

import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from ..storage.sqlite_store import get_store

_SESSION_TTL_SECONDS = 6 * 60 * 60
_MAX_FACTS = 8
_MAX_RECENT_TOOLS = 6
_MAX_GOAL_CHARS = 240
_MAX_HYPOTHESES = 4
_MAX_PENDING_QUESTIONS = 4
_MAX_TRANSCRIPT_TURNS = 30
_NODE_EUI64_RE = re.compile(r"\b([0-9a-f]{16})\b", re.IGNORECASE)
_QUESTION_PREFIXES = (
    "what ",
    "why ",
    "how ",
    "which ",
    "when ",
    "where ",
    "who ",
    "is ",
    "are ",
    "can ",
    "could ",
    "should ",
    "do ",
    "does ",
    "did ",
)
_RESPONSE_BLOCKERS = (
    "request failed",
    "i couldn't complete",
    "i could not complete",
    "please retry",
    "need more evidence",
    "i don't know",
    "i do not know",
    "not enough evidence",
    "context_length_exceeded",
)
_RESPONSE_HYPOTHESIS_PATTERNS: tuple[tuple[tuple[str, ...], str], ...] = (
    (
        ("stale thread dataset", "stale dataset", "credentials mismatch"),
        "Stale Thread dataset or credentials mismatch may explain the observed behavior.",
    ),
    (
        ("partition split", "split-brain", "two partitions"),
        "Partition split may explain the observed behavior.",
    ),
    (
        ("recommission", "identity churn", "ghost device", "duplicate physical identity"),
        "Recent recommission or identity churn may be relevant.",
    ),
    (
        ("weak link", "weak rf", "poor rssi", "link quality"),
        "Weak RF link quality may be contributing to the issue.",
    ),
)

log = logging.getLogger(__name__)


@dataclass(slots=True)
class SessionFact:
    key: str
    text: str
    source: str
    observed_at: float


@dataclass(slots=True)
class ChatSessionState:
    conversation_id: str
    created_at: float
    updated_at: float
    current_goal: str | None = None
    selected_node_eui64: str | None = None
    selected_partition_ids: list[int] = field(default_factory=list)
    confirmed_facts: list[SessionFact] = field(default_factory=list)
    hypotheses: list[str] = field(default_factory=list)
    pending_questions: list[str] = field(default_factory=list)
    recent_tools: list[str] = field(default_factory=list)
    transcript_turns: list[dict[str, Any]] = field(default_factory=list)


class ChatSessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, ChatSessionState] = {}

    def ensure_session(self, conversation_id: str | None) -> ChatSessionState:
        session_id = str(conversation_id or f"chat-{uuid.uuid4()}").strip()
        now = time.time()
        self._prune(now)
        existing = self._sessions.get(session_id)
        if existing is not None:
            existing.updated_at = now
            return existing
        persisted = self._load_persisted(session_id)
        if persisted is not None:
            persisted.updated_at = now
            self._sessions[session_id] = persisted
            return persisted
        state = ChatSessionState(
            conversation_id=session_id,
            created_at=now,
            updated_at=now,
        )
        self._sessions[session_id] = state
        return state

    def build_prompt_context(self, conversation_id: str | None) -> dict[str, Any] | None:
        if not conversation_id:
            return None
        session_id = str(conversation_id).strip()
        state = self._sessions.get(session_id)
        if state is None:
            state = self._load_persisted(session_id)
            if state is not None:
                self._sessions[session_id] = state
        if state is None:
            return None
        payload: dict[str, Any] = {}
        if state.current_goal:
            payload["current_goal"] = state.current_goal
        focus: dict[str, Any] = {}
        if state.selected_node_eui64:
            focus["selected_node_eui64"] = state.selected_node_eui64
        if state.selected_partition_ids:
            focus["selected_partition_ids"] = state.selected_partition_ids[:4]
        if focus:
            payload["focus"] = focus
        if state.confirmed_facts:
            payload["confirmed_facts"] = [fact.text for fact in state.confirmed_facts[:_MAX_FACTS]]
        if state.hypotheses:
            payload["hypotheses"] = state.hypotheses[:_MAX_HYPOTHESES]
        if state.pending_questions:
            payload["pending_questions"] = state.pending_questions[:_MAX_PENDING_QUESTIONS]
        if state.recent_tools:
            payload["recent_tools"] = state.recent_tools[:_MAX_RECENT_TOOLS]
        return payload or None

    def record_turn(
        self,
        *,
        conversation_id: str,
        message: str,
        page_context: dict[str, Any] | None,
        tool_calls: list[dict[str, Any]] | None,
        response_text: str | None = None,
        backend: str | None = None,
        agent_id: str | None = None,
        model_name: str | None = None,
        transcript: dict[str, Any] | None = None,
        persist: bool = True,
        persist_days: int | None = None,
    ) -> ChatSessionState:
        state = self.ensure_session(conversation_id)
        state.updated_at = time.time()
        goal = " ".join(str(message or "").split())
        state.current_goal = goal[:_MAX_GOAL_CHARS] if goal else state.current_goal
        asked_question = goal[:_MAX_GOAL_CHARS] if self._looks_like_question(message) else None
        if asked_question:
            self._push_pending_question(state, asked_question)

        selected_node = None
        if isinstance(page_context, dict):
            selected_node = page_context.get("selected_node_eui64")
            snapshot = page_context.get("snapshot_summary") if isinstance(page_context.get("snapshot_summary"), dict) else None
            if snapshot:
                partition_count = int(snapshot.get("partition_count") or 0)
                distinct_networks = int(snapshot.get("distinct_thread_networks") or 0)
                if partition_count > 1:
                    self._set_fact(
                        state,
                        key="dashboard_partition_count",
                        text=f"Dashboard snapshot shows {partition_count} Thread partitions.",
                        source="page_context",
                    )
                    self._set_hypothesis(
                        state,
                        "Partition split or stale Thread dataset may explain the observed behavior.",
                    )
                if distinct_networks > 1:
                    self._set_fact(
                        state,
                        key="dashboard_distinct_networks",
                        text=f"Dashboard snapshot shows {distinct_networks} distinct Thread networks.",
                        source="page_context",
                    )
        if not selected_node:
            selected_node = self._extract_node_eui64(message)
        if selected_node:
            state.selected_node_eui64 = str(selected_node).lower()

        for call in tool_calls or []:
            name = str(call.get("name") or "").strip()
            if not name:
                continue
            state.recent_tools = [tool for tool in state.recent_tools if tool != name]
            state.recent_tools.insert(0, name)
            state.recent_tools = state.recent_tools[:_MAX_RECENT_TOOLS]
            result = call.get("result") if isinstance(call.get("result"), dict) else call.get("result")
            self._derive_facts_from_tool(state, name, result)
        self._update_from_response(
            state,
            asked_question=asked_question,
            response_text=response_text,
            tool_call_count=len(tool_calls or []),
        )
        state.transcript_turns.insert(
            0,
            {
                "recorded_at": self._to_iso(state.updated_at),
                "backend": str(backend or "").strip() or None,
                "agent_id": str(agent_id or "").strip() or None,
                "model_name": str(model_name or "").strip() or None,
                "message": message,
                "page_context": page_context,
                "tool_calls": tool_calls or [],
                "response_text": response_text,
                "transcript": transcript,
            },
        )
        state.transcript_turns = state.transcript_turns[:_MAX_TRANSCRIPT_TURNS]
        if persist:
            self._persist(state, persist_days=persist_days)
        return state

    def get_session_snapshot(self, conversation_id: str | None) -> dict[str, Any] | None:
        if not conversation_id:
            return None
        state = self._sessions.get(str(conversation_id).strip())
        if state is None:
            state = self._load_persisted(str(conversation_id).strip())
            if state is not None:
                self._sessions[state.conversation_id] = state
        if state is None:
            return None
        payload = self._serialize_state(state)
        return {
            "conversation_id": state.conversation_id,
            "created_at": self._to_iso(state.created_at),
            "updated_at": self._to_iso(state.updated_at),
            **payload,
            "turn_count": len(state.transcript_turns),
        }

    def reset(self) -> None:
        self._sessions.clear()

    def _set_fact(self, state: ChatSessionState, *, key: str, text: str, source: str) -> None:
        now = time.time()
        fact = SessionFact(key=key, text=text, source=source, observed_at=now)
        state.confirmed_facts = [item for item in state.confirmed_facts if item.key != key]
        state.confirmed_facts.insert(0, fact)
        state.confirmed_facts = state.confirmed_facts[:_MAX_FACTS]

    def _derive_facts_from_tool(self, state: ChatSessionState, name: str, result: Any) -> None:
        if name == "analyze_node" and isinstance(result, dict):
            node = result.get("node") if isinstance(result.get("node"), dict) else {}
            eui64 = str(result.get("eui64") or node.get("eui64") or "").strip().lower()
            if eui64:
                state.selected_node_eui64 = eui64
            friendly = node.get("friendly_name") or eui64
            status = node.get("status")
            partition_id = node.get("partition_id")
            if friendly and (status or partition_id is not None):
                bits = [str(friendly)]
                if status:
                    bits.append(f"is currently {status}")
                if partition_id is not None:
                    bits.append(f"on partition {partition_id}")
                    self._push_partition(state, partition_id)
                self._set_fact(
                    state,
                    key=f"node_status:{eui64 or friendly}",
                    text="Node focus: " + ", ".join(bits) + ".",
                    source=name,
                )
            timeline = result.get("timeline") if isinstance(result.get("timeline"), list) else []
            timeline_kinds = [str(row.get("kind") or "") for row in timeline if isinstance(row, dict) and row.get("kind")]
            notable = [kind for kind in timeline_kinds if kind in {"re_attached_node", "parent_change", "status_change", "issue.opened", "issue.closed"}]
            if notable:
                joined = ", ".join(dict.fromkeys(notable))
                self._set_fact(
                    state,
                    key=f"node_timeline:{eui64 or friendly}",
                    text=f"Recent node timeline includes: {joined}.",
                    source=name,
                )
            if "re_attached_node" in notable:
                self._set_hypothesis(
                    state,
                    "Recent recommission or identity churn may be relevant.",
                )
            physical_identity = result.get("physical_identity") if isinstance(result.get("physical_identity"), dict) else None
            if physical_identity and int(physical_identity.get("duplicate_count") or 0) > 1:
                self._set_fact(
                    state,
                    key=f"physical_identity:{eui64 or friendly}",
                    text=f"Physical identity appears under {int(physical_identity.get('duplicate_count') or 0)} EUI64s.",
                    source=name,
                )
                self._set_hypothesis(
                    state,
                    "Duplicate physical identity suggests a recommissioned or ghost device record.",
                )
            return
        if name == "query_history" and isinstance(result, list):
            kinds = [str(row.get("kind") or "") for row in result if isinstance(row, dict) and row.get("kind")]
            notable = [kind for kind in kinds if kind in {"re_attached_node", "parent_change", "status_change", "issue.opened", "issue.closed"}]
            if notable:
                joined = ", ".join(dict.fromkeys(notable[:4]))
                self._set_fact(
                    state,
                    key="recent_history_kinds",
                    text=f"Recent history confirms: {joined}.",
                    source=name,
                )
            partition_ids = []
            for row in result:
                if not isinstance(row, dict):
                    continue
                details = row.get("details") if isinstance(row.get("details"), dict) else None
                if details and details.get("partition_id") is not None:
                    partition_ids.append(int(details.get("partition_id")))
            for partition_id in partition_ids[:4]:
                self._push_partition(state, partition_id)
            return
        if name == "get_mesh_state" and isinstance(result, dict):
            partitions = result.get("all_partitions") if isinstance(result.get("all_partitions"), list) else None
            if partitions is None:
                nodes = result.get("nodes") if isinstance(result.get("nodes"), list) else []
                partitions = sorted({row.get("partition_id") for row in nodes if isinstance(row, dict) and row.get("partition_id") is not None})
            for partition_id in partitions[:4]:
                self._push_partition(state, partition_id)
            if len(partitions) > 1:
                self._set_fact(
                    state,
                    key="mesh_partitions",
                    text=f"Current mesh state shows {len(partitions)} active partitions.",
                    source=name,
                )
                self._set_hypothesis(
                    state,
                    "Partition split or stale Thread dataset may explain the observed behavior.",
                )
            return
        if name == "start_triage" and isinstance(result, dict):
            health = result.get("health") if isinstance(result.get("health"), dict) else {}
            summary = health.get("summary") if isinstance(health.get("summary"), dict) else {}
            offline_nodes = int(summary.get("offline_nodes") or 0)
            distinct_networks = int(summary.get("distinct_thread_networks") or 0)
            if offline_nodes > 0:
                self._set_fact(
                    state,
                    key="triage_offline_nodes",
                    text=f"Triage snapshot reports {offline_nodes} offline nodes.",
                    source=name,
                )
            if distinct_networks > 1:
                self._set_fact(
                    state,
                    key="triage_distinct_networks",
                    text=f"Triage snapshot reports {distinct_networks} distinct Thread networks.",
                    source=name,
                )

    def _set_hypothesis(self, state: ChatSessionState, hypothesis: str) -> None:
        text = " ".join(str(hypothesis or "").split())
        if not text:
            return
        state.hypotheses = [item for item in state.hypotheses if item != text]
        state.hypotheses.insert(0, text)
        state.hypotheses = state.hypotheses[:_MAX_HYPOTHESES]

    def _update_from_response(
        self,
        state: ChatSessionState,
        *,
        asked_question: str | None,
        response_text: str | None,
        tool_call_count: int,
    ) -> None:
        text = " ".join(str(response_text or "").split())
        if not text:
            return
        normalized = text.lower()
        for patterns, hypothesis in _RESPONSE_HYPOTHESIS_PATTERNS:
            if any(pattern in normalized for pattern in patterns):
                self._set_hypothesis(state, hypothesis)
        if asked_question and self._response_resolves_question(normalized, tool_call_count=tool_call_count):
            state.pending_questions = [item for item in state.pending_questions if item != asked_question]

    def _response_resolves_question(self, normalized_response: str, *, tool_call_count: int) -> bool:
        if not normalized_response:
            return False
        if any(marker in normalized_response for marker in _RESPONSE_BLOCKERS):
            return False
        if tool_call_count > 0:
            return True
        return len(normalized_response) >= 24

    def _push_pending_question(self, state: ChatSessionState, question: str) -> None:
        text = " ".join(str(question or "").split())
        if not text:
            return
        state.pending_questions = [item for item in state.pending_questions if item != text]
        state.pending_questions.insert(0, text)
        state.pending_questions = state.pending_questions[:_MAX_PENDING_QUESTIONS]

    def _persist(self, state: ChatSessionState, *, persist_days: int | None = None) -> None:
        created_at = self._to_iso(state.created_at)
        updated_at = self._to_iso(state.updated_at)
        if persist_days is not None:
            expires_at = self._to_iso(state.updated_at + (max(1, int(persist_days)) * 24 * 60 * 60))
        else:
            expires_at = self._to_iso(state.updated_at + _SESSION_TTL_SECONDS)
        payload = self._serialize_state(state)
        try:
            get_store().upsert_chat_session_memory(
                conversation_id=state.conversation_id,
                created_at=created_at,
                updated_at=updated_at,
                expires_at=expires_at,
                payload=payload,
            )
        except Exception:  # noqa: BLE001
            log.exception("chat_memory: failed to persist session %s", state.conversation_id)

    def _load_persisted(self, conversation_id: str) -> ChatSessionState | None:
        try:
            row = get_store().get_chat_session_memory(conversation_id)
        except Exception:  # noqa: BLE001
            log.exception("chat_memory: failed to load session %s", conversation_id)
            return None
        if not row:
            return None
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        return self._deserialize_state(conversation_id, payload)

    def _serialize_state(self, state: ChatSessionState) -> dict[str, Any]:
        return {
            "conversation_id": state.conversation_id,
            "created_at": state.created_at,
            "updated_at": state.updated_at,
            "current_goal": state.current_goal,
            "selected_node_eui64": state.selected_node_eui64,
            "selected_partition_ids": state.selected_partition_ids[:4],
            "confirmed_facts": [
                {
                    "key": fact.key,
                    "text": fact.text,
                    "source": fact.source,
                    "observed_at": fact.observed_at,
                }
                for fact in state.confirmed_facts[:_MAX_FACTS]
            ],
            "hypotheses": state.hypotheses[:_MAX_HYPOTHESES],
            "pending_questions": state.pending_questions[:_MAX_PENDING_QUESTIONS],
            "recent_tools": state.recent_tools[:_MAX_RECENT_TOOLS],
            "transcript_turns": state.transcript_turns[:_MAX_TRANSCRIPT_TURNS],
        }

    def _deserialize_state(self, conversation_id: str, payload: dict[str, Any]) -> ChatSessionState:
        facts_raw = payload.get("confirmed_facts") if isinstance(payload.get("confirmed_facts"), list) else []
        facts = []
        for row in facts_raw[:_MAX_FACTS]:
            if not isinstance(row, dict):
                continue
            facts.append(
                SessionFact(
                    key=str(row.get("key") or "fact"),
                    text=str(row.get("text") or "").strip(),
                    source=str(row.get("source") or "unknown"),
                    observed_at=float(row.get("observed_at") or time.time()),
                )
            )
        return ChatSessionState(
            conversation_id=conversation_id,
            created_at=float(payload.get("created_at") or time.time()),
            updated_at=float(payload.get("updated_at") or time.time()),
            current_goal=str(payload.get("current_goal") or "").strip() or None,
            selected_node_eui64=str(payload.get("selected_node_eui64") or "").strip().lower() or None,
            selected_partition_ids=[int(value) for value in (payload.get("selected_partition_ids") or [])[:4]],
            confirmed_facts=facts,
            hypotheses=[str(value) for value in (payload.get("hypotheses") or [])[:_MAX_HYPOTHESES]],
            pending_questions=[str(value) for value in (payload.get("pending_questions") or [])[:_MAX_PENDING_QUESTIONS]],
            recent_tools=[str(value) for value in (payload.get("recent_tools") or [])[:_MAX_RECENT_TOOLS]],
            transcript_turns=[row for row in (payload.get("transcript_turns") or [])[:_MAX_TRANSCRIPT_TURNS] if isinstance(row, dict)],
        )

    def _to_iso(self, ts: float) -> str:
        return datetime.fromtimestamp(ts, tz=UTC).isoformat()

    def _push_partition(self, state: ChatSessionState, partition_id: Any) -> None:
        try:
            value = int(partition_id)
        except (TypeError, ValueError):
            return
        if value in state.selected_partition_ids:
            return
        state.selected_partition_ids.insert(0, value)
        state.selected_partition_ids = state.selected_partition_ids[:4]

    def _extract_node_eui64(self, text: str) -> str | None:
        match = _NODE_EUI64_RE.search(str(text or ""))
        if not match:
            return None
        return match.group(1).lower()

    def _looks_like_question(self, text: str) -> bool:
        normalized = " ".join(str(text or "").strip().lower().split())
        if not normalized:
            return False
        if normalized.endswith("?"):
            return True
        return normalized.startswith(_QUESTION_PREFIXES)

    def _prune(self, now: float) -> None:
        stale = [
            key
            for key, session in self._sessions.items()
            if (now - session.updated_at) > _SESSION_TTL_SECONDS
        ]
        for key in stale:
            self._sessions.pop(key, None)
        cutoff = datetime.fromtimestamp(now - _SESSION_TTL_SECONDS, tz=UTC).isoformat()
        try:
            get_store().prune_chat_session_memory(stale_before=cutoff)
        except Exception:  # noqa: BLE001
            log.exception("chat_memory: failed to prune persisted sessions")


_STORE = ChatSessionStore()


def ensure_session(conversation_id: str | None) -> ChatSessionState:
    return _STORE.ensure_session(conversation_id)


def build_prompt_context(conversation_id: str | None) -> dict[str, Any] | None:
    return _STORE.build_prompt_context(conversation_id)


def record_turn(
    *,
    conversation_id: str,
    message: str,
    page_context: dict[str, Any] | None,
    tool_calls: list[dict[str, Any]] | None,
    response_text: str | None = None,
    backend: str | None = None,
    agent_id: str | None = None,
    model_name: str | None = None,
    transcript: dict[str, Any] | None = None,
    persist: bool = True,
    persist_days: int | None = None,
) -> ChatSessionState:
    return _STORE.record_turn(
        conversation_id=conversation_id,
        message=message,
        page_context=page_context,
        tool_calls=tool_calls,
        response_text=response_text,
        backend=backend,
        agent_id=agent_id,
        model_name=model_name,
        transcript=transcript,
        persist=persist,
        persist_days=persist_days,
    )


def get_session_snapshot(conversation_id: str | None) -> dict[str, Any] | None:
    return _STORE.get_session_snapshot(conversation_id)


def reset() -> None:
    _STORE.reset()