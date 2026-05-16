from __future__ import annotations

import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException, status

from .. import supervisor_client
from ..chat_helpers import (
    HA_INTEGRATIONS_URL,
    HA_MCP_CLIENT_URL,
    agent_has_thread_tools,
    augment_chat_page_context,
    chat_turn_via_direct_model,
    chat_turn_via_supervisor_proxy,
    load_chat_starter_prompts,
    render_chat_message,
)
from ...config import ThreadObsConfig
from ...services import chat_memory
from ...services import direct_chat


def _runtime_chat_config(get_config_fn) -> ThreadObsConfig:  # noqa: ANN001
    cfg = get_config_fn()
    options_path = Path(str(getattr(cfg, "options_path", "") or "")).expanduser()
    if getattr(cfg, "options_loaded", False) or options_path.exists():
        try:
            return ThreadObsConfig.load(options_path)
        except Exception:  # noqa: BLE001
            # Best-effort reload; fall back to cached config.
            return cfg
    return cfg


def create_chat_router(
    *,
    get_config_fn,
    get_store_fn,
    utc_now: callable,
) -> APIRouter:
    router = APIRouter()

    @router.get("/v1/chat/agents")
    async def chat_agents() -> dict[str, object]:
        cfg = _runtime_chat_config(get_config_fn)
        starter_prompts = load_chat_starter_prompts()
        if not cfg.chat.enabled:
            return {
                "enabled": False,
                "agents": [],
                "count": 0,
                "source": None,
                "default_backend": direct_chat.default_chat_backend(cfg.ai, None),
                "default_label": "Chat disabled",
                "default_agent_id": str(cfg.chat.default_agent_id or "").strip() or None,
                "send_page_context": bool(cfg.chat.send_page_context),
                "persist_transcripts": bool(cfg.chat.persist_transcripts),
                "chat_retention_days": int(cfg.retention.chat_days),
                "thread_tools_connected": False,
                "mcp_connect_url": HA_MCP_CLIENT_URL,
                "ha_integrations_url": HA_INTEGRATIONS_URL,
                "starter_prompts": starter_prompts,
            }

        direct_target = direct_chat.resolve_direct_chat_target(cfg.ai)
        agents: list[dict[str, object]] = []
        source_parts: list[str] = []
        try:
            payload = await supervisor_client.list_conversation_agents()
            agents.extend(payload.get("agents") or [])
            source = payload.get("source")
            if source:
                source_parts.append(str(source))
        except Exception as exc:  # noqa: BLE001
            if direct_target is None:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Failed to list conversation agents: {exc}",
                ) from exc
        if direct_target is not None:
            agents.insert(0, direct_chat.direct_agent_row(direct_target))
            source_parts.append("direct")
        thread_tools_connected = any(agent_has_thread_tools(agent) for agent in agents)
        return {
            "enabled": True,
            "agents": agents,
            "count": len(agents),
            "source": "+".join(source_parts) if source_parts else None,
            "default_backend": direct_chat.default_chat_backend(cfg.ai, direct_target),
            "default_label": direct_chat.default_chat_label(cfg.ai, direct_target),
            "default_agent_id": str(cfg.chat.default_agent_id or "").strip() or None,
            "send_page_context": bool(cfg.chat.send_page_context),
            "persist_transcripts": bool(cfg.chat.persist_transcripts),
            "chat_retention_days": int(cfg.retention.chat_days),
            "thread_tools_connected": thread_tools_connected,
            "mcp_connect_url": HA_MCP_CLIENT_URL,
            "ha_integrations_url": HA_INTEGRATIONS_URL,
            "starter_prompts": starter_prompts,
        }

    @router.post("/v1/chat/turn")
    async def chat_turn(payload: dict[str, object]) -> dict[str, object]:
        cfg = _runtime_chat_config(get_config_fn)
        if not cfg.chat.enabled:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chat is disabled in add-on options.",
            )
        message = str((payload or {}).get("message") or "").strip()
        if not message:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="message required",
            )
        if bool((payload or {}).get("streaming")):
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="streaming not implemented yet; retry with streaming=false",
            )

        conversation_id = (payload or {}).get("conversation_id")
        conversation_id = str(conversation_id).strip() if conversation_id else None
        agent_id = (payload or {}).get("agent_id")
        agent_id = str(agent_id).strip() if agent_id else None
        if not agent_id:
            agent_id = str(cfg.chat.default_agent_id or "").strip() or None

        page_context = (payload or {}).get("page_context")
        page_context = page_context if isinstance(page_context, dict) else None
        if cfg.chat.send_page_context:
            page_context = augment_chat_page_context(page_context)
        else:
            page_context = None

        direct_target = direct_chat.resolve_direct_chat_target(cfg.ai)
        if direct_chat.direct_chat_preferred(cfg.ai, agent_id, direct_target) and not conversation_id:
            conversation_id = f"direct-{uuid.uuid4()}"

        session_context = chat_memory.build_prompt_context(conversation_id)
        rendered_message = render_chat_message(message, page_context, session_context)

        if direct_chat.direct_chat_preferred(cfg.ai, agent_id, direct_target):
            return await chat_turn_via_direct_model(
                cfg=cfg,
                message=message,
                rendered_message=rendered_message,
                conversation_id=conversation_id,
                agent_id=agent_id,
                page_context=page_context,
                utc_now=utc_now,
                get_store_fn=get_store_fn,
            )

        return await chat_turn_via_supervisor_proxy(
            message=message,
            rendered_message=rendered_message,
            conversation_id=conversation_id,
            agent_id=agent_id,
            page_context=page_context,
            utc_now=utc_now,
            persist_transcripts=bool(cfg.chat.persist_transcripts),
            retention_days=int(cfg.retention.chat_days),
            get_store_fn=get_store_fn,
        )

    @router.get("/v1/chat/stats")
    def chat_stats(since: str | None = None) -> dict[str, object]:
        try:
            return get_store_fn().get_chat_turn_stats(since=since)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @router.get("/v1/chat/transcript/{conversation_id}")
    def chat_transcript(conversation_id: str) -> dict[str, object]:
        snapshot = chat_memory.get_session_snapshot(conversation_id)
        if snapshot is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="conversation transcript not found",
            )
        return snapshot

    return router

