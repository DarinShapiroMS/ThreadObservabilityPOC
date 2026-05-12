"""OTBR REST API ingestion.

Fetches Thread Border Router identity & state from the HA OTBR add-on's REST
API and persists it as a node in our store. This is what makes the OTBR
appear as a first-class entry in the Thread Nodes table and lets the graph
resolve route-table edges that point at the border router.

We deliberately try several candidate base URLs because the HA OTBR add-on
exposes its REST API on different paths/ports depending on version. Whichever
responds first wins and is cached for subsequent cycles.

This module is intentionally tolerant: if the OTBR REST API is unreachable
the ingest loop just logs and moves on — Matter-cluster-53 discovery still
gives us the rest of the mesh.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import httpx

from ..storage.sqlite_store import SQLiteStore, get_store

log = logging.getLogger(__name__)

# Default candidates, tried in order. The first that returns a usable /node
# response is cached in ``_cached_base_url`` for subsequent ingest cycles.
# Order: env override → supervisor proxy variants → direct container hostnames.
_DEFAULT_CANDIDATES: tuple[str, ...] = (
    "http://supervisor:9203/addon/core_openthread_border_router/api",
    "http://core-openthread-border-router:8081",
    "http://otbr:8081",
    "http://core-openthread-border-router.local.hass.io:8081",
)

# OTBR State string → our routing_role vocabulary (Matter cluster 53 names).
_STATE_TO_ROUTING_ROLE: dict[str, str] = {
    "leader": "leader",
    "router": "router",
    "child": "end_device",
    "detached": "unassigned",
    "disabled": "unspecified",
}

_cached_base_url: str | None = None
_OTBR_FRIENDLY_NAME = "Thread Border Router"
_OTBR_ROLE = "border_router"


def _candidate_base_urls() -> list[str]:
    """Return ordered list of base URLs to probe, env override first."""
    env_override = os.getenv("OTBR_REST_BASE_URL", "").strip()
    cached = _cached_base_url
    seen: set[str] = set()
    out: list[str] = []
    for url in (env_override, cached, *_DEFAULT_CANDIDATES):
        if not url:
            continue
        url = url.rstrip("/")
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _normalize_eui(raw: str) -> str:
    """Normalize an OTBR ExtAddress hex string to 16-char lowercase hex."""
    s = (raw or "").strip()
    if s.startswith("0x"):
        s = s[2:]
    s = s.replace(":", "").replace("-", "")
    return s.lower().zfill(16)[-16:]


def _extract_leader_data(node: dict[str, Any]) -> tuple[int | None, int | None, int | None]:
    """Return (partition_id, leader_router_id, weighting) from /node payload."""
    ld = node.get("LeaderData") or node.get("leader_data") or {}
    if not isinstance(ld, dict):
        return (None, None, None)
    def _maybe_int(v: Any) -> int | None:
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None
    return (
        _maybe_int(ld.get("PartitionId") or ld.get("partition_id")),
        _maybe_int(ld.get("LeaderRouterId") or ld.get("leader_router_id")),
        _maybe_int(ld.get("Weighting") or ld.get("weighting")),
    )


def _extract_ext_address(node: dict[str, Any]) -> str | None:
    raw = (
        node.get("ExtAddress")
        or node.get("ext_address")
        or node.get("extendedAddress")
    )
    if not raw:
        return None
    try:
        eui = _normalize_eui(str(raw))
        if len(eui) == 16 and all(c in "0123456789abcdef" for c in eui):
            return eui
    except Exception:  # noqa: BLE001
        return None
    return None


def _extract_state(node: dict[str, Any]) -> str | None:
    raw = node.get("State") or node.get("state")
    if not raw:
        return None
    return str(raw).strip().lower() or None


async def fetch_otbr_node(base_url: str, *, timeout: float = 10.0) -> dict[str, Any]:
    """GET ``{base_url}/node`` and return parsed JSON. Raises httpx errors."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(f"{base_url}/node", headers={"Accept": "application/json"})
        resp.raise_for_status()
        payload = resp.json()
    if not isinstance(payload, dict):
        raise ValueError(f"OTBR /node returned non-dict payload: {type(payload).__name__}")
    return payload


async def _probe_for_otbr() -> tuple[str, dict[str, Any]] | None:
    """Try each candidate URL until one returns a usable /node response.

    Returns ``(base_url, node_payload)`` on success, ``None`` if all fail.
    Caches the winning URL in module state.
    """
    global _cached_base_url
    for base in _candidate_base_urls():
        try:
            node = await fetch_otbr_node(base)
        except Exception as exc:  # noqa: BLE001
            log.debug("otbr_rest: probe %s failed: %s", base, exc)
            continue
        if _extract_ext_address(node) is None:
            log.debug("otbr_rest: probe %s returned data without ExtAddress", base)
            continue
        log.info("otbr_rest: using base URL %s", base)
        _cached_base_url = base
        return base, node
    # If nothing succeeded, clear cache so we re-probe from scratch next cycle.
    _cached_base_url = None
    return None


async def ingest_once(store: SQLiteStore | None = None) -> dict[str, Any]:
    """Probe OTBR REST API, upsert the border-router node, persist diagnostics.

    Returns a small status dict ``{"error", "base_url", "eui64",
    "partition_id", "routing_role"}`` for diagnostics surfaces.
    """
    s = store or get_store()
    probe = await _probe_for_otbr()
    if probe is None:
        return {
            "error": "OTBR REST API unreachable on all candidate URLs",
            "base_url": None,
            "eui64": None,
            "partition_id": None,
            "routing_role": None,
        }
    base_url, node = probe
    eui64 = _extract_ext_address(node)
    if not eui64:
        # Probe filter guarantees this won't happen, but keep the type-checker happy.
        return {
            "error": "OTBR /node missing ExtAddress",
            "base_url": base_url,
            "eui64": None,
            "partition_id": None,
            "routing_role": None,
        }

    state = _extract_state(node)
    routing_role = _STATE_TO_ROUTING_ROLE.get(state or "", None)
    partition_id, leader_router_id, weighting = _extract_leader_data(node)
    num_of_router_raw = node.get("NumOfRouter") or node.get("num_of_router")
    try:
        active_routers = int(num_of_router_raw) if num_of_router_raw is not None else None
    except (TypeError, ValueError):
        active_routers = None

    # Only set friendly_name on first insert — never overwrite a user rename.
    existing = s.get_node(eui64)
    desired_friendly = None if (existing and existing.get("friendly_name")) else _OTBR_FRIENDLY_NAME

    s.upsert_node_metadata(
        eui64=eui64,
        friendly_name=desired_friendly,
        role=_OTBR_ROLE,
    )
    s.set_node_diagnostics(
        eui64,
        partition_id=partition_id,
        leader_router_id=leader_router_id,
        routing_role=routing_role,
        active_routers=active_routers,
        weighting=weighting,
    )
    # Mark referenced so phantom-sweep treats it as live.
    s.bump_last_referenced([eui64])

    log.info(
        "otbr_rest: ingested OTBR eui=%s state=%s partition=%s leader_router=%s active_routers=%s",
        eui64, state, partition_id, leader_router_id, active_routers,
    )
    return {
        "error": None,
        "base_url": base_url,
        "eui64": eui64,
        "partition_id": partition_id,
        "leader_router_id": leader_router_id,
        "routing_role": routing_role,
        "state": state,
        "active_routers": active_routers,
    }


async def run_forever(interval_seconds: int = 60) -> None:
    """Loop forever, calling :func:`ingest_once` every ``interval_seconds``.

    Sleeps before the first call so startup probes don't all stampede the
    Supervisor at once.
    """
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            await ingest_once()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("otbr_rest: ingest loop iteration failed")
