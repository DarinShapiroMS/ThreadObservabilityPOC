"""Home Assistant entity-availability lookup.

"Online" in Thread Observability v0.9.39+ means **HA can control this
device right now**, not "we saw radio traffic recently". This module is
the bridge from the user's mental model (entity availability in the UI)
to our internal ``nodes.available`` column.

The lookup has three pieces:

1. **Entity registry** — read from ``/config/.storage/core.entity_registry``
   to build ``{entity_id: device_id}``. Filtering to "primary control"
   entities (light/switch/cover/sensor/binary_sensor/fan/lock/climate)
   keeps diagnostic entities (battery low, identify, etc.) from voting.
2. **State fetch** — call HA REST ``/api/states`` via the Supervisor's HA
   proxy (``http://supervisor/core/api/states``) with the
   ``SUPERVISOR_TOKEN`` we already have. A state is considered *reachable*
   when it is not ``unavailable`` and not ``unknown``.
3. **Roll-up** — a device is ``available = True`` iff any of its primary
   entities is reachable; ``available = False`` iff all are unreachable;
   ``available = None`` iff the device has no entities we can score
   (e.g. OTBR has no HA entities — caller supplies its own answer there).

The fetch is best-effort: every failure path (missing token, file not
present, REST 4xx/5xx, JSON parse error) returns an empty result rather
than raising. Discovery degrades to the legacy ``last_referenced_at``
fallback in ``recompute_node_statuses`` when this module returns nothing.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Iterable

import httpx

log = logging.getLogger(__name__)

# Entity domains whose state we treat as "the device works". Diagnostic
# domains (sensor for battery / signal_strength, update, button, etc.) are
# excluded because their state can read as `unavailable` even when the
# device is fully controllable.
_PRIMARY_DOMAINS: frozenset[str] = frozenset(
    {
        "light",
        "switch",
        "cover",
        "fan",
        "lock",
        "climate",
        "media_player",
        "vacuum",
        "humidifier",
        "water_heater",
        "valve",
    }
)

# Fallback: when a device has zero primary-domain entities, we score it
# from these instead so battery-only sensors and binary contact sensors
# still produce a signal. Listed in priority order.
_FALLBACK_DOMAINS: frozenset[str] = frozenset(
    {"binary_sensor", "sensor", "event", "select", "number"}
)

# States that count as "HA cannot reach the device".
_UNREACHABLE_STATES: frozenset[str] = frozenset({"unavailable", "unknown", "none", ""})

HA_CONFIG_DIR = Path(os.getenv("HA_CONFIG_DIR", "/config"))
ENTITY_REGISTRY_PATH = HA_CONFIG_DIR / ".storage" / "core.entity_registry"

SUPERVISOR_URL = os.getenv("SUPERVISOR_URL", "http://supervisor")
SUPERVISOR_TOKEN_ENV = "SUPERVISOR_TOKEN"


def _load_entity_registry() -> list[dict[str, Any]]:
    """Read HA's entity registry. Returns ``[]`` on any failure."""
    try:
        if not ENTITY_REGISTRY_PATH.exists():
            log.debug("entity registry not found at %s", ENTITY_REGISTRY_PATH)
            return []
        raw = json.loads(ENTITY_REGISTRY_PATH.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("failed to load entity registry: %s", exc)
        return []
    entries = raw.get("data", {}).get("entities", [])
    return entries if isinstance(entries, list) else []


def _build_device_to_entities(
    entries: Iterable[dict[str, Any]],
) -> dict[str, list[tuple[str, str]]]:
    """Group entity_id by device_id, ignoring disabled / hidden entities.

    Returns ``{device_id: [(domain, entity_id), ...]}``.
    """
    out: dict[str, list[tuple[str, str]]] = {}
    for entry in entries:
        device_id = entry.get("device_id")
        entity_id = entry.get("entity_id")
        if not device_id or not entity_id:
            continue
        # Skip disabled or hidden entities — they won't have live states.
        if entry.get("disabled_by") or entry.get("hidden_by"):
            continue
        domain = str(entity_id).split(".", 1)[0]
        out.setdefault(device_id, []).append((domain, entity_id))
    return out


async def _fetch_states() -> dict[str, str]:
    """Fetch ``{entity_id: state}`` via the Supervisor HA proxy.

    Requires ``homeassistant_api: true`` in ``config.yaml`` (which gives us
    a ``SUPERVISOR_TOKEN`` permission to call ``/core/api/...``). Returns
    an empty dict on any failure.
    """
    token = os.getenv(SUPERVISOR_TOKEN_ENV)
    if not token:
        log.debug("SUPERVISOR_TOKEN not set; skipping availability fetch")
        return {}
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    url = f"{SUPERVISOR_URL}/core/api/states"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            log.info("availability: GET %s -> HTTP %s", url, resp.status_code)
            return {}
        payload = resp.json()
    except (httpx.HTTPError, json.JSONDecodeError, ValueError) as exc:
        log.warning("availability: failed to fetch HA states: %s", exc)
        return {}
    if not isinstance(payload, list):
        return {}
    out: dict[str, str] = {}
    for st in payload:
        eid = st.get("entity_id")
        state = st.get("state")
        if isinstance(eid, str) and isinstance(state, str):
            out[eid] = state
    return out


def _score_device(
    entities: list[tuple[str, str]],
    states: dict[str, str],
) -> bool | None:
    """Decide a device's availability from its entities' states.

    Returns ``True`` (HA can reach at least one primary entity), ``False``
    (HA cannot reach any), or ``None`` (no scoreable entities — caller
    should treat as "unknown" / leave the column NULL).
    """
    primary = [(d, eid) for d, eid in entities if d in _PRIMARY_DOMAINS]
    pool = primary or [(d, eid) for d, eid in entities if d in _FALLBACK_DOMAINS]
    if not pool:
        return None
    saw_any_state = False
    for _domain, eid in pool:
        state = states.get(eid)
        if state is None:
            continue
        saw_any_state = True
        if state.lower() not in _UNREACHABLE_STATES:
            return True
    if not saw_any_state:
        return None
    return False


async def fetch_device_availability() -> dict[str, bool]:
    """Return ``{device_id: available_bool}`` for every HA device we can score.

    Devices that yield ``None`` from :func:`_score_device` are omitted from
    the result so the caller can distinguish "explicitly unreachable" from
    "no data" (the database column stays ``NULL`` for the latter).
    """
    entries = _load_entity_registry()
    if not entries:
        return {}
    device_to_entities = _build_device_to_entities(entries)
    if not device_to_entities:
        return {}
    states = await _fetch_states()
    if not states:
        return {}
    out: dict[str, bool] = {}
    for device_id, entities in device_to_entities.items():
        score = _score_device(entities, states)
        if score is not None:
            out[device_id] = score
    log.info(
        "availability: scored %d/%d HA devices (states=%d, entities=%d)",
        len(out),
        len(device_to_entities),
        len(states),
        sum(len(v) for v in device_to_entities.values()),
    )
    return out


__all__ = [
    "fetch_device_availability",
    "_build_device_to_entities",
    "_score_device",
]
