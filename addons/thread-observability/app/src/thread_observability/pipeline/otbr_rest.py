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
from ..utils.coercion import coerce_int, to_tristate_int

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


def _extract_rloc16(node: dict[str, Any]) -> int | None:
    raw = node.get("Rloc16") or node.get("rloc16") or node.get("RLOC16")
    if raw is None:
        return None
    try:
        if isinstance(raw, str):
            s = raw.strip()
            if s.startswith("0x") or s.startswith("0X"):
                return int(s, 16)
            # Without an explicit prefix, treat as decimal. Callers that need
            # hex semantics (very rare) should send "0x..." form.
            return int(s)
        return int(raw)
    except (TypeError, ValueError):
        return None


def _router_id_from_rloc16(rloc16: int | None) -> int | None:
    """Thread Router ID is the top 6 bits of the RLOC16 (low 10 bits are CID).

    Returns ``None`` if the RLOC16 has a non-zero CID (i.e. it's an end-device
    short address, not a router address).
    """
    if rloc16 is None:
        return None
    cid = rloc16 & 0x03FF
    if cid != 0:
        return None
    return (rloc16 >> 10) & 0x3F


async def fetch_otbr_node(base_url: str, *, timeout: float = 10.0) -> dict[str, Any]:
    """GET ``{base_url}/node`` and return parsed JSON. Raises httpx errors."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(f"{base_url}/node", headers={"Accept": "application/json"})
        resp.raise_for_status()
        payload = resp.json()
    if not isinstance(payload, dict):
        raise ValueError(f"OTBR /node returned non-dict payload: {type(payload).__name__}")
    return payload


async def fetch_otbr_dataset_active(
    base_url: str, *, timeout: float = 10.0
) -> dict[str, Any] | None:
    """GET the OTBR's active operational dataset.

    Newer OTBR REST builds expose this at ``/node/dataset/active`` as JSON
    (with fields like ``NetworkName``, ``PanId``, ``ExtPanId``, ``Channel``,
    ``ChannelMask``, ``MeshLocalPrefix``, ``ActiveTimestamp``). Older builds
    return raw TLV hex; in that case we just return ``None`` and let the
    caller fall back to what ``/node`` already gave us.

    Returns ``None`` on any error or non-JSON response — this is best-effort
    enrichment; the OTBR ingest stage must not fail because of it.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(
                f"{base_url}/node/dataset/active",
                headers={"Accept": "application/json"},
            )
        if resp.status_code >= 400:
            return None
        payload = resp.json()
        return payload if isinstance(payload, dict) else None
    except Exception as exc:  # noqa: BLE001
        log.debug("otbr_rest: /node/dataset/active fetch failed: %s", exc)
        return None


async def fetch_otbr_network_data(
    base_url: str, *, timeout: float = 10.0
) -> dict[str, Any] | None:
    """GET ``{base_url}/node/network`` (leader-side Thread Network Data).

    Contains on-mesh prefixes, external routes, BR Server entries, and SRP
    services for the partition. Best-effort: returns ``None`` on any error.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(
                f"{base_url}/node/network",
                headers={"Accept": "application/json"},
            )
        if resp.status_code >= 400:
            return None
        payload = resp.json()
        return payload if isinstance(payload, dict) else None
    except Exception as exc:  # noqa: BLE001
        log.debug("otbr_rest: /node/network fetch failed: %s", exc)
        return None


async def fetch_otbr_neighbors(
    base_url: str, *, timeout: float = 10.0
) -> list[dict[str, Any]] | None:
    """GET ``{base_url}/node/neighbors`` → list of NeighborInfo dicts.

    OpenThread OTBR REST exposes the leader/router's own MLE NeighborTable
    here. Lets us treat the OTBR as a first-class reporter in the ``links``
    table instead of a destination-only node. Best-effort: returns ``None``
    if the endpoint is missing on this OTBR build (older releases) or any
    fetch error occurs — Matter-side discovery still covers the rest.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(
                f"{base_url}/node/neighbors",
                headers={"Accept": "application/json"},
            )
        if resp.status_code >= 400:
            return None
        payload = resp.json()
        if isinstance(payload, list):
            return [p for p in payload if isinstance(p, dict)]
        return None
    except Exception as exc:  # noqa: BLE001
        log.debug("otbr_rest: /node/neighbors fetch failed: %s", exc)
        return None


async def fetch_otbr_routers(
    base_url: str, *, timeout: float = 10.0
) -> list[dict[str, Any]] | None:
    """GET ``{base_url}/node/routers`` → list of RouterInfo dicts.

    This is the OTBR's RouteTable view — what it thinks the path to each
    other router in the partition is, with per-hop LQI and the same
    NextHopRouterId / PathCost / LinkEstablished fields as Matter cluster
    53 attribute 8. Best-effort.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(
                f"{base_url}/node/routers",
                headers={"Accept": "application/json"},
            )
        if resp.status_code >= 400:
            return None
        payload = resp.json()
        if isinstance(payload, list):
            return [p for p in payload if isinstance(p, dict)]
        return None
    except Exception as exc:  # noqa: BLE001
        log.debug("otbr_rest: /node/routers fetch failed: %s", exc)
        return None


# MGMT_DIAG_GET TLV IDs we care about (Thread spec § 8.4.3.2).
# Defaults chosen to mirror the MAC-counter and ChildTable signals we
# already collect from Matter cluster 53, so the two views are
# directly comparable.
OTBR_DIAG_TLV_MAC_COUNTERS = 17
OTBR_DIAG_TLV_CHILD_TABLE = 14
OTBR_DIAG_TLV_EXT_ADDR = 0
OTBR_DIAG_TLV_RLOC16 = 1
OTBR_DEFAULT_DIAG_TLVS: tuple[int, ...] = (
    OTBR_DIAG_TLV_EXT_ADDR,
    OTBR_DIAG_TLV_MAC_COUNTERS,
    OTBR_DIAG_TLV_CHILD_TABLE,
)


async def fetch_otbr_diagnostics(
    base_url: str,
    rloc16: int,
    *,
    tlv_types: tuple[int, ...] = OTBR_DEFAULT_DIAG_TLVS,
    timeout: float = 10.0,
) -> dict[str, Any] | None:
    """POST ``{base_url}/diagnostics`` and return the decoded TLV dict.

    The OTBR REST endpoint wraps ``MGMT_DIAG_GET``: the BR sends a CoAP
    diagnostic request to ``rloc16`` and returns the response TLVs as a
    JSON object. Returns ``None`` on any failure (unreachable BR, target
    didn't respond, malformed payload) — the caller treats this as a
    soft miss and skips this tick.

    We don't validate the TLV content here; downstream code in
    ``otbr_diagnostics.py`` extracts the MAC counter + child table
    fields it understands and stashes the rest in ``extra_json``.
    """
    try:
        rloc16_hex = f"0x{rloc16 & 0xFFFF:04x}"
        body = {"destination": rloc16_hex, "types": list(tlv_types)}
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                f"{base_url}/diagnostics",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                json=body,
            )
        if resp.status_code >= 400:
            log.debug(
                "otbr_rest: /diagnostics %s -> %d", rloc16_hex, resp.status_code
            )
            return None
        payload = resp.json()
        if isinstance(payload, dict):
            return payload
        return None
    except Exception as exc:  # noqa: BLE001
        log.debug("otbr_rest: /diagnostics fetch failed: %s", exc)
        return None


def _otbr_field(entry: dict[str, Any], *keys: str) -> Any:
    """Return the first non-None value among ``keys`` (case variants)."""
    for k in keys:
        v = entry.get(k)
        if v is not None:
            return v
    return None


def _otbr_eui_from(entry: dict[str, Any]) -> str | None:
    raw = _otbr_field(entry, "ExtAddress", "extAddress", "ext_address")
    if raw is None:
        return None
    try:
        eui = _normalize_eui(str(raw))
        if len(eui) == 16 and all(c in "0123456789abcdef" for c in eui):
            return eui
    except Exception:  # noqa: BLE001
        return None
    return None


def _otbr_coerce_int(v: Any) -> int | None:
    return coerce_int(v, allow_strings=True)


def _otbr_tri(v: Any) -> int | None:
    return to_tristate_int(v)


def _decode_otbr_neighbors(raw: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Map OTBR REST ``/node/neighbors`` JSON to ``links`` row dicts.

    Field names per OpenThread REST: ``ExtAddress``, ``Age``, ``Rloc16``,
    ``LinkQualityIn``, ``LinkQualityOut``, ``AverageRssi``, ``LastRssi``,
    ``FrameErrorRate``, ``MessageErrorRate``, ``IsChild``, ``RxOnWhenIdle``,
    ``FullThreadDevice``, ``FullNetworkData``, optionally
    ``LinkFrameCounter``, ``MleFrameCounter``. Unknown fields are ignored.
    """
    if not raw:
        return []
    out: list[dict[str, Any]] = []
    for entry in raw:
        eui = _otbr_eui_from(entry)
        if not eui:
            continue
        out.append({
            "neighbor_eui64": eui,
            "rssi_avg": _otbr_coerce_int(_otbr_field(entry, "AverageRssi", "averageRssi")),
            "rssi_last": _otbr_coerce_int(_otbr_field(entry, "LastRssi", "lastRssi")),
            "lqi_in": _otbr_coerce_int(_otbr_field(entry, "LinkQualityIn", "linkQualityIn")),
            "lqi_out": _otbr_coerce_int(_otbr_field(entry, "LinkQualityOut", "linkQualityOut")),
            "is_child": _otbr_tri(_otbr_field(entry, "IsChild", "isChild")),
            "age_seconds": _otbr_coerce_int(_otbr_field(entry, "Age", "age")),
            "frame_error_rate": _otbr_coerce_int(
                _otbr_field(entry, "FrameErrorRate", "frameErrorRate")
            ),
            "message_error_rate": _otbr_coerce_int(
                _otbr_field(entry, "MessageErrorRate", "messageErrorRate")
            ),
            "link_frame_counter": _otbr_coerce_int(
                _otbr_field(entry, "LinkFrameCounter", "linkFrameCounter")
            ),
            "mle_frame_counter": _otbr_coerce_int(
                _otbr_field(entry, "MleFrameCounter", "mleFrameCounter")
            ),
            "rx_on_when_idle": _otbr_tri(_otbr_field(entry, "RxOnWhenIdle", "rxOnWhenIdle")),
            "full_thread_device": _otbr_tri(
                _otbr_field(entry, "FullThreadDevice", "fullThreadDevice")
            ),
            "full_network_data": _otbr_tri(
                _otbr_field(entry, "FullNetworkData", "fullNetworkData")
            ),
        })
    return out


def _decode_otbr_routers(raw: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Map OTBR REST ``/node/routers`` JSON to ``links`` row dicts (RouteTable).

    Field names per OpenThread REST: ``ExtAddress``, ``Rloc16``, ``RouterId``,
    ``NextHop`` (or ``NextHopRouterId``), ``PathCost``, ``LinkQualityIn``
    / ``LqiIn``, ``LinkQualityOut`` / ``LqiOut``, ``Age``, ``Allocated``,
    ``LinkEstablished``.
    """
    if not raw:
        return []
    out: list[dict[str, Any]] = []
    for entry in raw:
        eui = _otbr_eui_from(entry)
        if not eui:
            continue
        out.append({
            "neighbor_eui64": eui,
            "rssi_avg": None,
            "rssi_last": None,
            "lqi_in": _otbr_coerce_int(
                _otbr_field(entry, "LinkQualityIn", "LqiIn", "lqiIn", "linkQualityIn")
            ),
            "lqi_out": _otbr_coerce_int(
                _otbr_field(entry, "LinkQualityOut", "LqiOut", "lqiOut", "linkQualityOut")
            ),
            "is_child": None,
            "age_seconds": _otbr_coerce_int(_otbr_field(entry, "Age", "age")),
            "frame_error_rate": None,
            "message_error_rate": None,
            "path_cost": _otbr_coerce_int(_otbr_field(entry, "PathCost", "pathCost")),
            "router_id": _otbr_coerce_int(_otbr_field(entry, "RouterId", "routerId")),
            "next_hop_router_id": _otbr_coerce_int(
                _otbr_field(entry, "NextHopRouterId", "NextHop", "nextHop", "nextHopRouterId")
            ),
            "allocated": _otbr_tri(_otbr_field(entry, "Allocated", "allocated")),
            "link_established": _otbr_tri(
                _otbr_field(entry, "LinkEstablished", "linkEstablished")
            ),
        })
    return out


def _dataset_to_network_data(
    dataset: dict[str, Any] | None,
    network: dict[str, Any] | None,
    node: dict[str, Any],
) -> dict[str, Any]:
    """Merge active dataset + network data + /node fallbacks into one dict.

    Returns the kwargs to feed ``store.upsert_network_data``. Only the keys
    with non-None values are returned. Callers must always supply
    ``partition_id`` separately; this helper only fills the descriptive
    fields.
    """
    def _from(src: dict[str, Any] | None, *keys: str) -> Any:
        if not src:
            return None
        for k in keys:
            v = src.get(k)
            if v is not None:
                return v
        return None

    ds = dataset or {}
    nd = network or {}

    out: dict[str, Any] = {}
    pan = _from(ds, "PanId", "panId", "pan_id") or _from(node, "PanId", "panId")
    if pan is not None:
        out["pan_id"] = str(pan)
    ext_pan = _from(ds, "ExtPanId", "extPanId", "extended_pan_id") or _from(node, "ExtPanId")
    if ext_pan is not None:
        out["extended_pan_id"] = str(ext_pan)
    name = _from(ds, "NetworkName", "networkName", "network_name") or _from(node, "NetworkName")
    if name is not None:
        out["network_name"] = str(name)
    channel = _from(ds, "Channel", "channel") or _from(node, "Channel", "channel")
    if channel is not None:
        try:
            out["channel"] = int(channel)
        except (TypeError, ValueError):
            pass
    chan_mask = _from(ds, "ChannelMask", "channelMask", "channel_mask")
    if chan_mask is not None:
        out["channel_mask"] = str(chan_mask)
    mlp = _from(ds, "MeshLocalPrefix", "meshLocalPrefix", "mesh_local_prefix")
    if mlp is not None:
        out["mesh_local_prefix"] = str(mlp)
    ts = _from(ds, "ActiveTimestamp", "activeTimestamp", "active_timestamp")
    if ts is not None:
        out["active_timestamp"] = str(ts)
    # Network Data structure (varies by OTBR build). Pass through whatever
    # we got under the standard names; consumers parse the JSON.
    for src_key, dst_key in (
        ("OnMeshPrefixes", "on_mesh_prefixes"),
        ("on_mesh_prefixes", "on_mesh_prefixes"),
        ("ExternalRoutes", "external_routes"),
        ("external_routes", "external_routes"),
        ("Services", "services"),
        ("services", "services"),
        ("BrServers", "br_servers"),
        ("br_servers", "br_servers"),
    ):
        if dst_key in out:
            continue
        v = nd.get(src_key) if isinstance(nd, dict) else None
        if v is not None:
            out[dst_key] = v
    return out


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
    rloc16 = _extract_rloc16(node)
    router_id = _router_id_from_rloc16(rloc16)

    # Only set friendly_name on first insert — never overwrite a user rename.
    existing = s.get_node(eui64)
    desired_friendly = None if (existing and existing.get("friendly_name")) else _OTBR_FRIENDLY_NAME

    s.upsert_node_metadata(
        eui64=eui64,
        friendly_name=desired_friendly,
        role=_OTBR_ROLE,
        is_thread=True,
    )
    s.set_node_diagnostics(
        eui64,
        partition_id=partition_id,
        leader_router_id=leader_router_id,
        routing_role=routing_role,
        active_routers=active_routers,
        weighting=weighting,
    )
    if router_id is not None:
        s.set_node_router_id(eui64, router_id)
    # Mark referenced so phantom-sweep treats it as live.
    s.bump_last_referenced([eui64])

    # v0.9.39: OTBR has no HA entity, so HA-availability scoring would
    # leave it ``available IS NULL``. Stamp it ``True`` here — we just
    # successfully hit ``/node`` and got back data, which is the OTBR
    # equivalent of "reachable". Falls back to ``False`` is unnecessary
    # because if the fetch had failed we'd have returned much earlier.
    try:
        s.apply_availability([(eui64, True, "otbr_rest")])
    except Exception as exc:  # noqa: BLE001
        log.debug("otbr apply_availability failed: %s", exc)

    # v10: persist Thread Network Data (partition-wide identity + routes /
    # services / BR servers). Best-effort: missing dataset endpoints are
    # logged at debug and skipped — OTBR ingest itself must not regress.
    network_data_persisted = False
    if partition_id is not None:
        try:
            dataset = await fetch_otbr_dataset_active(base_url)
            network = await fetch_otbr_network_data(base_url)
            kwargs = _dataset_to_network_data(dataset, network, node)
            s.upsert_network_data(
                partition_id=partition_id,
                otbr_eui64=eui64,
                **kwargs,
            )
            network_data_persisted = True
        except Exception as exc:  # noqa: BLE001
            log.warning("otbr_rest: network data persist failed: %s", exc)

    # v11: persist the OTBR's own NeighborTable + RouteTable so the border
    # router shows up as a first-class reporter in the ``links`` table.
    # Without this, the OTBR's view of which routers it directly reaches
    # is invisible — the mesh graph only has edges from non-OTBR routers
    # pointing *at* the OTBR, with no symmetric back-edge. Best-effort:
    # older OTBR builds may not expose these endpoints.
    otbr_neighbors_persisted: int | None = None
    otbr_routes_persisted: int | None = None
    try:
        neighbors_raw = await fetch_otbr_neighbors(base_url)
        if neighbors_raw is not None:
            neighbor_rows = _decode_otbr_neighbors(neighbors_raw)
            s.replace_links_for_reporter(
                eui64, "neighbor_table", neighbor_rows,
                partition_id=partition_id,
            )
            otbr_neighbors_persisted = len(neighbor_rows)
        routers_raw = await fetch_otbr_routers(base_url)
        if routers_raw is not None:
            route_rows = _decode_otbr_routers(routers_raw)
            s.replace_links_for_reporter(
                eui64, "route_table", route_rows,
                partition_id=partition_id,
            )
            otbr_routes_persisted = len(route_rows)
            # The OTBR's own router_id is the self-entry in its routers list.
            for row in route_rows:
                if row.get("neighbor_eui64") == eui64 and row.get("router_id") is not None:
                    s.set_node_router_id(eui64, int(row["router_id"]))
                    break
    except Exception as exc:  # noqa: BLE001
        log.warning("otbr_rest: OTBR neighbor/router ingest failed: %s", exc)

    log.info(
        "otbr_rest: ingested OTBR eui=%s state=%s partition=%s router_id=%s "
        "leader_router=%s active_routers=%s network_data=%s neighbors=%s routes=%s",
        eui64, state, partition_id, router_id, leader_router_id, active_routers,
        "ok" if network_data_persisted else "skipped",
        otbr_neighbors_persisted if otbr_neighbors_persisted is not None else "skipped",
        otbr_routes_persisted if otbr_routes_persisted is not None else "skipped",
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
        "router_id": router_id,
        "rloc16": rloc16,
        "network_data_persisted": network_data_persisted,
        "otbr_neighbors_persisted": otbr_neighbors_persisted,
        "otbr_routes_persisted": otbr_routes_persisted,
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
