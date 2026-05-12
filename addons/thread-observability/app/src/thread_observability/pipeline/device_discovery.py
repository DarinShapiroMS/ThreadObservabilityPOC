"""Discover Thread device names from Home Assistant's device registry.

Home Assistant maintains a device registry with IEEE addresses for Thread,
Zigbee, and other radio devices. This module fetches that registry and
correlates IEEE addresses with our extracted EUI64 nodes to populate
friendly names and device IDs automatically.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

from ..storage.sqlite_store import SQLiteStore, get_store

log = logging.getLogger(__name__)

# HA config directory - typically /config in the addon environment
HA_CONFIG_DIR = Path(os.getenv("HA_CONFIG_DIR", "/config"))
DEVICE_REGISTRY_PATH = HA_CONFIG_DIR / ".storage" / "core.device_registry"

# Matter server WebSocket endpoint. We query it to bridge Matter node_id
# (present in HA device registry as an identifier) to the Thread EUI64
# we extract from OTBR. Inside the HA stack, the matter_server addon is
# reachable by hostname; allow override for tests / non-default deployments.
MATTER_WS_URL = os.getenv(
    "MATTER_WS_URL",
    "ws://core-matter-server:5580/ws",
)
MATTER_WS_TIMEOUT = float(os.getenv("MATTER_WS_TIMEOUT", "5.0"))

# Matter General Diagnostics cluster id (0x0033 = 51), NetworkInterfaces
# attribute (0x0000 = 0). python-matter-server keys attribute values as
# "<endpoint>/<cluster>/<attribute>" strings.
_MATTER_GENERAL_DIAG_NETIF_KEY = "0/51/0"
# Matter Thread Network Diagnostics cluster id (0x0035 = 53). Attribute IDs:
#   0  Channel
#   1  RoutingRole (enum)
#   7  NeighborTable (list of struct)
#   8  RouteTable   (list of struct)
#   9  PartitionId
#   10 Weighting
#   13 LeaderRouterId
#   15 ExtAddress (8-byte Thread EUI64)
_MATTER_THREAD_DIAG_EXTADDR_SUFFIX = "/53/15"

# Matter RoutingRole enum (Matter 1.x Thread Network Diagnostics cluster).
_ROUTING_ROLE_NAMES: dict[int, str] = {
    0: "unspecified",
    1: "unassigned",
    2: "sleepy_end_device",
    3: "end_device",
    4: "reed",
    5: "router",
    6: "leader",
}

# Module-level cache populated by `_load_matter_node_bridge_async`. Holds the
# most recent rich per-node info (EUI64 + diagnostics + neighbor/route tables)
# so `discover_and_sync` can persist them without a second WS roundtrip.
# Shape: {canonical_node_id: {"eui64": str|None, "diagnostics": {...},
#         "neighbor_table": [...], "route_table": [...] } }
_LAST_MATTER_RICH_INFO: dict[str, dict[str, Any]] = {}

# Thread-only connection types (we intentionally do NOT include zigbee here).
_THREAD_CONN_TYPES = ("thread", "ieee802154")


def _normalize_ieee(ieee_str: str) -> str:
    """Normalize IEEE address to 16-char lowercase hex (EUI64 format).

    Handles formats like:
    - c6:b7:7f:58:e5:ac:ee:d4 → c6b77f58e5aceed4
    - c6b77f58e5aceed4 → c6b77f58e5aceed4
    - 0xc6b77f58e5aceed4 → c6b77f58e5aceed4
    """
    # Strip hex prefix if present
    if ieee_str.startswith("0x"):
        ieee_str = ieee_str[2:]
    # Remove colons/dashes
    ieee_str = ieee_str.replace(":", "").replace("-", "")
    return ieee_str.lower().zfill(16)[-16:]


def _canonical_matter_node_id(raw: Any) -> str | None:
    """Normalize a Matter node id to a canonical decimal string.

    HA's device registry stores Matter node ids as 16-char zero-padded
    hex strings (e.g. ``"0000000000000001"``). python-matter-server returns
    them as decimal integers (e.g. ``1``). Reduce both to ``str(int)`` so
    they compare equal as dict keys.
    """
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return str(raw)
    s = str(raw).strip()
    if not s:
        return None
    if s.lower().startswith("0x"):
        s = s[2:]
    # Try hex first (HA's registry format). If that fails, try decimal.
    try:
        return str(int(s, 16))
    except ValueError:
        pass
    try:
        return str(int(s, 10))
    except ValueError:
        return None


def _extract_matter_node_id(value: str) -> str | None:
    """Extract a Matter node id from a device-registry identifier value.

    HA Matter devices expose identifiers like:
      ["matter", "<node_id_hex16>"]   (most common — 16-char zero-padded hex)
      ["matter", "<fabric_id>-<node_id>"]
      ["matter", "<fabric_id>-<node_id>-<endpoint_id>"]
    Returns a canonical decimal-string node_id, or None.
    """
    if not value:
        return None
    parts = value.split("-")
    # Single segment: usually the hex node id directly.
    if len(parts) == 1:
        return _canonical_matter_node_id(parts[0])
    # Multi-segment: the node id is after the first hyphen.
    return _canonical_matter_node_id(parts[1])


def _load_matter_node_bridge() -> dict[str, str]:
    """Synchronous shim over the async WebSocket bridge.

    Used from sync test paths; in the live async pipeline we call
    ``_load_matter_node_bridge_async`` directly to avoid nested loops.
    """
    try:
        return asyncio.run(_load_matter_node_bridge_async())
    except RuntimeError:
        # Already inside a running loop; caller should use the async variant.
        return {}


def _hardware_address_to_eui64(raw: Any) -> str | None:
    """Convert a Matter ``HardwareAddress`` octet-string to a 16-hex EUI64.

    python-matter-server typically delivers octet strings as base64 strings or
    as a list of byte integers. We accept both, plus already-hex strings, and
    return ``None`` for anything that does not look like a 64-bit MAC.
    """
    import base64
    import binascii

    if raw is None:
        return None
    try:
        if isinstance(raw, (bytes, bytearray)):
            data = bytes(raw)
        elif isinstance(raw, list):
            data = bytes(int(b) & 0xFF for b in raw)
        elif isinstance(raw, str):
            stripped = raw.replace(":", "").replace("-", "").strip()
            if stripped.lower().startswith("0x"):
                stripped = stripped[2:]
            if (
                len(stripped) in (12, 16)
                and all(c in "0123456789abcdefABCDEF" for c in stripped)
            ):
                data = bytes.fromhex(stripped)
            else:
                try:
                    data = base64.b64decode(raw, validate=True)
                except (binascii.Error, ValueError):
                    return None
        else:
            return None
    except Exception:  # noqa: BLE001
        return None
    if len(data) == 8:
        return data.hex().lower()
    if len(data) == 6:
        # 48-bit MAC; not an EUI64 but caller may still want to record it.
        return None
    return None


def _ext_address_to_eui64(raw: Any) -> str | None:
    """Decode a NeighborTable / RouteTable ExtAddress field to 16-hex EUI64.

    Matter spec types ExtAddress as uint64. matter-server may deliver it as
    int, hex string, base64 octet string, or byte list. Returns None for
    anything that does not yield 8 bytes.
    """
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        if raw < 0 or raw > 0xFFFFFFFFFFFFFFFF:
            return None
        return f"{raw:016x}"
    return _hardware_address_to_eui64(raw)


def _field(struct: dict[str, Any], int_key: int, *str_keys: str) -> Any:
    """Defensively read a struct field by Matter integer id or named alias."""
    if not isinstance(struct, dict):
        return None
    val = struct.get(str(int_key))
    if val is not None:
        return val
    for k in str_keys:
        v = struct.get(k)
        if v is not None:
            return v
    return None


def _coerce_int(v: Any) -> int | None:
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    return None


def _decode_neighbor_table(raw: Any) -> list[dict[str, Any]]:
    """Decode a Matter NeighborTable attribute (cluster 53 attr 7).

    NeighborTableStruct fields per Matter spec:
      0 ExtAddress, 1 Age, 2 Rloc16, 5 LQI, 6 AverageRssi, 7 LastRssi,
      8 FrameErrorRate, 9 MessageErrorRate, 13 IsChild.
    """
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        eui = _ext_address_to_eui64(_field(entry, 0, "extAddress", "ExtAddress"))
        if not eui:
            continue
        is_child_raw = _field(entry, 13, "isChild", "IsChild")
        out.append({
            "neighbor_eui64": eui,
            "rssi_avg": _coerce_int(_field(entry, 6, "averageRssi", "AverageRssi")),
            "rssi_last": _coerce_int(_field(entry, 7, "lastRssi", "LastRssi")),
            "lqi_in": _coerce_int(_field(entry, 5, "lqi", "LQI")),
            "lqi_out": None,
            "is_child": 1 if is_child_raw else (0 if is_child_raw is not None else None),
            "age_seconds": _coerce_int(_field(entry, 1, "age", "Age")),
            "frame_error_rate": _coerce_int(_field(entry, 8, "frameErrorRate", "FrameErrorRate")),
            "message_error_rate": _coerce_int(_field(entry, 9, "messageErrorRate", "MessageErrorRate")),
            "path_cost": None,
        })
    return out


def _decode_route_table(raw: Any) -> list[dict[str, Any]]:
    """Decode a Matter RouteTable attribute (cluster 53 attr 8).

    RouteTableStruct fields per Matter spec:
      0 ExtAddress, 1 Rloc16, 2 RouterId, 3 NextHop, 4 PathCost,
      5 LQIIn, 6 LQIOut, 7 Age, 8 Allocated, 9 LinkEstablished.
    Entries with LinkEstablished=False are skipped (not direct neighbors).
    """
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        eui = _ext_address_to_eui64(_field(entry, 0, "extAddress", "ExtAddress"))
        if not eui:
            continue
        link_est = _field(entry, 9, "linkEstablished", "LinkEstablished")
        if link_est is False:
            continue
        out.append({
            "neighbor_eui64": eui,
            "rssi_avg": None,
            "rssi_last": None,
            "lqi_in": _coerce_int(_field(entry, 5, "lqiIn", "LQIIn")),
            "lqi_out": _coerce_int(_field(entry, 6, "lqiOut", "LQIOut")),
            "is_child": None,
            "age_seconds": _coerce_int(_field(entry, 7, "age", "Age")),
            "frame_error_rate": None,
            "message_error_rate": None,
            "path_cost": _coerce_int(_field(entry, 4, "pathCost", "PathCost")),
        })
    return out


def _extract_thread_diagnostics(attrs: dict[str, Any]) -> dict[str, Any]:
    """Pull cluster-53 Thread scalars from a matter-server node's attributes.

    Only considers endpoint 0 (root) — Thread diagnostics live there.
    """
    def _get_int(suffix: str) -> int | None:
        val = attrs.get(f"0/53/{suffix}")
        if isinstance(val, bool):
            return int(val)
        if isinstance(val, int):
            return val
        return None

    role_int = _get_int("1")
    return {
        "channel": _get_int("0"),
        "routing_role_int": role_int,
        "routing_role": _ROUTING_ROLE_NAMES.get(role_int) if role_int is not None else None,
        "partition_id": _get_int("9"),
        "weighting": _get_int("10"),
        "leader_router_id": _get_int("13"),
    }


async def _load_matter_node_bridge_async() -> dict[str, str]:
    """Build a Matter ``node_id`` -> Thread EUI64 mapping via matter-server WS.

    Connects to the matter_server addon's WebSocket API and issues a
    ``get_nodes`` command. For each returned node, we look at the General
    Diagnostics cluster's ``NetworkInterfaces`` attribute and extract the
    Thread interface's ``HardwareAddress`` (8-byte EUI64).

    Any failure (matter_server not installed, WS unreachable, schema drift)
    returns an empty mapping so discovery degrades gracefully.
    """
    try:
        import websockets  # type: ignore[import-not-found]
    except ImportError:
        log.info("Matter bridge: websockets package not installed")
        return {}

    bridge: dict[str, str] = {}
    try:
        async with asyncio.timeout(MATTER_WS_TIMEOUT):
            async with websockets.connect(MATTER_WS_URL) as ws:
                # Server sends a ServerInfoMessage on connect; drain it.
                try:
                    info_raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    log.info(
                        "Matter bridge: connected to %s, server_info=%s",
                        MATTER_WS_URL, str(info_raw)[:200],
                    )
                except asyncio.TimeoutError:
                    log.info("Matter bridge: connected to %s (no server_info)", MATTER_WS_URL)
                req = json.dumps({
                    "message_id": "thread-obs-get-nodes",
                    "command": "get_nodes",
                })
                await ws.send(req)
                # Loop until we get the response with our message_id (skip events).
                payload = None
                for _ in range(10):
                    raw = await ws.recv()
                    candidate = json.loads(raw)
                    if (
                        isinstance(candidate, dict)
                        and candidate.get("message_id") == "thread-obs-get-nodes"
                    ):
                        payload = candidate
                        break
                if payload is None:
                    log.info("Matter bridge: no matching response for get_nodes")
                    return {}
    except Exception as exc:  # noqa: BLE001
        log.info("Matter bridge: WS unavailable (%s): %s", MATTER_WS_URL, exc)
        return {}

    if "error_code" in payload:
        log.info("Matter bridge: get_nodes returned error: %s", payload.get("error_code"))
        return {}

    nodes = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(nodes, list):
        log.info(
            "Matter bridge: get_nodes returned unexpected shape: keys=%s",
            list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__,
        )
        return {}

    log.info("Matter bridge: get_nodes returned %d nodes", len(nodes))
    # Log a sample node's structure so we can see the actual schema.
    if nodes:
        sample = nodes[0] if isinstance(nodes[0], dict) else {}
        sample_attrs = sample.get("attributes") or {}
        all_keys = list(sample_attrs.keys()) if isinstance(sample_attrs, dict) else []
        diag_keys = [k for k in all_keys if "/51/" in k or "/53/" in k]
        log.info(
            "Matter bridge: sample node_id=%s top_keys=%s total_attrs=%d diag_keys=%s",
            sample.get("node_id"),
            list(sample.keys())[:15],
            len(all_keys),
            diag_keys[:20],
        )

    dumped_sample = False
    rich_cache: dict[str, dict[str, Any]] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = node.get("node_id")
        if node_id is None:
            continue
        attrs = node.get("attributes") or {}
        if not isinstance(attrs, dict):
            continue

        eui: str | None = None

        # Preferred path: Thread Network Diagnostics ExtAddress (any endpoint).
        for key, value in attrs.items():
            if key.endswith(_MATTER_THREAD_DIAG_EXTADDR_SUFFIX):
                eui = _hardware_address_to_eui64(value)
                if eui:
                    break

        # Fallback path: General Diagnostics NetworkInterfaces -> Thread iface HW addr.
        # python-matter-server represents struct fields by their Matter
        # attribute IDs as string keys:
        #   "0"=Name, "1"=IsOperational, "4"=HardwareAddress (octet string,
        #   base64-encoded), "7"=Type (4 == Thread).
        if not eui:
            for key, value in attrs.items():
                if not key.endswith("/51/0"):
                    continue
                if not isinstance(value, list):
                    continue
                if not dumped_sample:
                    log.info(
                        "Matter bridge: NetworkInterfaces sample for node_id=%s: %s",
                        node_id, json.dumps(value, default=str)[:600],
                    )
                    dumped_sample = True
                for iface in value:
                    if not isinstance(iface, dict):
                        continue
                    iface_type = iface.get("7", iface.get("Type"))
                    iface_name = iface.get("0", iface.get("Name", ""))
                    # Accept Thread by interface type (4) or name hint.
                    is_thread = (
                        iface_type == 4
                        or (isinstance(iface_name, str) and (
                            "thread" in iface_name.lower()
                            or "ieee802154" in iface_name.lower()
                        ))
                    )
                    if not is_thread:
                        continue
                    hw = (
                        iface.get("4")
                        or iface.get("HardwareAddress")
                        or iface.get("hardwareAddress")
                        or iface.get("hardware_address")
                    )
                    eui = _hardware_address_to_eui64(hw)
                    if eui:
                        break
                if eui:
                    break

        if eui:
            canon = _canonical_matter_node_id(node_id)
            if canon:
                bridge[canon] = eui

        # Always try to extract Thread diagnostics + neighbor/route tables,
        # even if we couldn't resolve EUI here (cache keyed by canonical
        # node_id so downstream can still cross-reference).
        canon_for_rich = _canonical_matter_node_id(node_id)
        if canon_for_rich:
            diagnostics = _extract_thread_diagnostics(attrs)
            neighbor_table = _decode_neighbor_table(attrs.get("0/53/7"))
            route_table = _decode_route_table(attrs.get("0/53/8"))
            if eui or diagnostics["partition_id"] is not None or neighbor_table or route_table:
                rich_cache[canon_for_rich] = {
                    "eui64": eui,
                    "diagnostics": diagnostics,
                    "neighbor_table": neighbor_table,
                    "route_table": route_table,
                }

    # Publish the rich cache so `discover_and_sync` can persist diagnostics.
    global _LAST_MATTER_RICH_INFO
    _LAST_MATTER_RICH_INFO = rich_cache
    log.info(
        "Matter bridge: extracted %d EUI64 mappings from %d nodes "
        "(rich_info entries=%d, with_neighbor_table=%d, with_route_table=%d)",
        len(bridge), len(nodes), len(rich_cache),
        sum(1 for v in rich_cache.values() if v["neighbor_table"]),
        sum(1 for v in rich_cache.values() if v["route_table"]),
    )
    return bridge


def _eui64_from_ipv6(addr: str) -> str | None:
    """Derive a 16-hex EUI64 from a Thread mesh IPv6 address if possible."""
    if not addr or ":" not in addr:
        return None
    parts = addr.split(":")
    if len(parts) < 4:
        return None
    last4 = parts[-4:]
    if not all(0 < len(p) <= 4 and all(c in "0123456789abcdefABCDEF" for c in p) for p in last4):
        return None
    try:
        return _normalize_ieee("".join(p.zfill(4) for p in last4))
    except Exception:  # noqa: BLE001
        return None


async def fetch_device_registry() -> list[dict[str, Any]]:
    """Fetch Thread device/node info from OTBR REST API + HA device registry.
    
    The OTBR addon exposes a /api/topology endpoint that returns information
    about all Thread nodes in the network, including their extended addresses (EUI64).
    The HA device registry provides friendly names and device IDs for those nodes.
    
    This function fetches both sources and merges them:
    - OTBR topology: authoritative node list with role and rloc info
    - HA device registry: friendly names and device metadata
    
    Returns a merged list of dicts combining both sources.
    """
    import httpx
    
    # Try OTBR API first for node topology
    otbr_nodes: dict[str, dict[str, Any]] = {}
    otbr_endpoints = [
        "http://supervisor:9203/addon/core_openthread_border_router/api/topology",  # Via Supervisor
        "http://otbr:8080/api/topology",  # Direct if accessible
    ]
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            for endpoint in otbr_endpoints:
                try:
                    resp = await client.get(
                        endpoint,
                        headers={"Accept": "application/json"},
                    )
                    log.info(
                        "discover: OTBR endpoint %s -> HTTP %s",
                        endpoint, resp.status_code,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        log.debug(
                            "Thread topology fetched from %s",
                            endpoint,
                        )
                        # Convert OTBR topology response to dict keyed by EUI64
                        if isinstance(data, dict):
                            topology = data.get("topology", {})
                            nodes = topology.get("nodes", [])
                            for node in nodes:
                                ext_addr = node.get("extendedAddress")
                                if ext_addr:
                                    try:
                                        eui = _normalize_ieee(str(ext_addr))
                                        otbr_nodes[eui] = {
                                            "extendedAddress": ext_addr,
                                            "rloc": node.get("rloc"),
                                            "role": node.get("role"),
                                        }
                                    except Exception as exc:
                                        log.debug("Failed to parse OTBR node %s: %s", ext_addr, exc)
                            if otbr_nodes:
                                log.debug("Discovered %d Thread nodes from OTBR topology", len(otbr_nodes))
                                break
                except Exception as exc:
                    log.info("discover: OTBR endpoint %s failed: %s", endpoint, exc)
                    continue
    except Exception as exc:
        log.warning("Failed to fetch OTBR topology: %s", exc)
    log.info("discover: otbr_nodes=%d", len(otbr_nodes))
    
    # Now fetch device registry to get friendly names and metadata.
    # Thread-only: we no longer match zigbee connections.
    reg_devices = _fallback_device_registry()
    registry_by_eui: dict[str, dict[str, Any]] = {}
    registry_by_matter_node: dict[str, dict[str, Any]] = {}
    for dev in reg_devices:
        dev_meta = {
            "device_id": dev.get("id"),
            "name": dev.get("name"),
            "name_by_user": dev.get("name_by_user"),
            "manufacturer": dev.get("manufacturer"),
            "model": dev.get("model"),
            "area_id": dev.get("area_id"),
            "primary_config_entry": dev.get("primary_config_entry"),
        }
        # Primary path: direct Thread connection on the device.
        connections = dev.get("connections", [])
        matched_thread_conn = False
        for conn_type, conn_id in connections:
            if conn_type in _THREAD_CONN_TYPES:
                try:
                    eui = _normalize_ieee(str(conn_id))
                    registry_by_eui[eui] = dict(dev_meta)
                    matched_thread_conn = True
                    break  # Use first Thread connection found
                except Exception as exc:
                    log.debug("Failed to parse connection %s: %s", conn_id, exc)
        # Secondary path: Matter identifier on the device (we bridge to EUI64 later).
        if not matched_thread_conn:
            for ident in dev.get("identifiers", []) or []:
                # identifiers entries look like ["matter", "<fabric_id>-<node_id>-<endpoint_id>"]
                try:
                    domain, value = ident[0], ident[1]
                except (IndexError, TypeError):
                    continue
                if domain != "matter" or not value:
                    continue
                node_id = _extract_matter_node_id(str(value))
                if node_id is None:
                    continue
                registry_by_matter_node[node_id] = dict(dev_meta)
                log.debug(
                    "Found Matter-only registry device: node_id=%s name=%s",
                    node_id, dev.get("name_by_user") or dev.get("name"),
                )
    if registry_by_matter_node:
        # Bridge Matter node_id -> EUI64 via matter-server WebSocket API.
        log.info(
            "discover: %d Matter-only registry devices; querying matter-server WS",
            len(registry_by_matter_node),
        )
        bridge = await _load_matter_node_bridge_async()
        log.info("discover: matter bridge returned %d entries", len(bridge))
        # Diagnostic: log the two key sets so we can see ID format mismatches.
        reg_keys = sorted(registry_by_matter_node.keys())[:10]
        bridge_keys = sorted(bridge.keys())[:10]
        log.info(
            "discover: registry_node_id_sample=%s bridge_node_id_sample=%s",
            reg_keys, bridge_keys,
        )
        merged_count = 0
        for node_id, meta in registry_by_matter_node.items():
            eui = bridge.get(node_id)
            if eui:
                registry_by_eui.setdefault(eui, meta)
                merged_count += 1
        log.info(
            "discover: matter bridge merged %d registry devices into EUI64 map",
            merged_count,
        )
    
    if registry_by_eui:
        log.info(
            "discover: registry contributed %d EUI64-keyed devices (thread+matter-bridged)",
            len(registry_by_eui),
        )
    else:
        log.info(
            "discover: registry contributed 0 devices (registry_devices=%d, matter_only=%d)",
            len(reg_devices), len(registry_by_matter_node),
        )
    
    # Merge: OTBR nodes are the primary source, supplemented with registry data
    merged: dict[str, dict[str, Any]] = {}

    # Add OTBR nodes with any matching registry data
    for eui, otbr_data in otbr_nodes.items():
        merged[eui] = {**otbr_data, "extendedAddress": eui}
        if eui in registry_by_eui:
            merged[eui].update(registry_by_eui[eui])

    # Add registry-only devices (not discovered from OTBR). Stamp the EUI64
    # onto each value as ``extendedAddress`` so ``_extract_thread_devices``
    # can key on it.
    for eui, reg_data in registry_by_eui.items():
        if eui not in merged:
            merged[eui] = {**reg_data, "extendedAddress": eui}

    # Convert to list format for downstream processing
    return list(merged.values())


def _fallback_device_registry() -> list[dict[str, Any]]:
    """Fallback: read device registry from .storage JSON file.
    
    If OTBR API is unavailable, read directly from HA's device registry file.
    """
    try:
        if not DEVICE_REGISTRY_PATH.exists():
            log.warning(
                "Device registry file not found at %s; ensure HA config dir is mounted",
                DEVICE_REGISTRY_PATH,
            )
            return []
        
        with open(DEVICE_REGISTRY_PATH, "r") as f:
            data = json.load(f)
        
        # The file structure is {"version": 1, "key": "...", "data": {"devices": [...]}}
        devices = data.get("data", {}).get("devices", [])
        log.debug(
            "Device registry loaded from %s: %d devices",
            DEVICE_REGISTRY_PATH,
            len(devices),
        )
        return devices
    except FileNotFoundError:
        log.warning("Device registry file not found at %s", DEVICE_REGISTRY_PATH)
        return []
    except json.JSONDecodeError as exc:
        log.warning("Failed to parse device registry JSON: %s", exc)
        return []
    except Exception as exc:
        log.warning("Failed to fetch device registry fallback: %s", exc)
        return []


def _extract_thread_devices(devices: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Extract Thread devices from OTBR topology or device registry.

    Returns a dict mapping EUI64 → {role, rloc, ...}
    
    Handles two formats:
    1. OTBR topology nodes: {"extendedAddress": "...", "rloc": ..., "role": ...}
    2. Device registry devices: {"connections": [["thread", "..."], ...], ...}
    """
    out: dict[str, dict[str, Any]] = {}
    
    for dev in devices:
        # Check if this is an OTBR topology node (has extendedAddress)
        if "extendedAddress" in dev:
            ext_addr = dev.get("extendedAddress")
            if ext_addr:
                try:
                    eui = _normalize_ieee(str(ext_addr))
                    # Preserve registry metadata if it's already stamped on the
                    # dict (matter-bridged devices and merged OTBR+registry).
                    out[eui] = {
                        "role": dev.get("role"),
                        "rloc": dev.get("rloc"),
                        "device_id": dev.get("device_id"),
                        "name": dev.get("name"),
                        "name_by_user": dev.get("name_by_user"),
                        "manufacturer": dev.get("manufacturer"),
                        "model": dev.get("model"),
                        "area_id": dev.get("area_id"),
                        "primary_config_entry": dev.get("primary_config_entry"),
                    }
                    log.debug(
                        "Found Thread node: eui=%s name=%s role=%s",
                        eui,
                        dev.get("name_by_user") or dev.get("name"),
                        dev.get("role"),
                    )
                except Exception as exc:
                    log.debug("Failed to parse OTBR node %s: %s", ext_addr, exc)
        
        # Otherwise, check if this is a device registry device (has connections)
        connections = dev.get("connections", [])
        for conn_type, conn_id in connections:
            # Thread-only: do not match zigbee.
            if conn_type in _THREAD_CONN_TYPES:
                try:
                    eui = _normalize_ieee(str(conn_id))
                    out[eui] = {
                        "device_id": dev.get("id"),
                        "name": dev.get("name"),
                        "name_by_user": dev.get("name_by_user"),
                        "manufacturer": dev.get("manufacturer"),
                        "model": dev.get("model"),
                        "area_id": dev.get("area_id"),
                        "primary_config_entry": dev.get("primary_config_entry"),
                    }
                    log.debug(
                        "Found Thread device from registry: eui=%s name=%s",
                        eui,
                        dev.get("name_by_user") or dev.get("name"),
                    )
                except Exception as exc:
                    log.debug("Failed to parse connection %s: %s", conn_id, exc)
    
    return out


async def discover_and_sync(store: SQLiteStore | None = None) -> dict[str, Any]:
    """Fetch device registry and sync metadata to nodes.

    Returns a summary of matches found and updated.
    """
    s = store or get_store()
    try:
        devices = await fetch_device_registry()
    except Exception as exc:
        log.exception("device discovery failed: %s", exc)
        return {"error": str(exc), "matched": 0, "updated": 0}

    thread_devs = _extract_thread_devices(devices)
    if not thread_devs:
        log.info("No Thread devices found in device registry")
        return {"matched": 0, "updated": 0, "devices": {}}

    # Correlate with our nodes, and also insert any registry/bridge devices
    # that don't yet have a row (so Matter-commissioned Thread devices appear
    # in the nodes list even before OTBR logs mention them).
    nodes = s.list_nodes()
    existing_euis = {n.get("eui64") for n in nodes if n.get("eui64")}
    updated = 0
    inserted = 0
    matches: dict[str, dict[str, Any]] = {}

    for eui, dev in thread_devs.items():
        friendly_name = dev.get("name_by_user") or dev.get("name")
        device_id = dev.get("device_id")
        if not friendly_name and not device_id:
            continue
        matches[eui] = {
            "friendly_name": friendly_name,
            "device_id": device_id,
            "manufacturer": dev.get("manufacturer"),
            "model": dev.get("model"),
        }
        try:
            s.upsert_node_metadata(
                eui64=eui,
                friendly_name=friendly_name,
                device_id=device_id,
            )
            if eui in existing_euis:
                updated += 1
                log.info("Updated node %s with device name '%s'", eui, friendly_name)
            else:
                inserted += 1
                log.info("Inserted node %s with device name '%s'", eui, friendly_name)
        except Exception as exc:
            log.warning("Failed to upsert node %s: %s", eui, exc)

    log.info(
        "device discovery: scanned %d devices, found %d matches, updated %d, inserted %d",
        len(devices), len(matches), updated, inserted,
    )

    # Persist Thread diagnostics + neighbor/route tables harvested from
    # matter-server (cluster 53). Also detect partition splits.
    diag_summary = _persist_matter_diagnostics(s, nodes)

    return {
        "devices_scanned": len(devices),
        "matched": len(matches),
        "updated": updated,
        "inserted": inserted,
        "matches": matches,
        "diagnostics": diag_summary,
    }


def _persist_matter_diagnostics(
    s: SQLiteStore,
    prior_nodes: list[dict[str, Any]],
) -> dict[str, Any]:
    """Persist cached Thread diagnostics (cluster 53) to the store.

    Uses `_LAST_MATTER_RICH_INFO` populated by the most recent bridge call.
    Returns a summary dict suitable for the discover_and_sync response.
    """
    rich = _LAST_MATTER_RICH_INFO
    if not rich:
        return {"nodes_with_diagnostics": 0, "links_recorded": 0, "partition_split": False}

    prior_by_eui = {n.get("eui64"): n for n in prior_nodes if n.get("eui64")}

    links_recorded = 0
    diag_nodes = 0
    partitions: dict[int, list[str]] = {}
    partition_change_events = 0
    leaders_by_partition: dict[int, str] = {}

    for _node_id, info in rich.items():
        eui = info.get("eui64")
        if not eui:
            continue
        diag = info.get("diagnostics") or {}
        neighbor_table = info.get("neighbor_table") or []
        route_table = info.get("route_table") or []

        # Persist links (replace per source). End devices typically have
        # neither table populated; we still issue replace calls so stale
        # rows from prior cycles get cleared.
        try:
            s.replace_links_for_reporter(eui, "neighbor_table", neighbor_table)
            s.replace_links_for_reporter(eui, "route_table", route_table)
            links_recorded += len(neighbor_table) + len(route_table)
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to persist links for %s: %s", eui, exc)

        # Persist scalars.
        try:
            updated_diag = s.set_node_diagnostics(
                eui,
                partition_id=diag.get("partition_id"),
                leader_router_id=diag.get("leader_router_id"),
                routing_role=diag.get("routing_role"),
                active_routers=len(route_table) or None,
                channel=diag.get("channel"),
                weighting=diag.get("weighting"),
            )
            if updated_diag:
                diag_nodes += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to persist diagnostics for %s: %s", eui, exc)

        # Partition tracking + change detection.
        pid = diag.get("partition_id")
        if isinstance(pid, int):
            partitions.setdefault(pid, []).append(eui)
            role = diag.get("routing_role")
            if role == "leader":
                leaders_by_partition.setdefault(pid, eui)
            prior = prior_by_eui.get(eui) or {}
            prior_pid = prior.get("partition_id")
            if prior_pid is not None and prior_pid != pid:
                try:
                    s.insert_event(
                        eui64=eui,
                        type="partition_change",
                        payload={"from": prior_pid, "to": pid},
                    )
                    partition_change_events += 1
                except Exception as exc:  # noqa: BLE001
                    log.warning("Failed to insert partition_change event for %s: %s", eui, exc)

    split = len(partitions) > 1
    partition_summary = [
        {
            "partition_id": pid,
            "leader_eui64": leaders_by_partition.get(pid),
            "member_count": len(members),
            "members": members,
        }
        for pid, members in sorted(partitions.items())
    ]

    # Open/close partition_split issue.
    try:
        active = [i for i in s.list_active_issues() if i.get("kind") == "partition_split"]
        if split:
            s.open_issue(
                kind="partition_split",
                severity="warning",
                evidence={
                    "partitions": partition_summary,
                    "partition_count": len(partitions),
                },
            )
        else:
            for issue in active:
                s.close_issue(int(issue["id"]))
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to update partition_split issue: %s", exc)

    log.info(
        "diagnostics persisted: nodes=%d links=%d partitions=%d split=%s changes=%d",
        diag_nodes, links_recorded, len(partitions), split, partition_change_events,
    )
    return {
        "nodes_with_diagnostics": diag_nodes,
        "links_recorded": links_recorded,
        "partition_split": split,
        "partitions": partition_summary,
        "partition_change_events": partition_change_events,
    }


def discover_and_sync_sync(store: SQLiteStore | None = None) -> dict[str, Any]:
    """Synchronous wrapper for discover_and_sync (for non-async contexts)."""
    return asyncio.run(discover_and_sync(store))
