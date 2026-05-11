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
# Matter Thread Network Diagnostics cluster id (0x0035 = 53), ExtAddress
# attribute (0x000F = 15) — defined as the 8-byte Thread EUI64.
_MATTER_THREAD_DIAG_EXTADDR_SUFFIX = "/53/15"

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

    log.info(
        "Matter bridge: extracted %d EUI64 mappings from %d nodes",
        len(bridge), len(nodes),
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

    # Correlate with our nodes
    nodes = s.list_nodes()
    updated = 0
    matches: dict[str, dict[str, Any]] = {}

    for node in nodes:
        eui = node.get("eui64")
        if not eui:
            continue
        if eui in thread_devs:
            dev = thread_devs[eui]
            # Use name_by_user (user-set) if available, else the auto name
            friendly_name = dev.get("name_by_user") or dev.get("name")
            device_id = dev.get("device_id")
            matches[eui] = {
                "friendly_name": friendly_name,
                "device_id": device_id,
                "manufacturer": dev.get("manufacturer"),
                "model": dev.get("model"),
            }
            # Update the node with metadata
            try:
                s.set_node_metadata(
                    eui64=eui,
                    friendly_name=friendly_name,
                    device_id=device_id,
                )
                updated += 1
                log.info(
                    "Updated node %s with device name '%s'",
                    eui, friendly_name,
                )
            except Exception as exc:
                log.warning("Failed to update node %s: %s", eui, exc)

    log.info(
        "device discovery: scanned %d devices, found %d matches, updated %d nodes",
        len(devices), len(matches), updated,
    )
    return {
        "devices_scanned": len(devices),
        "matched": len(matches),
        "updated": updated,
        "matches": matches,
    }


def discover_and_sync_sync(store: SQLiteStore | None = None) -> dict[str, Any]:
    """Synchronous wrapper for discover_and_sync (for non-async contexts)."""
    return asyncio.run(discover_and_sync(store))
