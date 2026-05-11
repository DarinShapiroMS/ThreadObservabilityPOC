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
                    log.debug("OTBR endpoint %s failed: %s", endpoint, exc)
                    continue
    except Exception as exc:
        log.warning("Failed to fetch OTBR topology: %s", exc)
    
    # Now fetch device registry to get friendly names and metadata
    reg_devices = _fallback_device_registry()
    registry_by_eui: dict[str, dict[str, Any]] = {}
    for dev in reg_devices:
        connections = dev.get("connections", [])
        for conn_type, conn_id in connections:
            if conn_type in ("thread", "zigbee", "ieee802154"):
                try:
                    eui = _normalize_ieee(str(conn_id))
                    registry_by_eui[eui] = {
                        "device_id": dev.get("id"),
                        "name": dev.get("name"),
                        "name_by_user": dev.get("name_by_user"),
                        "manufacturer": dev.get("manufacturer"),
                        "model": dev.get("model"),
                        "area_id": dev.get("area_id"),
                        "primary_config_entry": dev.get("primary_config_entry"),
                    }
                    break  # Use first Thread connection found
                except Exception as exc:
                    log.debug("Failed to parse connection %s: %s", conn_id, exc)
    
    if registry_by_eui:
        log.debug("Loaded device registry with %d Thread devices", len(registry_by_eui))
    
    # Merge: OTBR nodes are the primary source, supplemented with registry data
    merged: dict[str, dict[str, Any]] = {}
    
    # Add OTBR nodes with any matching registry data
    for eui, otbr_data in otbr_nodes.items():
        merged[eui] = {**otbr_data}
        if eui in registry_by_eui:
            merged[eui].update(registry_by_eui[eui])
    
    # Add registry-only devices (not discovered from OTBR)
    for eui, reg_data in registry_by_eui.items():
        if eui not in merged:
            merged[eui] = reg_data
    
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
                    out[eui] = {
                        "role": dev.get("role"),
                        "rloc": dev.get("rloc"),
                    }
                    log.debug(
                        "Found Thread node from OTBR: eui=%s role=%s",
                        eui,
                        dev.get("role"),
                    )
                except Exception as exc:
                    log.debug("Failed to parse OTBR node %s: %s", ext_addr, exc)
        
        # Otherwise, check if this is a device registry device (has connections)
        connections = dev.get("connections", [])
        for conn_type, conn_id in connections:
            # Thread devices typically use "thread" or "zigbee" connection types
            if conn_type in ("thread", "zigbee", "ieee802154"):
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
