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
from ..utils.coercion import coerce_int, first_present_field, to_tristate_int

log = logging.getLogger(__name__)

# HA config directory - typically /config in the addon environment
HA_CONFIG_DIR = Path(os.getenv("HA_CONFIG_DIR", "/config"))
DEVICE_REGISTRY_PATH = HA_CONFIG_DIR / ".storage" / "core.device_registry"
AREA_REGISTRY_PATH = HA_CONFIG_DIR / ".storage" / "core.area_registry"


def _load_area_registry() -> dict[str, str]:
    """Read HA's area registry and return ``{area_id: area_name}``.

    Returns an empty dict on any failure (file missing, malformed JSON,
    /config not mounted). Caller treats missing area_name as “unknown”.
    """
    try:
        raw = json.loads(AREA_REGISTRY_PATH.read_text())
    except FileNotFoundError:
        log.debug("area registry not found at %s", AREA_REGISTRY_PATH)
        return {}
    except Exception as exc:  # noqa: BLE001
        log.warning("failed to read area registry: %s", exc)
        return {}
    areas = (raw.get("data") or {}).get("areas") or []
    out: dict[str, str] = {}
    for a in areas:
        aid = a.get("id")
        name = a.get("name")
        if aid and name:
            out[str(aid)] = str(name)
    return out

# Matter server WebSocket endpoint. We query it to bridge Matter node_id
# (present in HA device registry as an identifier) to the Thread EUI64
# we extract from OTBR. Inside the HA stack, the matter_server addon is
# reachable by hostname; allow override for tests / non-default deployments.
MATTER_WS_URL = os.getenv(
    "MATTER_WS_URL",
    "ws://core-matter-server:5580/ws",
)
MATTER_WS_TIMEOUT = float(os.getenv("MATTER_WS_TIMEOUT", "5.0"))

# A node is considered phantom if it hasn't been referenced (as reporter or
# as a neighbor in any router's table) within this window. The default of
# 24h is forgiving enough to survive transient sleepy-end-device gaps while
# still flagging long-stale device-registry leftovers.
PHANTOM_THRESHOLD_HOURS = float(os.getenv("PHANTOM_THRESHOLD_HOURS", "24"))

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


def _decode_neighbor_table(raw: Any) -> list[dict[str, Any]]:
    """Decode a Matter NeighborTable attribute (cluster 53 attr 7).

    NeighborTableStruct fields per Matter spec:
      0 ExtAddress, 1 Age, 2 Rloc16, 3 LinkFrameCounter, 4 MleFrameCounter,
      5 LQI, 6 AverageRssi, 7 LastRssi, 8 FrameErrorRate, 9 MessageErrorRate,
      10 RxOnWhenIdle, 11 FullThreadDevice, 12 FullNetworkData, 13 IsChild.

    We surface the full struct (minus Rloc16 which is partition-local and
    ephemeral) so consumers can reason about neighbor capabilities, not just
    link quality. ``rx_on_when_idle=False`` plus ``full_thread_device=False``
    identifies SED/MED children that we should not expect to forward.
    """
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        eui = _ext_address_to_eui64(
            first_present_field(entry, "extAddress", "ExtAddress", int_key=0)
        )
        if not eui:
            continue
        is_child_raw = first_present_field(entry, "isChild", "IsChild", int_key=13)
        rx_on_raw = first_present_field(entry, "rxOnWhenIdle", "RxOnWhenIdle", int_key=10)
        ftd_raw = first_present_field(entry, "fullThreadDevice", "FullThreadDevice", int_key=11)
        fnd_raw = first_present_field(entry, "fullNetworkData", "FullNetworkData", int_key=12)
        out.append({
            "neighbor_eui64": eui,
            "rssi_avg": coerce_int(
                first_present_field(entry, "averageRssi", "AverageRssi", int_key=6)
            ),
            "rssi_last": coerce_int(
                first_present_field(entry, "lastRssi", "LastRssi", int_key=7)
            ),
            "lqi_in": coerce_int(first_present_field(entry, "lqi", "LQI", int_key=5)),
            "lqi_out": None,
            "is_child": to_tristate_int(is_child_raw),
            "age_seconds": coerce_int(first_present_field(entry, "age", "Age", int_key=1)),
            "frame_error_rate": coerce_int(
                first_present_field(entry, "frameErrorRate", "FrameErrorRate", int_key=8)
            ),
            "message_error_rate": coerce_int(
                first_present_field(entry, "messageErrorRate", "MessageErrorRate", int_key=9)
            ),
            "path_cost": None,
            "rx_on_when_idle": to_tristate_int(rx_on_raw),
            "full_thread_device": to_tristate_int(ftd_raw),
            "full_network_data": to_tristate_int(fnd_raw),
            "link_frame_counter": coerce_int(
                first_present_field(entry, "linkFrameCounter", "LinkFrameCounter", int_key=3)
            ),
            "mle_frame_counter": coerce_int(
                first_present_field(entry, "mleFrameCounter", "MleFrameCounter", int_key=4)
            ),
        })
    return out


def _decode_route_table(raw: Any) -> list[dict[str, Any]]:
    """Decode a Matter RouteTable attribute (cluster 53 attr 8).

    RouteTableStruct fields per Matter spec:
      0 ExtAddress, 1 Rloc16, 2 RouterId, 3 NextHop, 4 PathCost,
      5 LQIIn, 6 LQIOut, 7 Age, 8 Allocated, 9 LinkEstablished.

    We keep entries even when ``LinkEstablished=False`` because the NextHop +
    PathCost on those rows tell us the *multi-hop* routing path the reporter
    would use to reach that destination router (essential for resolving
    "next hop to OTBR" when the OTBR is not a direct neighbor).
    """
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        eui = _ext_address_to_eui64(
            first_present_field(entry, "extAddress", "ExtAddress", int_key=0)
        )
        if not eui:
            continue
        alloc_raw = first_present_field(entry, "allocated", "Allocated", int_key=8)
        est_raw = first_present_field(entry, "linkEstablished", "LinkEstablished", int_key=9)
        out.append({
            "neighbor_eui64": eui,
            "rssi_avg": None,
            "rssi_last": None,
            "lqi_in": coerce_int(first_present_field(entry, "lqiIn", "LQIIn", int_key=5)),
            "lqi_out": coerce_int(first_present_field(entry, "lqiOut", "LQIOut", int_key=6)),
            "is_child": None,
            "age_seconds": coerce_int(first_present_field(entry, "age", "Age", int_key=7)),
            "frame_error_rate": None,
            "message_error_rate": None,
            "path_cost": coerce_int(first_present_field(entry, "pathCost", "PathCost", int_key=4)),
            "router_id": coerce_int(first_present_field(entry, "routerId", "RouterId", int_key=2)),
            "next_hop_router_id": coerce_int(
                first_present_field(entry, "nextHop", "NextHop", int_key=3)
            ),
            "allocated": to_tristate_int(alloc_raw),
            "link_established": to_tristate_int(est_raw),
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

    def _get_str(suffix: str) -> str | None:
        val = attrs.get(f"0/53/{suffix}")
        if isinstance(val, str) and val:
            return val
        return None

    def _get_ext_pan_id() -> str | None:
        # 0x0004 ExtendedPanId — Matter spec defines it as octstr<8>
        # (8-byte). matter-server sometimes surfaces it as an int (raw
        # uint64), sometimes as a base64/hex string, depending on the
        # SDK version. Normalize to lowercase 16-char hex so two nodes
        # on the same Thread network always store the same string.
        val = attrs.get("0/53/4")
        if isinstance(val, int):
            return f"{val:016x}"
        if isinstance(val, str) and val:
            v = val.lower().removeprefix("0x")
            # Pure hex already?
            if all(c in "0123456789abcdef" for c in v) and len(v) <= 16:
                return v.rjust(16, "0")
            # base64 fallback for SDK builds that emit it that way.
            try:
                import base64  # noqa: PLC0415
                raw = base64.b64decode(val)
                if len(raw) == 8:
                    return raw.hex()
            except Exception:  # noqa: BLE001
                pass
        return None

    role_int = _get_int("1")
    # v10: stability counters from cluster 53. Spec attribute IDs noted in
    # parens; these are monotonic device-side counters that survive across
    # our snapshots. A fast climb in detached_role_count or
    # parent_change_count is the textbook signal of an unstable sleepy.
    #
    # Note: we intentionally skip attribute 15 here. Per Matter spec it is
    # ChildRoleCount (0x000F), but the python-matter-server build we target
    # surfaces ExtAddress at "/53/15" (see comment block at top of file),
    # so reading 15 as a counter would conflict with EUI64 resolution. The
    # other RoleCount attributes are unambiguous; ChildRoleCount can be
    # back-derived from the parent's NeighborTable child entries when
    # needed (and ``/v1/children/{eui64}`` exposes exactly that view).
    return {
        "channel": _get_int("0"),
        "routing_role_int": role_int,
        "routing_role": _ROUTING_ROLE_NAMES.get(role_int) if role_int is not None else None,
        "partition_id": _get_int("9"),
        "weighting": _get_int("10"),
        "leader_router_id": _get_int("13"),
        "detached_role_count": _get_int("14"),   # 0x000E
        "router_role_count": _get_int("16"),     # 0x0010
        "leader_role_count": _get_int("17"),     # 0x0011
        "attach_attempt_count": _get_int("18"),  # 0x0012
        "parent_change_count": _get_int("21"),   # 0x0015
        # v13 — partition stability counters.
        "partition_id_change_count": _get_int("19"),               # 0x0013
        "better_partition_attach_attempt_count": _get_int("20"),   # 0x0014
        # v13 — MAC Tx counters.
        "tx_total_count": _get_int("22"),                          # 0x0016
        "tx_retry_count": _get_int("33"),                          # 0x0021
        "tx_err_cca_count": _get_int("36"),                        # 0x0024
        "tx_err_abort_count": _get_int("37"),                      # 0x0025
        "tx_err_busy_channel_count": _get_int("38"),               # 0x0026
        # v13 — MAC Rx counters.
        "rx_total_count": _get_int("39"),                          # 0x0027
        "rx_duplicated_count": _get_int("49"),                     # 0x0031
        "rx_err_no_frame_count": _get_int("50"),                   # 0x0032
        "rx_err_sec_count": _get_int("53"),                        # 0x0035
        "rx_err_fcs_count": _get_int("54"),                        # 0x0036
        # v17 (0.9.46) — per-node Thread network identity.
        # attr 0x0002 NetworkName (e.g. "ha-thread-cb7d"),
        # attr 0x0004 ExtendedPanId (8-byte octstr, normalized to hex).
        "network_name": _get_str("2"),
        "extended_pan_id": _get_ext_pan_id(),
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
            # v17 (0.9.46): pull BasicInformation cluster (0x0028 = 40)
            # for vendor_id / product_id / serial_number so duplicate
            # physical-device detection can group rows by hardware
            # identity rather than relying on friendly_name.
            basic_info = {
                "vendor_id": attrs.get("0/40/2"),
                "product_id": attrs.get("0/40/4"),
                "serial_number": attrs.get("0/40/15"),
            }
            if eui or diagnostics["partition_id"] is not None or neighbor_table or route_table:
                rich_cache[canon_for_rich] = {
                    "eui64": eui,
                    "diagnostics": diagnostics,
                    "neighbor_table": neighbor_table,
                    "route_table": route_table,
                    "basic_info": basic_info,
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
    reg_devices = _load_device_registry()
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
            "sw_version": dev.get("sw_version"),
            "hw_version": dev.get("hw_version"),
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


def _load_device_registry() -> list[dict[str, Any]]:
    """Load the HA device registry from ``/config/.storage/core.device_registry``.

    Despite the legacy ``_fallback_`` name carried until v0.9.40, this is
    the **primary and only** source of HA device-registry data for the
    addon. The ``fetch_device_registry`` wrapper also consults OTBR's
    ``/api/topology`` for Thread-side hints (role, rloc), but every
    device_id / friendly_name / area mapping flows through this function.
    The Supervisor proxy (``/core/api/config/device_registry/list``) is
    a viable alternative, but the file read is faster and ``/config`` is
    already mounted for the entity registry that ``ha_availability.py``
    reads.
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
                        "sw_version": dev.get("sw_version"),
                        "hw_version": dev.get("hw_version"),
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
                        "sw_version": dev.get("sw_version"),
                        "hw_version": dev.get("hw_version"),
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

    # Resolve area_id -> area_name once. Empty dict if /config not mounted.
    area_names = _load_area_registry()

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
        manufacturer = dev.get("manufacturer")
        model = dev.get("model")
        area_id = dev.get("area_id")
        sw_version = dev.get("sw_version")
        hw_version = dev.get("hw_version")
        # Anything that contributes useful metadata is worth persisting,
        # even an unnamed registry device — area/manufacturer/model still
        # let the UI render context.
        if not any((friendly_name, device_id, area_id, manufacturer, model)):
            continue
        area_name = area_names.get(str(area_id)) if area_id else None
        # Deep link to the HA device page; HA renders /config/devices/device/<id>.
        ha_device_path = f"/config/devices/device/{device_id}" if device_id else None
        matches[eui] = {
            "friendly_name": friendly_name,
            "device_id": device_id,
            "manufacturer": manufacturer,
            "model": model,
            "area_id": area_id,
            "area_name": area_name,
            "sw_version": sw_version,
            "hw_version": hw_version,
            "ha_device_path": ha_device_path,
        }
        try:
            s.upsert_node_metadata(
                eui64=eui,
                friendly_name=friendly_name,
                device_id=device_id,
                area_id=area_id,
                area_name=area_name,
                manufacturer=manufacturer,
                model=model,
                sw_version=sw_version,
                hw_version=hw_version,
                ha_device_path=ha_device_path,
                is_thread=True,
            )
            if eui in existing_euis:
                updated += 1
                log.info(
                    "Updated node %s: name=%r area=%r mfg=%r model=%r",
                    eui, friendly_name, area_name, manufacturer, model,
                )
            else:
                inserted += 1
                log.info(
                    "Inserted node %s: name=%r area=%r mfg=%r model=%r",
                    eui, friendly_name, area_name, manufacturer, model,
                )
        except Exception as exc:
            log.warning("Failed to upsert node %s: %s", eui, exc)

    log.info(
        "device discovery: scanned %d devices, found %d matches, updated %d, inserted %d, area_registry=%d",
        len(devices), len(matches), updated, inserted, len(area_names),
    )

    # Persist Thread diagnostics + neighbor/route tables harvested from
    # matter-server (cluster 53). Also detect partition splits.
    diag_summary = await _persist_matter_diagnostics(s, nodes)

    # Evict stale link rows whose reporters have gone silent (~3\u00d7 discover
    # interval; configurable via env). Without this, zombie peers persist
    # forever — see CHANGELOG 0.9.30.
    link_ttl_s = int(os.getenv("LINK_TTL_SECONDS", "900"))
    try:
        evicted_links = s.sweep_stale_links(link_ttl_s)
        if evicted_links:
            log.info("link TTL sweep: evicted %d rows older than %ds", evicted_links, link_ttl_s)
    except Exception as exc:  # noqa: BLE001
        log.warning("link TTL sweep failed: %s", exc)
        evicted_links = 0

    return {
        "devices_scanned": len(devices),
        "matched": len(matches),
        "updated": updated,
        "inserted": inserted,
        "matches": matches,
        "diagnostics": diag_summary,
        "stale_links_evicted": evicted_links,
    }


async def _persist_matter_diagnostics(
    s: SQLiteStore,
    prior_nodes: list[dict[str, Any]],
) -> dict[str, Any]:
    """Persist cached Thread diagnostics (cluster 53) to the store.

    Uses `_LAST_MATTER_RICH_INFO` populated by the most recent bridge call.
    Returns a summary dict suitable for the discover_and_sync response.
    """
    rich = _LAST_MATTER_RICH_INFO
    if not rich:
        # v0.9.43: even with no rich info this cycle (matter-server WS hiccup
        # or a single empty poll), we MUST still reconcile the
        # ``partition_split`` issue. Otherwise an issue opened on a prior
        # cycle becomes immortal — it never sees a non-split observation
        # again because the empty-rich early-return below would skip the
        # close branch. Latent bug observed live as issue #54 hanging open
        # after the partition had long since healed.
        try:
            active = [
                i for i in s.list_active_issues()
                if i.get("kind") == "partition_split"
            ]
            for issue in active:
                s.close_issue(int(issue["id"]))
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "partition_split close-on-empty failed: %s", exc,
            )
        return {
            "nodes_with_diagnostics": 0,
            "links_recorded": 0,
            "partition_split": False,
            "phantom_marked": 0,
            "phantom_cleared": 0,
            "parent_change_events": 0,
            "link_acquired_events": 0,
            "link_lost_events": 0,
            "re_attached_events": 0,
            "rloc16_change_events": 0,
        }

    prior_by_eui = {n.get("eui64"): n for n in prior_nodes if n.get("eui64")}

    links_recorded = 0
    diag_nodes = 0
    partitions: dict[int, list[str]] = {}
    partition_change_events = 0
    parent_change_events = 0
    link_acquired_events = 0
    link_lost_events = 0
    re_attached_events = 0
    rloc16_change_events = 0
    leaders_by_partition: dict[int, str] = {}
    # v17 (0.9.46): track per-partition Thread network identity so the
    # partition_split issue's evidence can tell credentials-mismatch
    # apart from RF-fragmentation.
    network_identity_by_partition: dict[int, dict[str, str | None]] = {}

    # v0.9.45: pre-compute the set of reporters that re-attached this
    # cycle (parent_change_count strictly increased vs. the prior
    # snapshot). When a reporter re-attaches, MLE establishes new
    # sessions with all its neighbors and every neighbor's
    # link/MLE frame counter in this reporter's view resets — that
    # would otherwise emit one false ``re_attached_node`` per neighbor
    # per poll, attributed to the wrong device. Suppressing the
    # emission when the *reporter* re-attached is the right
    # attribution: the reporter swapped parents, not its neighbors.
    reporter_just_reattached: set[str] = set()
    for _node_id, info in rich.items():
        r_eui = info.get("eui64")
        if not r_eui:
            continue
        r_diag = info.get("diagnostics") or {}
        new_pcc = r_diag.get("parent_change_count")
        old_pcc = (prior_by_eui.get(r_eui) or {}).get("parent_change_count")
        if (
            isinstance(new_pcc, int)
            and isinstance(old_pcc, int)
            and new_pcc > old_pcc
        ):
            reporter_just_reattached.add(r_eui)

    # Collect every EUI we observe this cycle, either as a reporter or as a
    # neighbor in any router's table. This drives the phantom sweep below.
    referenced: set[str] = set()

    for _node_id, info in rich.items():
        eui = info.get("eui64")
        if not eui:
            continue
        referenced.add(eui)
        diag = info.get("diagnostics") or {}
        neighbor_table = info.get("neighbor_table") or []
        route_table = info.get("route_table") or []
        for entry in neighbor_table:
            nei = entry.get("neighbor_eui64")
            if nei:
                referenced.add(nei)
        for entry in route_table:
            nei = entry.get("neighbor_eui64")
            if nei:
                referenced.add(nei)

        # Persist links (replace per source). End devices typically have
        # neither table populated; we still issue replace calls so stale
        # rows from prior cycles get cleared.
        #
        # v13: emit link_acquired / link_lost events using the per-call
        # diff returned by replace_links_for_reporter. Suppress events on
        # the very first observation of a (reporter, source) tuple — we
        # detect this by checking whether the reporter had any prior row
        # in the links table for this source. Without this guard a cold
        # start would fire link_acquired for every existing edge.
        link_partition_id = diag.get("partition_id")
        try:
            # Drop the reporter's own EUI from its route_table before
            # persistence. Some Thread stacks (e.g. Eve) include a self
            # destination row (path_cost=0, indirect link); it's a no-op
            # routing entry that pollutes the links table, the
            # /v1/neighbors/{eui} view, and any consumer reasoning over
            # the routing graph. The self-row is still consulted below
            # to derive the reporter's own RouterId — that lookup runs
            # against ``route_table`` (the in-memory list), not the
            # filtered copy, so router-ID discovery is preserved.
            # See issue #1.
            route_table_persist = [
                r for r in route_table if r.get("neighbor_eui64") != eui
            ]
            for source, table in (
                ("neighbor_table", neighbor_table),
                ("route_table", route_table_persist),
            ):
                diff = s.replace_links_for_reporter(
                    eui, source, table,
                    partition_id=link_partition_id,
                )
                links_recorded += diff.get("inserted", 0)
                # The first-ever sweep for this (reporter, source) will
                # have prior_neighbors == empty AND new_neighbors !=
                # empty, so ``added`` is the full table and ``removed``
                # is empty. We can't reliably distinguish that from a
                # genuine mass-acquire (e.g. router just attached) at
                # the storage layer, so the suppression heuristic lives
                # here: if the reporter previously had no observed_at
                # timestamp for this source, treat ``added`` as
                # baseline. The cheap proxy: the prior_nodes snapshot's
                # diag_updated_at being None means we've never persisted
                # diagnostics for this node before.
                prior = prior_by_eui.get(eui) or {}
                first_observation = prior.get("diag_updated_at") is None
                if first_observation:
                    continue
                for neighbor in diff.get("added", []):
                    s.insert_event(
                        eui64=eui,
                        type="link_acquired",
                        payload={
                            "reporter_eui64": eui,
                            "neighbor_eui64": neighbor,
                            "source": source,
                            "partition_id": link_partition_id,
                        },
                    )
                    link_acquired_events += 1
                for neighbor in diff.get("removed", []):
                    s.insert_event(
                        eui64=eui,
                        type="link_lost",
                        payload={
                            "reporter_eui64": eui,
                            "neighbor_eui64": neighbor,
                            "source": source,
                            "partition_id": link_partition_id,
                        },
                    )
                    link_lost_events += 1
                # v0.9.43: emit re_attached_node when a neighbor's frame
                # counter drops between consecutive observations. Matter
                # MLE / link frame counters are monotonic for the
                # lifetime of a session; a strictly-smaller new value is
                # the cryptographic signal of a fresh attach (new
                # session keys, counters reinitialised). This is the
                # primary tell for the Foyer-Light triple-identity case:
                # an old EUI keeps appearing in the parent's
                # NeighborTable with counters that keep resetting to 1
                # because the operational identity behind it is being
                # re-created every commissioning cycle.
                #
                # We only check kept neighbours (present both before and
                # after). Newly-added ones have no prior counter; removed
                # ones already emitted link_lost.
                #
                # v0.9.45: skip when the *reporter* itself re-attached
                # this cycle. Its new MLE session resets every
                # neighbor's counter from its point of view, which
                # would otherwise fire one false ``re_attached_node``
                # per neighbor attributed to the wrong device.
                if eui in reporter_just_reattached:
                    continue
                prior_fcs = diff.get("prior_frame_counters") or {}
                table_by_neighbor = {
                    e.get("neighbor_eui64"): e for e in table
                    if e.get("neighbor_eui64")
                }
                for neighbor, prior_pair in prior_fcs.items():
                    if neighbor in diff.get("removed", []):
                        continue
                    new_entry = table_by_neighbor.get(neighbor)
                    if not new_entry:
                        continue
                    for counter_name in ("link_frame_counter", "mle_frame_counter"):
                        old_v = prior_pair.get(counter_name)
                        new_v = new_entry.get(counter_name)
                        if (
                            isinstance(old_v, int)
                            and isinstance(new_v, int)
                            and new_v < old_v
                        ):
                            s.insert_event(
                                eui64=neighbor,
                                type="re_attached_node",
                                payload={
                                    "reporter_eui64": eui,
                                    "neighbor_eui64": neighbor,
                                    "source": source,
                                    "counter": counter_name,
                                    "old_value": old_v,
                                    "new_value": new_v,
                                    "partition_id": link_partition_id,
                                },
                            )
                            re_attached_events += 1
                            # Only emit once per (neighbor, source) per
                            # cycle even if both counters reset.
                            break
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to persist links for %s: %s", eui, exc)

        # Determine this router's own Router ID from its RouteTable self-entry.
        # A router's own RouteTable always contains a row where ExtAddress
        # equals its own EUI64; that row's RouterId field is the reporter's
        # ID within the partition. Needed to resolve next-hop references.
        #
        # v0.9.43: ``set_node_router_id`` now returns the prior + new
        # router_id / rloc16 so we can emit an ``rloc16_change`` event
        # when the assignment changes. The first observation (prior was
        # None) is suppressed — that's a baseline, not a change.
        try:
            for entry in route_table:
                if entry.get("neighbor_eui64") == eui and entry.get("router_id") is not None:
                    diff = s.set_node_router_id(eui, int(entry["router_id"]))
                    old_r = diff.get("old_router_id")
                    new_r = diff.get("new_router_id")
                    if (
                        isinstance(old_r, int)
                        and isinstance(new_r, int)
                        and old_r != new_r
                    ):
                        s.insert_event(
                            eui64=eui,
                            type="rloc16_change",
                            payload={
                                "from_router_id": old_r,
                                "to_router_id": new_r,
                                "from_rloc16": diff.get("old_rloc16"),
                                "to_rloc16": diff.get("new_rloc16"),
                                "partition_id": diag.get("partition_id"),
                            },
                        )
                        rloc16_change_events += 1
                    break
        except Exception as exc:  # noqa: BLE001
            log.debug("router_id self-detect failed for %s: %s", eui, exc)

        # v13: emit parent_change event when parent_change_count
        # increments. The cluster counter is monotonic on the device, so
        # any positive delta versus our last snapshot is a genuine new
        # parent swap (or a batch of them, which we encode as ``delta``).
        # A drop indicates the device reset its counters (firmware
        # update, factory reset); we treat that as a re-baseline and
        # don't emit an event.
        try:
            new_pcc = diag.get("parent_change_count")
            prior = prior_by_eui.get(eui) or {}
            old_pcc = prior.get("parent_change_count")
            if (
                isinstance(new_pcc, int)
                and isinstance(old_pcc, int)
                and new_pcc > old_pcc
            ):
                s.insert_event(
                    eui64=eui,
                    type="parent_change",
                    payload={
                        "from_count": old_pcc,
                        "to_count": new_pcc,
                        "delta": new_pcc - old_pcc,
                        "partition_id": diag.get("partition_id"),
                    },
                )
                parent_change_events += 1
        except Exception as exc:  # noqa: BLE001
            log.debug("parent_change diff failed for %s: %s", eui, exc)

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
                detached_role_count=diag.get("detached_role_count"),
                router_role_count=diag.get("router_role_count"),
                leader_role_count=diag.get("leader_role_count"),
                attach_attempt_count=diag.get("attach_attempt_count"),
                parent_change_count=diag.get("parent_change_count"),
                partition_id_change_count=diag.get("partition_id_change_count"),
                better_partition_attach_attempt_count=diag.get(
                    "better_partition_attach_attempt_count"
                ),
                tx_total_count=diag.get("tx_total_count"),
                tx_retry_count=diag.get("tx_retry_count"),
                tx_err_cca_count=diag.get("tx_err_cca_count"),
                tx_err_abort_count=diag.get("tx_err_abort_count"),
                tx_err_busy_channel_count=diag.get("tx_err_busy_channel_count"),
                rx_total_count=diag.get("rx_total_count"),
                rx_duplicated_count=diag.get("rx_duplicated_count"),
                rx_err_no_frame_count=diag.get("rx_err_no_frame_count"),
                rx_err_sec_count=diag.get("rx_err_sec_count"),
                rx_err_fcs_count=diag.get("rx_err_fcs_count"),
                network_name=diag.get("network_name"),
                extended_pan_id=diag.get("extended_pan_id"),
            )
            if updated_diag:
                diag_nodes += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to persist diagnostics for %s: %s", eui, exc)

        # v19 (0.10.0 / Phase 4): also persist the volatile counters as a
        # time-series sample so the reasoner / counter_series tool can
        # compute correct deltas across ticks. Best-effort; never block
        # the pipeline on this.
        try:
            counter_keys = (
                "tx_total_count", "tx_retry_count",
                "tx_err_cca_count", "tx_err_abort_count", "tx_err_busy_channel_count",
                "rx_total_count", "rx_duplicated_count",
                "rx_err_no_frame_count", "rx_err_sec_count", "rx_err_fcs_count",
                "parent_change_count", "attach_attempt_count",
                "partition_id_change_count", "better_partition_attach_attempt_count",
            )
            counters = {k: diag.get(k) for k in counter_keys if diag.get(k) is not None}
            if counters:
                s.record_counter_sample(eui64=eui, counters=counters)
        except Exception as exc:  # noqa: BLE001
            log.debug("record_counter_sample failed for %s: %s", eui, exc)

        # v17 (0.9.46): persist hardware identity (vendor_id, product_id,
        # serial_number) so duplicate physical devices (same hardware
        # commissioned under multiple EUI64s) can be detected.
        basic_info = info.get("basic_info") or {}
        bi_vid = basic_info.get("vendor_id")
        bi_pid = basic_info.get("product_id")
        bi_sn = basic_info.get("serial_number")
        if any((isinstance(bi_vid, int), isinstance(bi_pid, int), isinstance(bi_sn, str) and bi_sn)):
            try:
                s.upsert_node_metadata(
                    eui64=eui,
                    vendor_id=bi_vid if isinstance(bi_vid, int) else None,
                    product_id=bi_pid if isinstance(bi_pid, int) else None,
                    serial_number=bi_sn if isinstance(bi_sn, str) and bi_sn else None,
                )
            except Exception as exc:  # noqa: BLE001
                log.debug("Failed to persist basic_info for %s: %s", eui, exc)

        # Partition tracking + change detection.
        pid = diag.get("partition_id")
        if isinstance(pid, int):
            partitions.setdefault(pid, []).append(eui)
            # First node that reports a network_name / extended_pan_id
            # in this partition wins (they should all agree within a
            # partition; if they don't, that's a separate problem).
            ident_slot = network_identity_by_partition.setdefault(
                pid, {"network_name": None, "extended_pan_id": None}
            )
            if ident_slot["network_name"] is None and diag.get("network_name"):
                ident_slot["network_name"] = diag.get("network_name")
            if ident_slot["extended_pan_id"] is None and diag.get("extended_pan_id"):
                ident_slot["extended_pan_id"] = diag.get("extended_pan_id")
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

    # Bump last_referenced_at for everything we observed, then recompute
    # node status (online / offline / unregistered / phantom). The legacy
    # binary phantom sweep stays for one cycle of backwards compat with the
    # diagnostics summary; the new column is the authoritative signal.
    try:
        s.bump_last_referenced(referenced)
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to bump last_referenced_at: %s", exc)

    # Registry-first (v9): the registry sync above may have added or
    # removed nodes; reconcile each link's ``neighbor_known`` flag so
    # ``/v1/links/stale`` reflects the current node set without waiting
    # for the next reporter poll cycle.
    try:
        nk = s.refresh_neighbor_known()
        if nk["marked_known"] or nk["marked_stale"]:
            log.info(
                "neighbor_known refresh: marked_known=%d marked_stale=%d",
                nk["marked_known"], nk["marked_stale"],
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("refresh_neighbor_known failed: %s", exc)

    # Status thresholds. `OFFLINE_AFTER_SECONDS` flips online -> offline;
    # `PHANTOM_AFTER_SECONDS` flips offline -> phantom (eligible for purge,
    # unless HA-registered). Both env-configurable for ops dial-in.
    offline_after_s = int(os.getenv("OFFLINE_AFTER_SECONDS", "900"))         # 15 min
    phantom_after_s = int(os.getenv("PHANTOM_AFTER_SECONDS",
                                     str(int(PHANTOM_THRESHOLD_HOURS * 3600))))  # 24h default

    # v0.9.39: refresh per-node ``available`` from HA entity states before
    # recomputing status. This is the canonical "can HA control it right
    # now?" signal — the source of truth the user sees in the HA UI.
    # ``last_referenced_at`` continues to track mesh-side visibility as an
    # independent diagnostic field. Best-effort: any failure (missing
    # token, REST 4xx, JSON error) leaves the columns unchanged and the
    # recompute falls back to the legacy last_referenced_at heuristic.
    avail_summary: dict[str, int] = {}
    try:
        from . import ha_availability  # local import to avoid circular load
        device_avail = await ha_availability.fetch_device_availability()
        if device_avail:
            nodes_now = s.list_nodes()
            # Map device_id -> eui64 from our authoritative node set.
            updates: list[tuple[str, bool | None, str]] = []
            for n in nodes_now:
                dev_id = n.get("device_id")
                eui = n.get("eui64")
                if not eui or not dev_id:
                    continue
                if dev_id in device_avail:
                    updates.append((eui, bool(device_avail[dev_id]), "ha_entity"))
            if updates:
                avail_summary = s.apply_availability(updates)
                log.info(
                    "availability: applied=%d skipped=%d (ha_devices=%d, nodes=%d)",
                    avail_summary.get("applied", 0),
                    avail_summary.get("skipped", 0),
                    len(device_avail),
                    len(nodes_now),
                )
    except Exception as exc:  # noqa: BLE001
        log.warning("availability refresh failed: %s", exc)

    status_summary: dict[str, int] = {}
    try:
        status_summary = s.recompute_node_statuses(
            offline_seconds=offline_after_s,
            phantom_seconds=phantom_after_s,
        )
        if status_summary.get("changed"):
            log.info(
                "status: online=%d offline=%d unregistered=%d phantom=%d (changed=%d)",
                status_summary.get("online", 0),
                status_summary.get("offline", 0),
                status_summary.get("unregistered", 0),
                status_summary.get("phantom", 0),
                status_summary["changed"],
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("recompute_node_statuses failed: %s", exc)

    # Purge eligible expired nodes (phantom OR offline-beyond-retention,
    # never HA-registered). 30-day retention by default.
    max_offline_s = int(os.getenv("OFFLINE_RETENTION_SECONDS", str(30 * 86400)))
    try:
        purged = s.purge_expired_nodes(max_offline_seconds=max_offline_s)
        if purged.get("deleted_nodes"):
            log.info(
                "purge_expired_nodes: deleted %d nodes / %d links (retention=%ds)",
                purged["deleted_nodes"], purged["deleted_links"], max_offline_s,
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("purge_expired_nodes failed: %s", exc)

    # v19 (0.10.0 / Phase 4): apply retention to node_counter_samples.
    # Downsample rows older than full_resolution_days to 5-minute averages,
    # then drop rows older than sampled_archive_days. Best-effort.
    try:
        from ..config import get_config as _get_config
        ret = _get_config().retention
        pruned = s.prune_counter_samples(
            full_resolution_days=ret.full_resolution_days,
            sampled_archive_days=ret.sampled_archive_days,
        )
        if pruned.get("deleted") or pruned.get("downsampled"):
            log.info(
                "prune_counter_samples: deleted=%d downsampled=%d kept=%d",
                pruned["deleted"], pruned["downsampled"], pruned["kept"],
            )
    except Exception as exc:  # noqa: BLE001
        log.debug("prune_counter_samples failed: %s", exc)

    # ``recompute_node_statuses`` (above) is the single source of truth
    # for phantom state since v0.9.40 — the legacy ``sweep_phantoms``
    # was retired along with the ``is_phantom`` column. The counters
    # below are kept so downstream stats consumers don't break.
    phantom_marked = phantom_cleared = 0

    # Filter out partitions whose only members are currently phantom (the
    # soil-sensor / re-commissioned-Foyer-Light case). A real split must
    # involve at least one live node beyond a single phantom.
    live_euis = {
        n["eui64"] for n in s.list_nodes()
        if n.get("eui64") and n.get("status") != "phantom"
    }
    live_partitions: dict[int, list[str]] = {}
    excluded_partitions: list[int] = []
    for pid, members in partitions.items():
        live_members = [m for m in members if m in live_euis]
        if live_members:
            live_partitions[pid] = members
        else:
            excluded_partitions.append(pid)

    split = len(live_partitions) > 1
    partition_summary = [
        {
            "partition_id": pid,
            "leader_eui64": leaders_by_partition.get(pid),
            "member_count": len(members),
            "members": members,
            "network_name": network_identity_by_partition.get(pid, {}).get("network_name"),
            "extended_pan_id": network_identity_by_partition.get(pid, {}).get("extended_pan_id"),
        }
        for pid, members in sorted(live_partitions.items())
    ]

    # Open/close partition_split issue (now reasoning over live partitions only).
    try:
        active = [i for i in s.list_active_issues() if i.get("kind") == "partition_split"]
        if split:
            distinct_epids = sorted({
                p["extended_pan_id"] for p in partition_summary
                if p.get("extended_pan_id")
            })
            s.open_issue(
                kind="partition_split",
                severity="warning",
                evidence={
                    "partitions": partition_summary,
                    "partition_count": len(live_partitions),
                    # If partitions report different extended_pan_ids,
                    # this is a credentials-mismatch (stale dataset on
                    # one device) not an RF-fragmentation issue.
                    "distinct_extended_pan_ids": distinct_epids,
                    "credentials_mismatch_suspected": len(distinct_epids) > 1,
                },
            )
        else:
            for issue in active:
                s.close_issue(int(issue["id"]))
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to update partition_split issue: %s", exc)

    log.info(
        "diagnostics persisted: nodes=%d links=%d partitions=%d split=%s "
        "changes=%d phantoms_marked=%d phantoms_cleared=%d excluded_partitions=%d "
        "parent_changes=%d link_acq=%d link_lost=%d re_attached=%d rloc16_changes=%d",
        diag_nodes, links_recorded, len(live_partitions), split,
        partition_change_events, phantom_marked, phantom_cleared,
        len(excluded_partitions),
        parent_change_events, link_acquired_events, link_lost_events,
        re_attached_events, rloc16_change_events,
    )
    return {
        "nodes_with_diagnostics": diag_nodes,
        "links_recorded": links_recorded,
        "partition_split": split,
        "partitions": partition_summary,
        "partition_change_events": partition_change_events,
        "parent_change_events": parent_change_events,
        "link_acquired_events": link_acquired_events,
        "link_lost_events": link_lost_events,
        "re_attached_events": re_attached_events,
        "rloc16_change_events": rloc16_change_events,
        "phantom_marked": phantom_marked,
        "phantom_cleared": phantom_cleared,
        "excluded_phantom_partitions": excluded_partitions,
    }


def discover_and_sync_sync(store: SQLiteStore | None = None) -> dict[str, Any]:
    """Synchronous wrapper for discover_and_sync (for non-async contexts)."""
    return asyncio.run(discover_and_sync(store))
