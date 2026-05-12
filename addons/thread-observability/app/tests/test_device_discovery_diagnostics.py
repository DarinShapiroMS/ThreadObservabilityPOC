"""Tests for Matter cluster-53 struct decoders in device_discovery."""

from __future__ import annotations

from thread_observability.pipeline.device_discovery import (
    _decode_neighbor_table,
    _decode_route_table,
    _ext_address_to_eui64,
    _extract_thread_diagnostics,
)


def test_ext_address_decode_int() -> None:
    # uint64 fitting Matter ExtAddress.
    assert _ext_address_to_eui64(0xC6B77F58E5ACEED4) == "c6b77f58e5aceed4"


def test_ext_address_decode_hex_string() -> None:
    assert _ext_address_to_eui64("0xC6B77F58E5ACEED4") == "c6b77f58e5aceed4"
    assert _ext_address_to_eui64("c6b77f58e5aceed4") == "c6b77f58e5aceed4"


def test_ext_address_decode_invalid() -> None:
    assert _ext_address_to_eui64(None) is None
    assert _ext_address_to_eui64(-1) is None
    assert _ext_address_to_eui64("zzzz") is None


def test_decode_neighbor_table_struct_fields() -> None:
    # Matter struct uses integer field IDs as string keys.
    raw = [
        {
            "0": 0xAABBCCDDEEFF0011,  # ExtAddress
            "1": 12,                   # Age
            "5": 240,                  # LQI
            "6": -55,                  # AverageRssi
            "7": -57,                  # LastRssi
            "8": 0,                    # FrameErrorRate
            "9": 0,                    # MessageErrorRate
            "13": True,                # IsChild
        },
        {
            "0": "0011223344556677",   # Hex string ExtAddress
            "5": 100,
            "6": -85,
            "13": False,
        },
    ]
    out = _decode_neighbor_table(raw)
    assert len(out) == 2
    assert out[0]["neighbor_eui64"] == "aabbccddeeff0011"
    assert out[0]["rssi_avg"] == -55
    assert out[0]["lqi_in"] == 240
    assert out[0]["is_child"] == 1
    assert out[0]["age_seconds"] == 12
    assert out[1]["is_child"] == 0
    assert out[1]["rssi_avg"] == -85


def test_decode_neighbor_table_skips_invalid() -> None:
    assert _decode_neighbor_table(None) == []
    assert _decode_neighbor_table("not a list") == []
    # Entry without ExtAddress is skipped.
    assert _decode_neighbor_table([{"5": 240}]) == []


def test_decode_route_table_struct_fields() -> None:
    raw = [
        {
            "0": 0x1122334455667788,
            "2": 5,    # RouterId
            "4": 1,    # PathCost
            "5": 200,  # LQIIn
            "6": 180,  # LQIOut
            "7": 30,   # Age
            "9": True, # LinkEstablished
        },
        # LinkEstablished=False should be skipped.
        {"0": 0x99AABBCCDDEEFF00, "4": 2, "9": False},
    ]
    out = _decode_route_table(raw)
    assert len(out) == 1
    assert out[0]["neighbor_eui64"] == "1122334455667788"
    assert out[0]["path_cost"] == 1
    assert out[0]["lqi_in"] == 200
    assert out[0]["lqi_out"] == 180


def test_extract_thread_diagnostics() -> None:
    attrs = {
        "0/53/0": 15,       # Channel
        "0/53/1": 6,        # RoutingRole: leader
        "0/53/9": 0xCAFEBABE,  # PartitionId
        "0/53/10": 64,      # Weighting
        "0/53/13": 0,       # LeaderRouterId
        "0/53/15": "irrelevant",
    }
    d = _extract_thread_diagnostics(attrs)
    assert d["channel"] == 15
    assert d["routing_role"] == "leader"
    assert d["partition_id"] == 0xCAFEBABE
    assert d["weighting"] == 64
    assert d["leader_router_id"] == 0


def test_extract_thread_diagnostics_missing() -> None:
    d = _extract_thread_diagnostics({})
    assert d["channel"] is None
    assert d["routing_role"] is None
    assert d["partition_id"] is None
