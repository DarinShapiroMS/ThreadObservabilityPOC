"""JSON-driven scenario loader.

A scenario file describes a Thread mesh as plain data: nodes, their
Thread diagnostics, neighbor/route tables, recent events, and the
assertions that should hold once the data is loaded. Adding a new mesh
quirk to the test matrix is a new JSON file, not new Python.

JSON schema (see ``fixtures/single_otbr_three_routers.json`` for a full
example)::

    {
        "name": "...",
        "description": "...",
        "nodes": [
            {
                "eui64": "16-hex-chars",
                "friendly_name": "...",
                "role": "border_router" | "router" | "end_device" | null,
                "device_id": "<HA device id>" | null,
                "diagnostics": {
                    "partition_id": int | null,
                    "routing_role": "leader" | "router" | "reed" | "child" | null,
                    "leader_router_id": int | null,
                    "active_routers": int | null
                },
                "router_id": int | null,
                "phantom": bool                     // force status='phantom' if true
            }
        ],
        "links": [
            {
                "reporter": "16-hex-chars",
                "source": "neighbor_table" | "route_table",
                "partition_id": int | null,
                "rows": [ {<replace_links_for_reporter row>}, ... ]
            }
        ],
        "events": [
            {"eui64": "...", "type": "attach", "rssi": int, "lqi": int}
        ],
        "expectations": {
            "topology":   {"node_count": int, "split": bool, "link_count": int},
            "partitions": {"partition_count": int, "split": bool},
            "routes":     [{"source": "<eui>", "complete": bool, "hop_count": int,
                            "issue_codes": ["..."]}],
            "dev_status": {"otbr_eui64": "<eui> | null", "node_counts": {...}}
        }
    }

Every key in ``expectations`` is optional; only the keys present are
asserted, so a fixture can focus on a specific behaviour without
needing to declare the whole world.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from thread_observability.storage.sqlite_store import SQLiteStore


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def discover_fixtures() -> list[Path]:
    """Return every ``*.json`` file under ``fixtures/`` in sorted order."""
    if not FIXTURES_DIR.exists():
        return []
    return sorted(FIXTURES_DIR.glob("*.json"))


def load_scenario(path: Path) -> dict[str, Any]:
    """Parse and lightly validate a scenario JSON file."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if "name" not in data:
        raise ValueError(f"{path.name}: missing required 'name' field")
    return data


def seed_store(store: SQLiteStore, scenario: dict[str, Any]) -> None:
    """Populate ``store`` from a scenario dict.

    Order matters: nodes first (so links can reference them), then
    diagnostics + router IDs, then links, then events. We do not run the
    pipeline — these tests assert what the API would see if the pipeline
    had just finished.
    """
    # 1) Nodes.
    for node in scenario.get("nodes", []):
        eui = node["eui64"]
        kwargs: dict[str, Any] = {"eui64": eui}
        for k in ("friendly_name", "role", "device_id", "area", "manufacturer",
                 "model", "sw_version", "hw_version", "ha_device_path"):
            if k in node:
                kwargs[k] = node[k]
        store.upsert_node_metadata(**kwargs)

    # 2) Diagnostics + router IDs.
    for node in scenario.get("nodes", []):
        eui = node["eui64"]
        diag = node.get("diagnostics") or {}
        if diag:
            store.set_node_diagnostics(
                eui,
                partition_id=diag.get("partition_id"),
                leader_router_id=diag.get("leader_router_id"),
                routing_role=diag.get("routing_role"),
                active_routers=diag.get("active_routers"),
                channel=diag.get("channel"),
                weighting=diag.get("weighting"),
            )
        if "router_id" in node and node["router_id"] is not None:
            store.set_node_router_id(eui, int(node["router_id"]))
        # Phantom flag: mirror what recompute_node_statuses would have set.
        if node.get("phantom") is True:
            with store._lock:  # noqa: SLF001 - test seeding only
                store._conn.execute(  # noqa: SLF001
                    "UPDATE nodes SET status = 'phantom' "
                    "WHERE eui64 = ?",
                    (eui,),
                )

    # 3) Links.
    for spec in scenario.get("links", []):
        store.replace_links_for_reporter(
            spec["reporter"],
            spec["source"],
            spec.get("rows", []),
            partition_id=spec.get("partition_id"),
        )

    # 4) Events.
    for ev in scenario.get("events", []):
        store.insert_event(
            eui64=ev["eui64"],
            type=ev["type"],
            rssi=ev.get("rssi"),
            lqi=ev.get("lqi"),
            parent_eui64=ev.get("parent_eui64"),
            payload=ev.get("payload"),
        )
