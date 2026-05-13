"""Tests for the SQLite store (schema, events, issues)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from thread_observability.storage.sqlite_store import SQLiteStore


def test_migrations_apply(store: SQLiteStore) -> None:
    assert store.schema_version == 22
    stats = store.stats()
    assert stats["schema_version"] == 22
    assert stats["row_counts"]["events"] == 0


def test_insert_event_updates_known_node_only(store: SQLiteStore) -> None:
    """Registry-first (v9): events only update existing node rows.

    Unknown EUIs never get auto-inserted from event ingestion — they
    belong on the link side via the ``neighbor_known`` flag, not as
    phantom nodes.
    """
    eui = "aa" * 8
    # Unknown EUI: event records but no node row created.
    eid = store.insert_event(eui64=eui, type="attach", rssi=-60, lqi=200)
    assert eid >= 1
    assert store.get_node(eui) is None

    # Once registered, subsequent events update last_seen on the row.
    store.upsert_node_metadata(eui64=eui)
    store.insert_event(eui64=eui, type="attach", rssi=-55, lqi=210)
    node = store.get_node(eui)
    assert node is not None
    assert node["last_seen"]


def test_query_events_filters(store: SQLiteStore) -> None:
    store.insert_event(eui64="11" * 8, type="attach")
    store.insert_event(eui64="22" * 8, type="attach_failed")
    store.insert_event(eui64="11" * 8, type="parent_change", parent_eui64="22" * 8)

    by_node = store.query_events(eui64="11" * 8)
    assert {e["type"] for e in by_node} == {"attach", "parent_change"}

    by_type = store.query_events(event_type="attach_failed")
    assert len(by_type) == 1 and by_type[0]["eui64"] == "22" * 8


def test_issue_dedupe_and_close(store: SQLiteStore) -> None:
    first = store.open_issue(kind="parent_churn", severity="warn", eui64="11" * 8,
                             evidence={"count": 3})
    second = store.open_issue(kind="parent_churn", severity="warn", eui64="11" * 8,
                              evidence={"count": 5})
    assert first == second, "dedupe should return same id"

    active = store.list_active_issues()
    assert len(active) == 1
    assert active[0]["evidence"]["count"] == 5

    assert store.close_issue(first) is True
    assert store.list_active_issues() == []
    assert store.close_issue(first) is False, "double-close is a no-op"


def test_query_events_since(store: SQLiteStore) -> None:
    old = (datetime.now(tz=UTC) - timedelta(hours=2)).isoformat()
    new = datetime.now(tz=UTC).isoformat()
    store.insert_event(eui64="11" * 8, type="attach", ts=old)
    store.insert_event(eui64="11" * 8, type="attach", ts=new)

    recent = store.query_events(since=(datetime.now(tz=UTC) - timedelta(hours=1)).isoformat())
    assert len(recent) == 1
    assert recent[0]["ts"] == new


def test_links_replace_and_list(store: SQLiteStore) -> None:
    A = "aa" * 8
    B = "bb" * 8
    C = "cc" * 8
    n = store.replace_links_for_reporter(A, "neighbor_table", [
        {"neighbor_eui64": B, "rssi_avg": -50},
        {"neighbor_eui64": C, "rssi_avg": -60, "is_child": 1},
    ])
    assert n["inserted"] == 2
    rows = store.list_links()
    assert len(rows) == 2
    # Replace overwrites prior entries for the same (reporter, source).
    store.replace_links_for_reporter(A, "neighbor_table", [
        {"neighbor_eui64": B, "rssi_avg": -45},
    ])
    rows = store.list_links()
    assert len(rows) == 1
    assert rows[0]["rssi_avg"] == -45
    # Different source coexists.
    store.replace_links_for_reporter(A, "route_table", [
        {"neighbor_eui64": C, "path_cost": 1},
    ])
    assert len(store.list_links()) == 2
    assert len(store.list_links(source="route_table")) == 1


def test_set_node_diagnostics(store: SQLiteStore) -> None:
    A = "aa" * 8
    store.upsert_node_metadata(eui64=A)
    ok = store.set_node_diagnostics(
        A, partition_id=1234, leader_router_id=0,
        routing_role="leader", active_routers=3, channel=15, weighting=64,
    )
    assert ok is True
    nodes = {n["eui64"]: n for n in store.list_nodes()}
    assert nodes[A]["partition_id"] == 1234
    assert nodes[A]["routing_role"] == "leader"
    assert nodes[A]["channel"] == 15
    assert nodes[A]["diag_updated_at"] is not None


def test_bump_last_referenced_skips_unknown_and_touches_known(
    store: SQLiteStore,
) -> None:
    """Registry-first (v9): ``bump_last_referenced`` is UPDATE-only.

    Unknown EUIs (not in the registry-driven ``nodes`` table) are
    silently skipped; they surface via ``links.neighbor_known = 0``
    instead.
    """
    unknown = "bb" * 8
    known = "cc" * 8
    # Unknown EUI: no row created, count is 0.
    assert store.bump_last_referenced([unknown]) == 0
    assert store.get_node(unknown) is None

    # Known EUI: row touched, count is 1.
    store.upsert_node_metadata(eui64=known)
    assert store.bump_last_referenced([known]) == 1
    node = store.get_node(known)
    assert node is not None
    assert node["last_referenced_at"] is not None


def test_sweep_phantoms_marks_old_and_clears_fresh(store: SQLiteStore) -> None:
    """Removed in v0.9.40: ``sweep_phantoms`` retired with the
    ``is_phantom`` column. ``recompute_node_statuses`` covers the same
    transition (see ``test_recompute_node_statuses_state_machine``).
    """
    return


def test_purge_phantom_nodes_removes_links(store: SQLiteStore) -> None:
    A = "ee" * 8
    B = "ff" * 8
    store.upsert_node_metadata(eui64=A)
    store.upsert_node_metadata(eui64=B)
    store.bump_last_referenced([A, B])
    store.replace_links_for_reporter(A, "neighbor_table", [
        {"neighbor_eui64": B, "rssi_avg": -55, "is_child": True},
    ])
    # Mark A as phantom via stale ts + recompute.
    stale = (datetime.now(tz=UTC) - timedelta(hours=48)).isoformat()
    with store._tx() as conn:  # noqa: SLF001
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?", (stale, A))
    store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)
    result = store.purge_phantom_nodes()
    assert result["deleted_nodes"] >= 1
    assert result["deleted_links"] >= 1
    assert store.get_node(A) is None


def test_reset_data_wipes_cache_tables_preserves_schema(store: SQLiteStore) -> None:
    A = "aa" * 8
    B = "bb" * 8
    # Seed some state across the cache tables.
    store.upsert_node_metadata(eui64=A)
    store.upsert_node_metadata(eui64=B)
    store.insert_event(eui64=A, type="attach", rssi=-50)
    store.bump_last_referenced([A, B])
    store.replace_links_for_reporter(A, "neighbor_table", [
        {"neighbor_eui64": B, "rssi_avg": -55, "is_child": True},
    ])
    store.open_issue(kind="weak_link", severity="warn", eui64=A)
    assert store.stats()["row_counts"]["nodes"] >= 1

    deleted = store.reset_data()
    assert deleted >= 1

    counts = store.stats()["row_counts"]
    assert counts["nodes"] == 0
    assert counts["links"] == 0
    assert counts["events"] == 0
    assert counts["issues"] == 0
    # Schema migrations still recorded.
    assert store.schema_version == 22

def test_upsert_node_metadata_persists_ha_fields(store: SQLiteStore) -> None:
    eui = "cc" * 8
    store.upsert_node_metadata(
        eui64=eui,
        friendly_name="Kitchen Plug",
        device_id="abc123",
        area_id="kitchen",
        area_name="Kitchen",
        manufacturer="Eve",
        model="Energy",
        sw_version="2.1.0",
        hw_version="1",
        ha_device_path="/config/devices/device/abc123",
    )
    node = store.get_node(eui)
    assert node is not None
    assert node["friendly_name"] == "Kitchen Plug"
    assert node["area_id"] == "kitchen"
    assert node["area_name"] == "Kitchen"
    # Legacy `area` mirrors area_name for backwards compatibility.
    assert node["area"] == "Kitchen"
    assert node["manufacturer"] == "Eve"
    assert node["model"] == "Energy"
    assert node["sw_version"] == "2.1.0"
    assert node["hw_version"] == "1"
    assert node["ha_device_path"] == "/config/devices/device/abc123"

    # COALESCE semantics: partial update must not wipe existing fields.
    store.upsert_node_metadata(eui64=eui, friendly_name="Kitchen Plug v2")
    node2 = store.get_node(eui)
    assert node2["friendly_name"] == "Kitchen Plug v2"
    assert node2["manufacturer"] == "Eve"
    assert node2["area_name"] == "Kitchen"


def test_sweep_stale_links_deletes_old_rows(store: SQLiteStore) -> None:
    A = "11" * 8
    B = "22" * 8
    store.replace_links_for_reporter(A, "neighbor_table", [
        {"neighbor_eui64": B, "rssi_avg": -60, "is_child": False},
    ])
    # Force the observed_at into the past.
    stale = (datetime.now(tz=UTC) - timedelta(seconds=3600)).isoformat()
    with store._tx() as conn:  # noqa: SLF001
        conn.execute("UPDATE links SET observed_at = ?", (stale,))

    # TTL too generous: nothing should be evicted.
    assert store.sweep_stale_links(ttl_seconds=7200) == 0
    assert len(store.list_links()) == 1

    # TTL tight: row evicted.
    assert store.sweep_stale_links(ttl_seconds=900) == 1
    assert store.list_links() == []


def test_recompute_node_statuses_state_machine(store: SQLiteStore) -> None:
    fresh = "aa" * 8       # online: referenced now, registered
    stale = "bb" * 8       # offline: referenced 1h ago, registered
    dead = "cc" * 8        # phantom: referenced 48h ago, no device_id
    unreg = "dd" * 8       # unregistered: never referenced, no device_id
    registered_old = "ee" * 8  # offline: registered, last ref 48h ago (never goes phantom)

    # Registered nodes (have device_id).
    store.upsert_node_metadata(eui64=fresh, friendly_name="Fresh", device_id="d1")
    store.upsert_node_metadata(eui64=stale, friendly_name="Stale", device_id="d2")
    store.upsert_node_metadata(eui64=registered_old, friendly_name="Old", device_id="d3")
    # Mesh-only nodes (no device_id). Registry-first (v9): bump is now
    # UPDATE-only, so the rows must be created explicitly first.
    store.upsert_node_metadata(eui64=dead)
    store.upsert_node_metadata(eui64=unreg)
    store.bump_last_referenced([dead, unreg])
    # Clear unreg's last_referenced_at so it really has none.
    with store._tx() as conn:  # noqa: SLF001
        conn.execute("UPDATE nodes SET last_referenced_at = NULL WHERE eui64 = ?", (unreg,))

    now = datetime.now(tz=UTC)
    fresh_ts = now.isoformat()
    stale_ts = (now - timedelta(hours=1)).isoformat()
    dead_ts = (now - timedelta(hours=48)).isoformat()
    with store._tx() as conn:  # noqa: SLF001
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?", (fresh_ts, fresh))
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?", (stale_ts, stale))
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?", (dead_ts, dead))
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?", (dead_ts, registered_old))

    summary = store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)
    assert summary["online"] == 1
    # stale + registered_old are both offline (registered, recent-ish or old).
    assert summary["offline"] == 2
    assert summary["unregistered"] == 1
    assert summary["phantom"] == 1

    assert store.get_node(fresh)["status"] == "online"
    assert store.get_node(stale)["status"] == "offline"
    assert store.get_node(registered_old)["status"] == "offline"  # protected
    assert store.get_node(unreg)["status"] == "unregistered"
    assert store.get_node(dead)["status"] == "phantom"


def test_apply_availability_updates_columns(store: SQLiteStore) -> None:
    """v11: apply_availability stamps available/source/checked_at and is
    UPDATE-only (skips unknown EUIs to preserve registry-first contract).
    """
    known = "aa" * 8
    unknown = "ff" * 8
    store.upsert_node_metadata(eui64=known, friendly_name="K", device_id="d1")

    result = store.apply_availability([
        (known, True, "ha_entity"),
        (unknown, False, "ha_entity"),  # not in nodes → skipped
        ("", True, "ha_entity"),         # empty → ignored
    ])
    assert result == {"applied": 1, "skipped": 1}

    row = store.get_node(known)
    assert row["available"] == 1
    assert row["availability_source"] == "ha_entity"
    assert row["availability_checked_at"] is not None

    # Flip to unavailable.
    store.apply_availability([(known, False, "ha_entity")])
    assert store.get_node(known)["available"] == 0

    # None preserves the source but clears availability (no data).
    store.apply_availability([(known, None, "ha_entity")])
    assert store.get_node(known)["available"] is None


def test_recompute_node_statuses_availability_first(store: SQLiteStore) -> None:
    """v11: HA entity availability is the primary online/offline signal.

    Mesh-side ``last_referenced_at`` is the fallback only when availability
    has never been probed (``available IS NULL``).
    """
    online_via_ha = "01" * 8     # available=1 → online (even with stale mesh ref)
    offline_via_ha = "02" * 8    # available=0, registered → offline
    null_avail_fresh = "03" * 8  # available=NULL, recent mesh ref → online (fallback)
    null_avail_stale = "04" * 8  # available=NULL, stale mesh ref, registered → offline
    mesh_only_phantom = "05" * 8 # no device_id, very stale → phantom

    store.upsert_node_metadata(eui64=online_via_ha, friendly_name="A", device_id="d1")
    store.upsert_node_metadata(eui64=offline_via_ha, friendly_name="B", device_id="d2")
    store.upsert_node_metadata(eui64=null_avail_fresh, friendly_name="C", device_id="d3")
    store.upsert_node_metadata(eui64=null_avail_stale, friendly_name="D", device_id="d4")
    store.upsert_node_metadata(eui64=mesh_only_phantom)

    # Apply availability for first two; leave the rest NULL.
    store.apply_availability([
        (online_via_ha, True, "ha_entity"),
        (offline_via_ha, False, "ha_entity"),
    ])

    # Set last_referenced_at: online_via_ha is *stale* mesh-wise (proves HA
    # availability wins over mesh recency); null_avail_fresh is fresh.
    now = datetime.now(tz=UTC)
    fresh_ts = now.isoformat()
    stale_ts = (now - timedelta(hours=2)).isoformat()
    ancient_ts = (now - timedelta(hours=48)).isoformat()
    with store._tx() as conn:  # noqa: SLF001
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?",
                     (stale_ts, online_via_ha))
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?",
                     (fresh_ts, offline_via_ha))
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?",
                     (fresh_ts, null_avail_fresh))
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?",
                     (stale_ts, null_avail_stale))
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?",
                     (ancient_ts, mesh_only_phantom))

    store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)

    # HA availability dominates mesh recency in both directions.
    assert store.get_node(online_via_ha)["status"] == "online"
    assert store.get_node(offline_via_ha)["status"] == "offline"
    # Fallback path when available IS NULL.
    assert store.get_node(null_avail_fresh)["status"] == "online"
    assert store.get_node(null_avail_stale)["status"] == "offline"
    # Mesh-only stays subject to phantom sweep.
    assert store.get_node(mesh_only_phantom)["status"] == "phantom"


def test_purge_expired_nodes_preserves_ha_registered(store: SQLiteStore) -> None:
    keep = "11" * 8
    purge = "22" * 8
    store.upsert_node_metadata(eui64=keep, friendly_name="Keep", device_id="x")
    # Registry-first (v9): seed the mesh-only row explicitly; bump no
    # longer auto-creates unknown EUIs.
    store.upsert_node_metadata(eui64=purge)
    store.bump_last_referenced([purge])
    very_old = (datetime.now(tz=UTC) - timedelta(days=90)).isoformat()
    with store._tx() as conn:  # noqa: SLF001
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?", (very_old, keep))
        conn.execute("UPDATE nodes SET last_referenced_at = ? WHERE eui64 = ?", (very_old, purge))
    store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)
    # `keep` is HA-registered: offline forever. `purge` is mesh-only: phantom.
    assert store.get_node(keep)["status"] == "offline"
    assert store.get_node(purge)["status"] == "phantom"

    result = store.purge_expired_nodes(max_offline_seconds=30 * 86400)
    assert result["deleted_nodes"] == 1
    assert purge in result["euis"]
    # HA-registered preserved.
    assert store.get_node(keep) is not None
    assert store.get_node(purge) is None


# ---------------------------------------------------------------------------
# v10: role-count counters and network_data
# ---------------------------------------------------------------------------

def test_set_node_diagnostics_role_counts(store: SQLiteStore) -> None:
    """v10: cluster-53 stability counters land on the node row."""
    A = "aa" * 8
    store.upsert_node_metadata(eui64=A)
    ok = store.set_node_diagnostics(
        A,
        partition_id=42,
        routing_role="router",
        detached_role_count=2,
        router_role_count=5,
        leader_role_count=0,
        attach_attempt_count=3,
        parent_change_count=1,
    )
    assert ok is True
    n = store.get_node(A)
    assert n is not None
    assert n["detached_role_count"] == 2
    assert n["router_role_count"] == 5
    assert n["leader_role_count"] == 0
    assert n["attach_attempt_count"] == 3
    assert n["parent_change_count"] == 1


def test_upsert_network_data_roundtrip(store: SQLiteStore) -> None:
    """v10: network_data persists JSON columns and lists newest-first."""
    store.upsert_network_data(
        partition_id=1111,
        otbr_eui64="ff" * 8,
        pan_id="0x1234",
        extended_pan_id="dead00beef00cafe",
        network_name="MyMesh",
        channel=15,
        channel_mask="0x07fff800",
        mesh_local_prefix="fd00:db8::/64",
        on_mesh_prefixes=[{"prefix": "fd11::/64", "preferred": True}],
        external_routes=[{"prefix": "::/0"}],
        services=[],
        br_servers=[{"rloc16": "0x0400"}],
        active_timestamp="1",
    )
    got = store.get_network_data(1111)
    assert got is not None
    assert got["pan_id"] == "0x1234"
    assert got["network_name"] == "MyMesh"
    assert got["channel"] == 15
    assert got["on_mesh_prefixes"] == [{"prefix": "fd11::/64", "preferred": True}]
    assert got["br_servers"] == [{"rloc16": "0x0400"}]
    rows = store.list_network_data()
    assert len(rows) == 1 and rows[0]["partition_id"] == 1111

    # Second partition (split-brain detection surface).
    store.upsert_network_data(partition_id=2222, otbr_eui64="ee" * 8, network_name="Other")
    rows = store.list_network_data()
    assert {r["partition_id"] for r in rows} == {1111, 2222}


def test_list_children_filters_neighbors(store: SQLiteStore) -> None:
    """``links.is_child`` lets us split the NeighborTable into child/peer."""
    from thread_observability.pipeline.routing import list_children_enriched

    parent = "aa" * 8
    child = "bb" * 8
    peer = "cc" * 8
    store.upsert_node_metadata(eui64=parent, friendly_name="Parent")
    store.upsert_node_metadata(eui64=child, friendly_name="Sleepy Sensor")
    store.upsert_node_metadata(eui64=peer, friendly_name="Router Peer")

    store.replace_links_for_reporter(parent, "neighbor_table", [
        {"neighbor_eui64": child, "is_child": 1, "rx_on_when_idle": 0,
         "lqi_in": 200, "rssi_avg": -55},
        {"neighbor_eui64": peer, "is_child": 0, "rx_on_when_idle": 1,
         "lqi_in": 240, "rssi_avg": -40},
    ])

    out = list_children_enriched(parent, store=store)
    assert out["parent_eui64"] == parent
    assert out["child_count"] == 1
    assert out["children"][0]["eui64"] == child
    assert out["children"][0]["rx_on_when_idle"] == 0
    assert out["is_at_capacity"] is False


# ---------------------------------------------------------------------------
# v0.9.41: status_change event emission + flap history
# ---------------------------------------------------------------------------


def test_recompute_node_statuses_emits_status_change_events(store: SQLiteStore) -> None:
    """v0.9.41: every status transition is persisted to the events table.

    The first recompute populates baseline state (NULL -> default 'online')
    which we treat as a transition; flipping availability should then emit
    an additional online->offline row in events.
    """
    eui = "aa" * 8
    store.upsert_node_metadata(eui64=eui, friendly_name="A", device_id="d1")
    store.apply_availability([(eui, True, "ha_entity")])

    s1 = store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)
    # No availability flip yet � only the implicit DEFAULT 'online' -> 'online'
    # path runs, which is a no-op (old == new), so no event row.
    assert s1["transitions"] == 0

    # Now flip to unavailable; recompute should write one status_change row.
    store.apply_availability([(eui, False, "ha_entity")])
    s2 = store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)
    assert s2["transitions"] == 1
    assert s2["changed"] == 1

    events = store.query_events(eui64=eui, event_type="status_change")
    assert len(events) == 1
    payload = events[0]["payload"]
    assert payload["from"] == "online"
    assert payload["to"] == "offline"


def test_get_node_flap_history_aggregates_counts(store: SQLiteStore) -> None:
    """v0.9.41: get_node_flap_history returns transitions + per-EUI counts."""
    a = "aa" * 8
    b = "bb" * 8
    store.upsert_node_metadata(eui64=a, device_id="d1")
    store.upsert_node_metadata(eui64=b, device_id="d2")

    # Drive a few transitions through availability flips.
    store.apply_availability([(a, True, "ha_entity"), (b, True, "ha_entity")])
    store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)
    store.apply_availability([(a, False, "ha_entity")])
    store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)
    store.apply_availability([(a, True, "ha_entity"), (b, False, "ha_entity")])
    store.recompute_node_statuses(offline_seconds=900, phantom_seconds=24 * 3600)

    hist = store.get_node_flap_history(limit=50)
    assert hist["count"] == 3
    counts = hist["flap_counts"]
    assert counts[a]["total"] == 2
    assert counts[a]["by_transition"]["online->offline"] == 1
    assert counts[a]["by_transition"]["offline->online"] == 1
    assert counts[b]["total"] == 1
    assert counts[b]["by_transition"]["online->offline"] == 1

    # eui64 filter restricts to a single device.
    only_a = store.get_node_flap_history(eui64=a, limit=50)
    assert only_a["count"] == 2
    assert set(only_a["flap_counts"].keys()) == {a}


# ---------------------------------------------------------------------------
# v0.9.42: link flap events + MAC error counters + diff-returning links
# ---------------------------------------------------------------------------


def test_replace_links_returns_diff(store: SQLiteStore) -> None:
    """v0.9.42: replace_links_for_reporter returns added/removed neighbor sets."""
    reporter = "aa" * 8
    n1 = "11" * 8
    n2 = "22" * 8
    n3 = "33" * 8
    for eui in (reporter, n1, n2, n3):
        store.upsert_node_metadata(eui64=eui)

    # First call � nothing prior, everything is "added".
    r1 = store.replace_links_for_reporter(
        reporter, "neighbor_table",
        [{"neighbor_eui64": n1}, {"neighbor_eui64": n2}],
    )
    assert r1["inserted"] == 2
    assert sorted(r1["added"]) == sorted([n1, n2])
    assert r1["removed"] == []

    # Swap n2 -> n3: one added, one removed.
    r2 = store.replace_links_for_reporter(
        reporter, "neighbor_table",
        [{"neighbor_eui64": n1}, {"neighbor_eui64": n3}],
    )
    assert r2["inserted"] == 2
    assert r2["added"] == [n3]
    assert r2["removed"] == [n2]

    # Identical set: no diff.
    r3 = store.replace_links_for_reporter(
        reporter, "neighbor_table",
        [{"neighbor_eui64": n1}, {"neighbor_eui64": n3}],
    )
    assert r3["added"] == []
    assert r3["removed"] == []


def test_set_node_diagnostics_persists_v13_counters(store: SQLiteStore) -> None:
    """v0.9.42: MAC error counters and partition-stability counters round-trip."""
    eui = "ab" * 8
    store.upsert_node_metadata(eui64=eui, device_id="d1")
    ok = store.set_node_diagnostics(
        eui,
        tx_total_count=10_000,
        tx_retry_count=42,
        tx_err_cca_count=3,
        tx_err_abort_count=1,
        tx_err_busy_channel_count=2,
        rx_total_count=9_500,
        rx_duplicated_count=12,
        rx_err_no_frame_count=4,
        rx_err_sec_count=0,
        rx_err_fcs_count=7,
        partition_id_change_count=5,
        better_partition_attach_attempt_count=8,
    )
    assert ok is True
    node = store.get_node(eui)
    assert node["tx_total_count"] == 10_000
    assert node["tx_retry_count"] == 42
    assert node["tx_err_cca_count"] == 3
    assert node["tx_err_abort_count"] == 1
    assert node["tx_err_busy_channel_count"] == 2
    assert node["rx_total_count"] == 9_500
    assert node["rx_duplicated_count"] == 12
    assert node["rx_err_no_frame_count"] == 4
    assert node["rx_err_sec_count"] == 0
    assert node["rx_err_fcs_count"] == 7
    assert node["partition_id_change_count"] == 5
    assert node["better_partition_attach_attempt_count"] == 8


def test_get_link_flap_history_aggregates_pairs(store: SQLiteStore) -> None:
    """v0.9.42: get_link_flap_history buckets events by sorted (reporter, neighbor) pair.

    Both directions of an edge land in the same bucket so a single flap
    surfaces once even if both endpoints reported it.
    """
    a = "aa" * 8
    b = "bb" * 8
    c = "cc" * 8
    for eui in (a, b, c):
        store.upsert_node_metadata(eui64=eui)

    # a sees b appear, then disappear, then re-appear.
    store.insert_event(
        eui64=a, type="link_acquired",
        payload={"reporter_eui64": a, "neighbor_eui64": b,
                 "source": "neighbor_table", "partition_id": 1},
    )
    store.insert_event(
        eui64=a, type="link_lost",
        payload={"reporter_eui64": a, "neighbor_eui64": b,
                 "source": "neighbor_table", "partition_id": 1},
    )
    # b also reports the same edge � same bucket.
    store.insert_event(
        eui64=b, type="link_acquired",
        payload={"reporter_eui64": b, "neighbor_eui64": a,
                 "source": "neighbor_table", "partition_id": 1},
    )
    # An unrelated edge a<->c.
    store.insert_event(
        eui64=a, type="link_acquired",
        payload={"reporter_eui64": a, "neighbor_eui64": c,
                 "source": "route_table", "partition_id": 1},
    )

    hist = store.get_link_flap_history(limit=50)
    assert hist["count"] == 4
    counts = hist["flap_counts"]
    # Pair key is sorted "neighbor|reporter".
    ab_key = "|".join(sorted([a, b]))
    ac_key = "|".join(sorted([a, c]))
    assert counts[ab_key]["total"] == 3
    assert counts[ab_key]["acquired"] == 2
    assert counts[ab_key]["lost"] == 1
    assert counts[ac_key]["total"] == 1
    assert counts[ac_key]["acquired"] == 1

    # source filter
    only_neighbor = store.get_link_flap_history(source="neighbor_table", limit=50)
    assert only_neighbor["count"] == 3
    # neighbor_eui64 filter
    only_c = store.get_link_flap_history(neighbor_eui64=c, limit=50)
    assert only_c["count"] == 1


# ---------------------------------------------------------------------------
# v0.9.43 — Tier 2 storage-layer additions
# ---------------------------------------------------------------------------


def test_replace_links_returns_prior_frame_counters(store: SQLiteStore) -> None:
    """Storage transactionally snapshots prior frame counters on replace.

    Without this guarantee, a concurrent sweep could DELETE the prior
    row before the consumer can compare counters, making
    ``re_attached_node`` detection unreliable.
    """
    reporter = "aa" * 8
    neighbor = "bb" * 8
    store.upsert_node_metadata(eui64=reporter, friendly_name="Reporter")
    store.upsert_node_metadata(eui64=neighbor, friendly_name="Neighbor")

    first = store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [{
            "neighbor_eui64": neighbor,
            "link_frame_counter": 100,
            "mle_frame_counter": 50,
        }],
        partition_id=1,
    )
    # First call has empty prior; the dict key still exists.
    assert "prior_frame_counters" in first
    assert first["prior_frame_counters"] == {}

    second = store.replace_links_for_reporter(
        reporter,
        "neighbor_table",
        [{
            "neighbor_eui64": neighbor,
            "link_frame_counter": 200,
            "mle_frame_counter": 75,
        }],
        partition_id=1,
    )
    assert neighbor in second["prior_frame_counters"]
    assert second["prior_frame_counters"][neighbor] == {
        "link_frame_counter": 100,
        "mle_frame_counter": 50,
    }


def test_set_node_router_id_returns_diff_and_derives_rloc16(store: SQLiteStore) -> None:
    eui = "cc" * 8
    store.upsert_node_metadata(eui64=eui, friendly_name="Router")

    first = store.set_node_router_id(eui, 5)
    assert first["updated"] is True
    assert first["old_router_id"] is None
    assert first["new_router_id"] == 5
    assert first["old_rloc16"] is None
    assert first["new_rloc16"] == 5 << 10  # = 5120

    second = store.set_node_router_id(eui, 9)
    assert second["old_router_id"] == 5
    assert second["new_router_id"] == 9
    assert second["old_rloc16"] == 5 << 10
    assert second["new_rloc16"] == 9 << 10

    cleared = store.set_node_router_id(eui, None)
    assert cleared["new_router_id"] is None
    assert cleared["new_rloc16"] is None


def test_otbr_diagnostics_insert_and_fetch_latest(store: SQLiteStore) -> None:
    target = "dd" * 8
    store.upsert_node_metadata(eui64=target, friendly_name="Target")

    row_id = store.insert_otbr_diagnostic(
        target_eui64=target,
        target_rloc16=0x4400,
        partition_id=42,
        mac_tx_total=1000,
        mac_tx_retry=10,
        mac_tx_err=2,
        mac_rx_total=900,
        mac_rx_err=1,
        mac_rx_dup=3,
        child_table=[{"child_id": 1}],
        extra={"raw": "tlv"},
    )
    assert row_id > 0

    latest = store.get_latest_otbr_diagnostic(target)
    assert latest is not None
    assert latest["target_rloc16"] == 0x4400
    assert latest["mac_tx_total"] == 1000
    assert latest["child_table"] == [{"child_id": 1}]
    assert latest["extra"] == {"raw": "tlv"}

    # Second insert returns the newer row from get_latest_*.
    store.insert_otbr_diagnostic(
        target_eui64=target,
        target_rloc16=0x4400,
        mac_tx_total=1500,
    )
    latest2 = store.get_latest_otbr_diagnostic(target)
    assert latest2 is not None
    assert latest2["mac_tx_total"] == 1500


# ---------------------------------------------------------------------------
# v0.9.44 — Tier 3 observer_events
# ---------------------------------------------------------------------------


def test_observer_event_insert_close_and_overlap(store: SQLiteStore) -> None:
    """Insert an open window, close it, and confirm overlap query semantics."""
    base = datetime(2026, 5, 12, 14, 0, 0, tzinfo=UTC)
    started = base.isoformat()
    ev_id = store.insert_observer_event(
        source="addon:core_matter_server",
        kind="outage",
        started_at=started,
        details={"prev_state": "started", "new_state": "stopped"},
    )
    assert ev_id > 0

    latest = store.get_latest_observer_event("addon:core_matter_server")
    assert latest is not None
    assert latest["kind"] == "outage"
    assert latest["ended_at"] is None
    assert latest["details"] == {"prev_state": "started", "new_state": "stopped"}

    # Overlap query: window that fully contains the event start.
    overlaps = store.list_observer_events_in_window(
        since=(base - timedelta(minutes=5)).isoformat(),
        until=(base + timedelta(minutes=5)).isoformat(),
    )
    assert any(e["id"] == ev_id for e in overlaps)

    # Window strictly before the event — should NOT overlap.
    none_overlaps = store.list_observer_events_in_window(
        since=(base - timedelta(hours=2)).isoformat(),
        until=(base - timedelta(hours=1)).isoformat(),
    )
    assert all(e["id"] != ev_id for e in none_overlaps)

    # Close the event and re-query: a window AFTER ended_at should not overlap.
    ended = (base + timedelta(seconds=30)).isoformat()
    assert store.close_observer_event(ev_id, ended_at=ended) is True
    closed = store.get_latest_observer_event("addon:core_matter_server")
    assert closed is not None
    assert closed["ended_at"] == ended

    after = store.list_observer_events_in_window(
        since=(base + timedelta(minutes=10)).isoformat(),
        until=(base + timedelta(minutes=20)).isoformat(),
    )
    assert all(e["id"] != ev_id for e in after)


def test_observer_event_close_is_idempotent(store: SQLiteStore) -> None:
    ev_id = store.insert_observer_event(source="addon:self", kind="start")
    assert store.close_observer_event(ev_id) is True
    # Second close on the same id is a no-op (already-closed events
    # are skipped by the WHERE clause).
    assert store.close_observer_event(ev_id) is False



