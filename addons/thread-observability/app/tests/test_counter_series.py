"""Phase 4 (0.10.0) counter time-series tools and storage."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from thread_observability.api import counter_series
from thread_observability.api import mcp_tools


# --- schema migration --------------------------------------------------------

def test_schema_v19_creates_node_counter_samples_table(store):
    assert store.schema_version >= 19
    # Table exists and accepts a row.
    inserted = store.record_counter_sample(
        eui64="AABBCCDDEEFF0000",
        counters={"tx_total_count": 100, "tx_retry_count": 3},
    )
    assert inserted is True
    assert store.count_counter_samples() == 1


def test_record_counter_sample_ignores_duplicate_timestamp(store):
    ts = datetime.now(tz=UTC).isoformat()
    a = store.record_counter_sample(
        eui64="AAAA", counters={"tx_total_count": 10}, observed_at=ts,
    )
    b = store.record_counter_sample(
        eui64="AAAA", counters={"tx_total_count": 999}, observed_at=ts,
    )
    assert a is True
    assert b is False  # INSERT OR IGNORE — same PK rejected
    samples = store.get_counter_samples(eui64="AAAA")
    assert len(samples) == 1
    assert samples[0]["counters"]["tx_total_count"] == 10


def test_record_counter_sample_drops_none_values(store):
    inserted = store.record_counter_sample(
        eui64="BBBB",
        counters={"tx_total_count": 5, "tx_retry_count": None, "rx_total_count": 0},
    )
    assert inserted is True
    sample = store.get_counter_samples(eui64="BBBB")[0]
    assert "tx_retry_count" not in sample["counters"]
    assert sample["counters"]["rx_total_count"] == 0


def test_record_counter_sample_empty_returns_false(store):
    assert store.record_counter_sample(eui64="CCCC", counters={}) is False
    assert store.record_counter_sample(eui64="", counters={"x": 1}) is False
    assert store.record_counter_sample(eui64="CCCC", counters={"x": None}) is False


# --- get_counter_samples ----------------------------------------------------

def test_get_counter_samples_oldest_first_within_window(store):
    base = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    for i in range(5):
        store.record_counter_sample(
            eui64="NODE",
            counters={"tx_total_count": 100 + i * 10},
            observed_at=(base + timedelta(seconds=i * 10)).isoformat(),
        )
    rows = store.get_counter_samples(
        eui64="NODE",
        since=(base + timedelta(seconds=15)).isoformat(),
        until=(base + timedelta(seconds=35)).isoformat(),
    )
    assert [r["counters"]["tx_total_count"] for r in rows] == [120, 130]


# --- get_counter_series tool -------------------------------------------------

def test_get_counter_series_returns_series_and_deltas(store):
    base = datetime.now(tz=UTC) - timedelta(minutes=10)
    store.record_counter_sample(eui64="N1", counters={"tx_total_count": 100, "tx_retry_count": 5}, observed_at=base.isoformat())
    store.record_counter_sample(eui64="N1", counters={"tx_total_count": 110, "tx_retry_count": 6}, observed_at=(base + timedelta(minutes=2)).isoformat())
    store.record_counter_sample(eui64="N1", counters={"tx_total_count": 130, "tx_retry_count": 8}, observed_at=(base + timedelta(minutes=4)).isoformat())

    out = counter_series.get_counter_series(eui64="N1")
    assert out["sample_count"] == 3
    assert len(out["series"]) == 3
    assert out["deltas"]["tx_total_count"]["delta"] == 30
    assert out["deltas"]["tx_total_count"]["reset_detected"] is False
    assert out["deltas"]["tx_retry_count"]["delta"] == 3


def test_get_counter_series_detects_reset(store):
    base = datetime.now(tz=UTC) - timedelta(minutes=10)
    # Reset between two samples: 500 -> 10.
    store.record_counter_sample(eui64="R1", counters={"tx_total_count": 500}, observed_at=base.isoformat())
    store.record_counter_sample(eui64="R1", counters={"tx_total_count": 10}, observed_at=(base + timedelta(minutes=2)).isoformat())
    out = counter_series.get_counter_series(eui64="R1")
    assert out["deltas"]["tx_total_count"]["reset_detected"] is True
    assert out["deltas"]["tx_total_count"]["delta"] is None


def test_get_counter_series_filters_by_counter_names(store):
    base = datetime.now(tz=UTC) - timedelta(minutes=5)
    store.record_counter_sample(eui64="N2", counters={"tx_total_count": 1, "rx_total_count": 9}, observed_at=base.isoformat())
    store.record_counter_sample(eui64="N2", counters={"tx_total_count": 2, "rx_total_count": 18}, observed_at=(base + timedelta(minutes=1)).isoformat())
    out = counter_series.get_counter_series(eui64="N2", counter_names=["tx_total_count"])
    assert all(set(p["counters"]) <= {"tx_total_count"} for p in out["series"])
    assert "rx_total_count" not in out["deltas"]


def test_get_counter_series_5min_resolution_buckets(store):
    base = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    # Three samples within the same 5-min bucket, then one in the next.
    store.record_counter_sample(eui64="B1", counters={"tx_total_count": 10}, observed_at=base.isoformat())
    store.record_counter_sample(eui64="B1", counters={"tx_total_count": 20}, observed_at=(base + timedelta(minutes=1)).isoformat())
    store.record_counter_sample(eui64="B1", counters={"tx_total_count": 30}, observed_at=(base + timedelta(minutes=2)).isoformat())
    store.record_counter_sample(eui64="B1", counters={"tx_total_count": 100}, observed_at=(base + timedelta(minutes=6)).isoformat())
    out = counter_series.get_counter_series(
        eui64="B1",
        since=(base - timedelta(minutes=1)).isoformat(),
        until=(base + timedelta(minutes=10)).isoformat(),
        resolution="5min",
    )
    assert len(out["series"]) == 2
    # First bucket avg = (10+20+30)/3 = 20.0
    assert out["series"][0]["counters"]["tx_total_count"] == 20.0
    assert out["series"][0]["sample_count"] == 3
    assert out["series"][1]["counters"]["tx_total_count"] == 100.0


def test_get_counter_series_requires_eui64(store):  # noqa: ARG001
    out = counter_series.get_counter_series(eui64="")
    assert "error" in out


# --- compare_node_counters --------------------------------------------------

def test_compare_node_counters_flags_disparate_deltas(store):
    base = datetime.now(tz=UTC) - timedelta(minutes=10)
    # Node A: tx_retry climbs from 5 -> 105 (delta 100).
    store.record_counter_sample(eui64="A", counters={"tx_retry_count": 5}, observed_at=base.isoformat())
    store.record_counter_sample(eui64="A", counters={"tx_retry_count": 105}, observed_at=(base + timedelta(minutes=3)).isoformat())
    # Node B: tx_retry climbs from 5 -> 7 (delta 2).
    store.record_counter_sample(eui64="B", counters={"tx_retry_count": 5}, observed_at=base.isoformat())
    store.record_counter_sample(eui64="B", counters={"tx_retry_count": 7}, observed_at=(base + timedelta(minutes=3)).isoformat())
    out = counter_series.compare_node_counters(eui64_a="A", eui64_b="B")
    assert out["peer_summary"]["flagged_count"] == 1
    flagged = out["peer_summary"]["flagged"][0]
    assert flagged["counter"] == "tx_retry_count"
    assert flagged["a_delta"] == 100
    assert flagged["b_delta"] == 2
    assert flagged["ratio"] >= 2


def test_compare_node_counters_requires_both_euis(store):  # noqa: ARG001
    out = counter_series.compare_node_counters(eui64_a="A", eui64_b="")
    assert "error" in out


# --- retention / pruning -----------------------------------------------------

def test_prune_counter_samples_drops_old_and_downsamples_mid(store):
    now = datetime.now(tz=UTC)
    # 1 row older than archive horizon (>14d) → should be deleted.
    store.record_counter_sample(
        eui64="OLD", counters={"tx_total_count": 1},
        observed_at=(now - timedelta(days=20)).isoformat(),
    )
    # 4 rows in the downsample range (between 3d and 14d ago) all in the same
    # 5-min bucket → should collapse to 1 row.
    bucket_base = now - timedelta(days=5)
    bucket_base = bucket_base.replace(minute=10, second=0, microsecond=0)
    for i in range(4):
        store.record_counter_sample(
            eui64="MID", counters={"tx_total_count": 100 + i * 10},
            observed_at=(bucket_base + timedelta(seconds=i * 30)).isoformat(),
        )
    # 1 row in the fresh window (< 3 days) → untouched.
    store.record_counter_sample(
        eui64="NEW", counters={"tx_total_count": 5},
        observed_at=(now - timedelta(hours=1)).isoformat(),
    )

    before = store.count_counter_samples()
    assert before == 6

    res = store.prune_counter_samples(full_resolution_days=3, sampled_archive_days=14)
    assert res["deleted"] == 1   # the >14d row
    assert res["downsampled"] == 4  # the 4 rows in MID bucket collapsed
    # After: 1 fresh + 1 downsampled MID = 2 rows.
    assert store.count_counter_samples() == 2
    assert res["kept"] == 2

    # The collapsed MID row averages 100+110+120+130 = 460/4 = 115.0.
    mid = store.get_counter_samples(eui64="MID")
    assert len(mid) == 1
    assert mid[0]["counters"]["tx_total_count"] == 115.0


# --- mcp catalog -------------------------------------------------------------

def test_phase4_tools_registered():
    names = {t["name"] for t in mcp_tools.TOOL_DEFS}
    assert {"get_counter_series", "compare_node_counters"}.issubset(names)
    assert {"get_counter_series", "compare_node_counters"}.issubset(mcp_tools._READ_TOOLS)
