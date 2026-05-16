"""Diagnostics payload shaping for the HTTP API.

The FastAPI route layer should stay focused on wiring and transport concerns.
Diagnostics shaping is extracted here so it can be reasoned about and tested
independently from the application factory.
"""

from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from ..storage.sqlite_store import get_store

_CONFIG_SECRET_KEYS = frozenset({"token", "api_key", "ha_admin_token"})


def redact_config_secrets(value: object) -> object:
    if isinstance(value, dict):
        redacted: dict[str, object] = {}
        for key, item in value.items():
            if key in _CONFIG_SECRET_KEYS and item:
                redacted[key] = "***"
            else:
                redacted[key] = redact_config_secrets(item)
        return redacted
    if isinstance(value, list):
        return [redact_config_secrets(item) for item in value]
    return value


def build_storage_capacity(storage: dict[str, object]) -> dict[str, object]:
    size_bytes = int(storage.get("size_bytes") or 0)
    db_path = Path(str(storage.get("db_path") or "")).expanduser()
    free_bytes: int | None = None
    total_bytes: int | None = None
    warning_free_bytes = max(size_bytes * 10, 1024 * 1024 * 1024)
    critical_free_bytes = max(size_bytes * 3, 256 * 1024 * 1024)
    growth_rate_bytes_per_day: float | None = None
    risk = "unknown"
    note = "disk capacity could not be determined"
    try:
        recent_ticks = get_store().get_recent_pipeline_ticks(limit=48)
    except Exception:
        recent_ticks = []
    sized_ticks = [
        row
        for row in reversed(recent_ticks)
        if row.get("db_size_bytes") is not None and row.get("completed_at")
    ]
    if len(sized_ticks) >= 2:
        first = sized_ticks[0]
        last = sized_ticks[-1]
        try:
            started = datetime.fromisoformat(str(first["completed_at"]))
            ended = datetime.fromisoformat(str(last["completed_at"]))
            seconds = max((ended - started).total_seconds(), 1.0)
            growth_rate_bytes_per_day = (
                (float(last["db_size_bytes"]) - float(first["db_size_bytes"])) / seconds
            ) * 86400.0
        except Exception:
            growth_rate_bytes_per_day = None
    if db_path:
        try:
            usage = shutil.disk_usage(db_path.parent)
            free_bytes = int(usage.free)
            total_bytes = int(usage.total)
        except OSError:
            free_bytes = None
            total_bytes = None
    if free_bytes is not None and total_bytes:
        db_fraction = (size_bytes / total_bytes) if total_bytes > 0 else 0.0
        if free_bytes < critical_free_bytes:
            risk = "high"
            note = "free space is low relative to the current SQLite size"
        elif free_bytes < warning_free_bytes:
            risk = "medium"
            note = "SQLite is healthy now, but capacity headroom is getting tighter"
        else:
            risk = "low"
            note = "SQLite has comfortable free-space headroom"
        return {
            "size_bytes": size_bytes,
            "free_bytes": free_bytes,
            "total_bytes": total_bytes,
            "db_fraction": round(db_fraction, 6),
            "warning_free_bytes": warning_free_bytes,
            "critical_free_bytes": critical_free_bytes,
            "growth_rate_bytes_per_day": growth_rate_bytes_per_day,
            "risk": risk,
            "note": note,
        }
    return {
        "size_bytes": size_bytes,
        "free_bytes": free_bytes,
        "total_bytes": total_bytes,
        "db_fraction": None,
        "warning_free_bytes": warning_free_bytes,
        "critical_free_bytes": critical_free_bytes,
        "growth_rate_bytes_per_day": growth_rate_bytes_per_day,
        "risk": risk,
        "note": note,
    }


def build_diagnostics_summary(
    *,
    supervisor: dict[str, object],
    storage: dict[str, object],
    timeseries: dict[str, object],
    ingestion: dict[str, object],
    pipeline: dict[str, object],
    health: dict[str, object],
    partitions: dict[str, object],
    phantoms: dict[str, object],
    stale_link_count: int,
    config: dict[str, object],
    graph_diagnostics: list[dict[str, object]],
) -> dict[str, object]:
    health_summary = health.get("summary") if isinstance(health, dict) else {}
    issue_summary = health.get("active_issues") if isinstance(health, dict) else {}
    stages_failed = (
        list(pipeline.get("stages_failed") or []) if isinstance(pipeline, dict) else []
    )
    assessment_cfg = config.get("assessment") if isinstance(config, dict) else {}
    storage_capacity = build_storage_capacity(storage)
    ingestion_error = (
        str(ingestion.get("error") or "").strip() if isinstance(ingestion, dict) else ""
    )
    ingestion_slug = (
        str(ingestion.get("slug") or "").strip() if isinstance(ingestion, dict) else ""
    )
    sources = {
        "supervisor": {
            "status": "error" if supervisor.get("error") else "ok",
            "detail": str(supervisor.get("error") or "reachable via Supervisor API"),
        },
        "pipeline": {
            "status": "error"
            if stages_failed
            else ("running" if pipeline.get("running") else "ok"),
            "detail": (
                f"failed stages: {', '.join(stages_failed)}"
                if stages_failed
                else (
                    f"tick #{pipeline.get('tick_count') or 0} in progress"
                    if pipeline.get("running")
                    else f"last tick #{pipeline.get('tick_count') or 0} completed"
                )
            ),
            "failed_stages": stages_failed,
            "last_finished_at": pipeline.get("finished_at"),
        },
        "otbr_ingestion": {
            "status": "error" if ingestion_error else ("warn" if not ingestion_slug else "ok"),
            "detail": ingestion_error
            or (
                "no OTBR add-on selected for log ingestion"
                if not ingestion_slug
                else "OTBR ingest state available"
            ),
            "last_run_at": ingestion.get("last_run_at") if isinstance(ingestion, dict) else None,
        },
        "timeseries": {
            "status": "ok" if timeseries.get("ok") else "warn",
            "detail": str(timeseries.get("error") or timeseries.get("backend") or "unknown backend"),
            "backend": timeseries.get("backend"),
        },
        "assessment": {
            "status": "ok" if assessment_cfg.get("enabled") else "warn",
            "detail": "Adaptive Monitoring enabled"
            if assessment_cfg.get("enabled")
            else "Adaptive Monitoring disabled",
        },
    }
    data_quality = {
        "status": health.get("status") if isinstance(health, dict) else "unknown",
        "data_age_seconds": health.get("data_age_seconds") if isinstance(health, dict) else None,
        "stale_nodes": int((health_summary or {}).get("stale_nodes") or 0),
        "offline_nodes": int((health_summary or {}).get("offline_nodes") or 0),
        "duplicate_physical_device_groups": int(
            (health_summary or {}).get("duplicate_physical_device_groups") or 0
        ),
        "distinct_thread_networks": int((health_summary or {}).get("distinct_thread_networks") or 0),
        "active_issue_count": int((issue_summary or {}).get("count") or 0),
        "partition_count": int(partitions.get("partition_count") or 0)
        if isinstance(partitions, dict)
        else 0,
        "phantom_count": int(phantoms.get("count") or 0) if isinstance(phantoms, dict) else 0,
        "stale_link_count": int(stale_link_count or 0),
    }
    attention_items: list[dict[str, str]] = []
    if stages_failed:
        attention_items.append(
            {
                "severity": "bad",
                "title": "Pipeline stages are failing",
                "detail": f"Failed stages: {', '.join(stages_failed)}",
            }
        )
    if data_quality["distinct_thread_networks"] > 1:
        attention_items.append(
            {
                "severity": "warn",
                "title": "Multiple Thread networks detected",
                "detail": f"{data_quality['distinct_thread_networks']} distinct Thread networks are present in current node data.",
            }
        )
    if data_quality["duplicate_physical_device_groups"] > 0:
        attention_items.append(
            {
                "severity": "warn",
                "title": "Duplicate hardware identities need cleanup",
                "detail": f"{data_quality['duplicate_physical_device_groups']} duplicate device groups remain in the mesh inventory.",
            }
        )
    if data_quality["offline_nodes"] > 0 or data_quality["stale_nodes"] > 0:
        attention_items.append(
            {
                "severity": "warn",
                "title": "Node freshness is degraded",
                "detail": f"{data_quality['offline_nodes']} offline and {data_quality['stale_nodes']} stale nodes are currently reported.",
            }
        )
    if storage_capacity["risk"] in {"medium", "high"}:
        attention_items.append(
            {
                "severity": "warn" if storage_capacity["risk"] == "medium" else "bad",
                "title": "SQLite capacity headroom is tightening",
                "detail": str(storage_capacity["note"]),
            }
        )
    for fact in graph_diagnostics[:2]:
        attention_items.append(
            {
                "severity": str(fact.get("severity") or "warn"),
                "title": str(fact.get("title") or "Graph-derived concern"),
                "detail": str(fact.get("detail") or ""),
            }
        )
    if not attention_items:
        attention_items.append(
            {
                "severity": "good",
                "title": "No urgent observability blockers detected",
                "detail": "Current sources and retained mesh data look healthy enough for normal troubleshooting.",
            }
        )
    return {
        "sources": sources,
        "data_quality": data_quality,
        "storage_capacity": storage_capacity,
        "attention_items": attention_items,
        "graph_diagnostics": graph_diagnostics,
    }

