"""Typed configuration loader.

Home Assistant injects the merged user options at ``/data/options.json``. We
parse it with Pydantic so downstream code gets validated, typed access; we
also expose a few env-var overrides for development outside the Supervisor.
"""

from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

OPTIONS_PATH = Path(os.getenv("THREAD_OBS_OPTIONS_PATH", "/data/options.json"))


class RetentionConfig(BaseModel):
    full_resolution_days: int = Field(default=3, ge=1, le=30)
    sampled_archive_days: int = Field(default=14, ge=1, le=60)


class AIConfig(BaseModel):
    enabled: bool = False
    provider: str = Field(default="local")


class AssessmentConfig(BaseModel):
    """Background Diagnostics (Phase 4, #18-#22) cadence + budget knobs.

    Defaults match documentation/07-agentic-ai-sprint.md §11.1. ``enabled``
    is set at install time via the addon options radio; the runtime switch
    entity flips it without touching options.
    """

    enabled: bool = False
    probation_interval_minutes: int = Field(default=15, ge=1, le=120)
    probation_checks: int = Field(default=3, ge=1, le=10)
    relaxing_initial_hours: int = Field(default=1, ge=1, le=24)
    relaxing_max_hours: int = Field(default=24, ge=1, le=72)
    heightened_initial_minutes: int = Field(default=30, ge=5, le=240)
    heightened_max_hours: int = Field(default=6, ge=1, le=24)
    engaged_interval_minutes: int = Field(default=5, ge=1, le=60)
    engaged_decay_minutes: int = Field(default=60, ge=10, le=720)
    daily_budget_calls: int = Field(default=12, ge=1, le=288)


class SchedulerConfig(BaseModel):
    ingestion_interval_seconds: int = Field(default=10, ge=5, le=60)
    topology_recompute_seconds: int = Field(default=30, ge=10, le=120)
    metadata_refresh_seconds: int = Field(default=900, ge=60, le=3600)
    discover_interval_seconds: int = Field(default=300, ge=60, le=3600)
    reasoner_interval_seconds: int = Field(default=120, ge=30, le=3600)
    otbr_rest_interval_seconds: int = Field(default=60, ge=15, le=3600)
    # Unified pipeline cadence (0.9.32+): a single atomic tick replaces the
    # four independent loops above. Rest-time between ticks, not wall clock.
    pipeline_interval_seconds: int = Field(default=30, ge=10, le=600)


class InfluxConfig(BaseModel):
    """Time-series backend settings.

    ``url`` and ``token`` are typically supplied via environment variables (set
    in the add-on options or by the InfluxDB add-on's service discovery). If
    no token is present we fall back to the SQLite store automatically.
    """

    url: str = Field(default_factory=lambda: os.getenv("INFLUX_URL", ""))
    org: str = Field(default_factory=lambda: os.getenv("INFLUX_ORG", "thread-observability"))
    bucket: str = Field(default_factory=lambda: os.getenv("INFLUX_BUCKET", "thread"))
    token: str = Field(default_factory=lambda: os.getenv("INFLUX_TOKEN", ""))


class ThreadObsConfig(BaseModel):
    """Top-level add-on config."""

    log_level: str = "info"
    timezone: str = "UTC"
    reset_db_on_start: bool = Field(default=True)
    # Optional HA long-lived access token for an admin user. When set,
    # ``ha_update_addon`` (and other privileged self-management tools) can
    # bypass Supervisor's self-update blacklist by calling HA Core's REST
    # API directly under this user identity. Never logged.
    ha_admin_token: str = Field(default="", repr=False)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    ai: AIConfig = Field(default_factory=AIConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    influx: InfluxConfig = Field(default_factory=InfluxConfig)
    assessment: AssessmentConfig = Field(default_factory=AssessmentConfig)
    options_path: str = str(OPTIONS_PATH)
    options_loaded: bool = False

    # v0.9.43 (Tier 2 #1): enable OTBR ``MGMT_DIAG_GET`` second-witness
    # polling. Off by default — the call hits the BR's CoAP path and adds
    # mesh load proportional to router count, so operators should opt in
    # when they want the cross-check.
    enable_otbr_diagnostics: bool = Field(default=False)
    # v0.9.43 (Tier 2 #4 scaffold): enable DBus signal subscription on the
    # Supervisor host for sub-second OTBR partition / role events. Off
    # by default — requires ``host_dbus: true`` in config.yaml and the
    # ``dbus_next`` package, both currently absent, so today this flag
    # only flips logging in the stub module.
    enable_otbr_dbus_push: bool = Field(default=False)

    @classmethod
    def load(cls, path: Path | str | None = None) -> "ThreadObsConfig":
        p = Path(path) if path else OPTIONS_PATH
        if not p.exists():
            log.info("options file %s not present; using defaults", p)
            return cls(options_loaded=False, options_path=str(p))
        try:
            raw = json.loads(p.read_text())
        except Exception as exc:  # noqa: BLE001
            log.warning("failed to parse %s (%s); using defaults", p, exc)
            return cls(options_loaded=False, options_path=str(p))
        # Filter to known keys to keep validation tolerant of new options.
        known = set(cls.model_fields)
        data = {k: v for k, v in raw.items() if k in known}
        cfg = cls(**data)
        cfg.options_loaded = True
        cfg.options_path = str(p)
        return cfg


@lru_cache(maxsize=1)
def get_config() -> ThreadObsConfig:
    """Process-wide cached config. Call ``reload_config`` to refresh."""
    return ThreadObsConfig.load()


def reload_config() -> ThreadObsConfig:
    get_config.cache_clear()
    return get_config()


# Backwards-compatibility shim for the early scaffold code.
class ServiceConfig:
    """Minimal pre-Pydantic placeholder kept so old imports don't break."""

    def __init__(self, log_level: str = "info", timezone: str = "UTC") -> None:
        self.log_level = log_level
        self.timezone = timezone
