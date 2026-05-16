"""Shared application metadata helpers for the HTTP API surface.

Keeping these small, dependency-free utilities in a dedicated module reduces
cross-cutting concerns in the main FastAPI app factory.
"""

from __future__ import annotations

import re
from pathlib import Path

from ..utils.datetime import utc_now_iso


def _read_addon_version() -> str:
    """Read version from config.yaml so it never drifts from the manifest."""
    here = Path(__file__).resolve()
    candidates = [
        Path("/opt/thread-observability/config.yaml"),  # baked into image
        Path("/config.yaml"),  # mounted into container
        Path("/app/config.yaml"),  # alt container layout
        *(p / "config.yaml" for p in here.parents),  # walk up (covers dev tree)
    ]
    for candidate in candidates:
        try:
            if candidate.exists():
                match = re.search(
                    r"^version:\\s*([^\\s#]+)", candidate.read_text(), re.MULTILINE
                )
                if match:
                    return match.group(1).strip().strip('"').strip("'")
        except OSError:
            continue
    return "unknown"


ADDON_VERSION = _read_addon_version()
LOG_PATH = Path("/data/thread-observability/addon.log")

# Ingress dashboard HTML. Loaded once because it's a static asset packaged
# with the app.
DASHBOARD_HTML = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")


def utc_now() -> str:
    return utc_now_iso()


def tail_log(n: int = 80) -> list[str]:
    if not LOG_PATH.exists():
        return []
    try:
        return LOG_PATH.read_text(errors="replace").splitlines()[-n:]
    except OSError:
        return []
