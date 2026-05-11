"""Thin async client for the Home Assistant Supervisor REST API.

Used by MCP tools to give VS Code a live view into add-on state and logs
without manual UI round-trips. Requires the add-on to be granted
``hassio_api: true`` (and for privileged operations ``hassio_role: manager``)
in ``config.yaml``.

The Supervisor injects ``SUPERVISOR_TOKEN`` and exposes its API at
``http://supervisor``.
"""

from __future__ import annotations

import os
from typing import Any

import httpx

SUPERVISOR_URL = os.getenv("SUPERVISOR_URL", "http://supervisor")
SUPERVISOR_TOKEN_ENV = "SUPERVISOR_TOKEN"
DEFAULT_TIMEOUT = 10.0


class SupervisorUnavailable(RuntimeError):
    """Raised when the Supervisor API cannot be reached or auth is missing."""


def _token() -> str:
    token = os.getenv(SUPERVISOR_TOKEN_ENV)
    if not token:
        raise SupervisorUnavailable(
            f"{SUPERVISOR_TOKEN_ENV} not set; running outside Supervisor?"
        )
    return token


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_token()}",
        "Accept": "application/json",
    }


async def _get_json(path: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        resp = await client.get(f"{SUPERVISOR_URL}{path}", headers=_headers())
        resp.raise_for_status()
        payload = resp.json()
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload if isinstance(payload, dict) else {"value": payload}


async def _get_text(path: str) -> str:
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        resp = await client.get(
            f"{SUPERVISOR_URL}{path}",
            headers={"Authorization": f"Bearer {_token()}", "Accept": "text/plain"},
        )
        resp.raise_for_status()
        return resp.text


async def _post(path: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(f"{SUPERVISOR_URL}{path}", headers=_headers())
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            return {"status": "ok"}


async def get_addon_info() -> dict[str, Any]:
    """Return Supervisor's view of this add-on (state, version, boot, etc.)."""
    info = await _get_json("/addons/self/info")
    # Surface the most useful fields up top for AI consumption.
    summary_keys = (
        "name", "slug", "version", "version_latest", "update_available",
        "state", "boot", "auto_update", "watchdog", "ingress", "ingress_url",
        "hostname", "available", "protected", "stage",
    )
    summary = {k: info[k] for k in summary_keys if k in info}
    return {"summary": summary, "raw": info}


async def get_addon_logs(lines: int = 200) -> list[str]:
    """Return the last *lines* lines of the add-on's container log."""
    text = await _get_text("/addons/self/logs")
    return text.splitlines()[-lines:]


async def get_supervisor_logs(lines: int = 200) -> list[str]:
    """Return the last *lines* lines of the Supervisor log."""
    text = await _get_text("/supervisor/logs")
    return text.splitlines()[-lines:]


async def restart_addon() -> dict[str, Any]:
    """Restart this add-on via Supervisor (fast; does not rebuild image)."""
    return await _post("/addons/self/restart")


async def rebuild_addon() -> dict[str, Any]:
    """Rebuild this add-on from its repository source, then restart."""
    return await _post("/addons/self/rebuild")


async def reload_store() -> dict[str, Any]:
    """Force Supervisor to re-scan all add-on repositories.

    Use this right after pushing a new version to the upstream repo so
    Supervisor sees the new ``config.yaml`` version without waiting for its
    next periodic poll.
    """
    return await _post("/store/reload")


async def check_for_update() -> dict[str, Any]:
    """Reload the store, then report current vs latest version for this add-on.

    Returns ``{current, latest, update_available, auto_update, state}``.
    """
    try:
        await reload_store()
    except Exception:  # noqa: BLE001
        # Store reload is best-effort; we still want the current info.
        pass
    info = await _get_json("/addons/self/info")
    return {
        "current": info.get("version"),
        "latest": info.get("version_latest"),
        "update_available": info.get("update_available"),
        "auto_update": info.get("auto_update"),
        "state": info.get("state"),
    }


async def update_addon() -> dict[str, Any]:
    """Update this add-on to the latest version available in the store.

    Equivalent to clicking "Update" in the HA UI. Supervisor will pull the
    new image (or rebuild from source for local repos) and restart.

    Supervisor's ``/addons/self/update`` alias is unreliable across versions;
    the canonical path is ``/store/addons/{slug}/update``. We look up the
    full slug from ``/addons/self/info`` first so the caller doesn't need
    to know it.
    """
    info = await _get_json("/addons/self/info")
    slug = info.get("slug")
    if not slug:
        raise RuntimeError("could not determine addon slug from /addons/self/info")
    return await _post(f"/store/addons/{slug}/update")


async def set_auto_update(enabled: bool) -> dict[str, Any]:
    """Toggle Supervisor's auto-update flag for this add-on."""
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        resp = await client.post(
            f"{SUPERVISOR_URL}/addons/self/options",
            headers=_headers(),
            json={"auto_update": bool(enabled)},
        )
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            return {"status": "ok", "auto_update": bool(enabled)}


async def reinstall_addon(slug: str) -> dict[str, Any]:
    """Uninstall then reinstall this add-on by slug.

    WARNING: this terminates the process making the call. The HTTP response
    will be cut off mid-flight once Supervisor stops the container. Caller
    should treat a connection-reset as the expected success signal and poll
    ``get_addon_info`` afterwards to confirm the new install.
    """
    async with httpx.AsyncClient(timeout=120.0) as client:
        # /addons/self/uninstall works while we're still running.
        await client.post(
            f"{SUPERVISOR_URL}/addons/self/uninstall", headers=_headers()
        )
        # After uninstall the "self" alias is gone; use the slug.
        resp = await client.post(
            f"{SUPERVISOR_URL}/store/addons/{slug}/install",
            headers=_headers(),
        )
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            return {"status": "ok", "action": "reinstall", "slug": slug}


async def get_ha_device_registry() -> list[dict[str, Any]]:
    """Attempt to fetch Home Assistant's device registry via REST API.

    Requires ``homeassistant_api: true`` in config.yaml and valid HA token.
    Falls back to empty list if HA is unreachable or returns no devices.
    """
    try:
        # Try to reach HA's REST API directly (assumes HA is accessible on network).
        # This is best-effort; if it fails, we fall back to local SQLite metadata.
        ha_url = os.getenv("HOMEASSISTANT_URL", "http://homeassistant.local:8123")
        ha_token = os.getenv("HOMEASSISTANT_TOKEN", "")
        if not ha_token:
            return []
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{ha_url}/api/config/device_registry/list",
                headers={"Authorization": f"Bearer {ha_token}", "Accept": "application/json"},
            )
            if resp.status_code == 200:
                devices = resp.json()
                return devices if isinstance(devices, list) else []
    except Exception:
        pass
    return []


