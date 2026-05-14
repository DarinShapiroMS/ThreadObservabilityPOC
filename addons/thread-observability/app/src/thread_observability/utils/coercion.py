"""Shared internal coercion helpers."""

from __future__ import annotations

from typing import Any


def coerce_int(value: Any, *, allow_strings: bool = False) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if allow_strings and isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped, 16) if stripped.lower().startswith("0x") else int(stripped)
        except ValueError:
            return None
    return None


def to_tristate_int(value: Any) -> int | None:
    if value is None:
        return None
    return 1 if bool(value) else 0


def first_present_field(struct: Any, *keys: str, int_key: int | None = None) -> Any:
    if not isinstance(struct, dict):
        return None
    if int_key is not None:
        value = struct.get(str(int_key))
        if value is not None:
            return value
    for key in keys:
        value = struct.get(key)
        if value is not None:
            return value
    return None