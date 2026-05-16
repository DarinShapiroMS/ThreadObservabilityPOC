from __future__ import annotations

from datetime import UTC, datetime, timedelta

from thread_observability.utils.coercion import (
    coerce_int,
    first_present_field,
    to_tristate_int,
)
from thread_observability.utils.datetime import parse_iso_datetime, to_iso_utc


def test_parse_iso_datetime_normalizes_z_and_naive_values() -> None:
    assert parse_iso_datetime("2026-05-12T10:00:00Z") == datetime(2026, 5, 12, 10, 0, 0, tzinfo=UTC)
    assert parse_iso_datetime("2026-05-12T10:00:00") == datetime(2026, 5, 12, 10, 0, 0, tzinfo=UTC)


def test_parse_iso_datetime_converts_offsets_to_utc() -> None:
    assert parse_iso_datetime("2026-05-12T12:30:00+02:00") == datetime(2026, 5, 12, 10, 30, 0, tzinfo=UTC)


def test_to_iso_utc_normalizes_timezone() -> None:
    value = datetime(2026, 5, 12, 12, 30, 0, tzinfo=UTC) + timedelta(hours=2)
    assert to_iso_utc(value) == "2026-05-12T14:30:00+00:00"


def test_coerce_int_supports_hex_and_blank_string_handling() -> None:
    assert coerce_int("0x10", allow_strings=True) == 16
    assert coerce_int("  ", allow_strings=True) is None
    assert coerce_int("10") is None


def test_first_present_field_checks_integer_key_before_aliases() -> None:
    payload = {"0": "by-id", "ExtAddress": "by-name"}
    assert first_present_field(payload, "ExtAddress", int_key=0) == "by-id"


def test_to_tristate_int_coerces_truthy_and_falsey_values() -> None:
    assert to_tristate_int(True) == 1
    assert to_tristate_int(0) == 0
    assert to_tristate_int(None) is None
