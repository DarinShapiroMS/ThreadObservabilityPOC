"""Unit tests for the OTBR / openthread log line parser."""

from __future__ import annotations

from thread_observability.pipeline import otbr_parser


def test_parse_attach_succeeded_with_iso_ts() -> None:
    line = "2026-05-11T20:14:07.123Z [N] Mle-----------: Attach succeeded"
    ev = otbr_parser.parse_line(line)
    assert ev is not None
    assert ev.type == "attach"
    assert ev.ts.startswith("2026-05-11T20:14:07")


def test_parse_attach_failed_with_space_ts() -> None:
    line = "2026-05-11 20:14:08,500 [W] Mle: AttachState ParentRequest -> Idle (attach failed)"
    ev = otbr_parser.parse_line(line)
    assert ev is not None
    assert ev.type == "attach_failed"
    assert ev.ts.startswith("2026-05-11T20:14:08")


def test_parse_attach_attempt_without_ts_falls_back_to_now() -> None:
    line = "[N] Mle-----------: Attach attempt 1, AnyPartition"
    ev = otbr_parser.parse_line(line)
    assert ev is not None
    assert ev.type == "attach_attempt"
    assert ev.eui64 is None


def test_parse_parent_response_extracts_eui64() -> None:
    line = "[I] Mle: Parent response from 1234567890abcdef rss:-65 lqi:210"
    ev = otbr_parser.parse_line(line)
    assert ev is not None
    assert ev.type == "parent_response"
    assert ev.eui64 == "1234567890abcdef"
    assert ev.rssi == -65
    assert ev.lqi == 210


def test_parse_child_added() -> None:
    line = "[N] ChildTable----: Child added ext_addr=abcdef0011223344"
    ev = otbr_parser.parse_line(line)
    assert ev is not None
    assert ev.type == "child_added"
    assert ev.eui64 == "abcdef0011223344"


def test_parse_child_timeout() -> None:
    line = "[I] ChildSupervsn-: Child timeout expired, ext_addr=cafebabecafebabe"
    ev = otbr_parser.parse_line(line)
    assert ev is not None
    assert ev.type == "child_removed"
    assert ev.eui64 == "cafebabecafebabe"


def test_parse_detach() -> None:
    line = "[N] Mle: Detached from parent 0011223344556677"
    ev = otbr_parser.parse_line(line)
    assert ev is not None
    assert ev.type == "detach"
    assert ev.eui64 == "0011223344556677"


def test_parse_role_change() -> None:
    line = "[N] Mle-----------: Role detached -> child"
    ev = otbr_parser.parse_line(line)
    assert ev is not None
    assert ev.type == "role_change"


def test_parse_unrecognised_returns_none() -> None:
    assert otbr_parser.parse_line("[D] Random unrelated noise line") is None
    assert otbr_parser.parse_line("") is None
    assert otbr_parser.parse_line("   \n") is None


def test_parse_lines_filters_none() -> None:
    lines = [
        "[N] Mle: Attach succeeded",
        "garbage",
        "[N] Mle: Detached from parent 1111222233334444",
        "",
    ]
    evs = otbr_parser.parse_lines(lines)
    assert [e.type for e in evs] == ["attach", "detach"]


def test_to_storage_kwargs_round_trip() -> None:
    line = "[I] Mle: Parent response from 1234567890abcdef rss:-72"
    ev = otbr_parser.parse_line(line)
    assert ev is not None
    kw = ev.to_storage_kwargs()
    assert kw["eui64"] == "1234567890abcdef"
    assert kw["type"] == "parent_response"
    assert kw["rssi"] == -72
    assert kw["payload"]["source"] == "otbr"
