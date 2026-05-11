"""Parse OTBR / OpenThread log lines into canonical Thread events.

The Supervisor exposes raw container stdout via ``/addons/{slug}/logs``. Lines
typically look like one of::

    2026-05-11 20:14:07.123 [N] Mle-----------: Role detached -> child
    [I] ChildTable-----: 1234567890abcdef OnUnsecureFrameReceived rss:-65 lqi:210
    [W] Mle-----------: AttachState ParentRequest -> Idle (attach failed)
    2026-05-11T20:14:08Z otbr-agent[42]: Sending child update request to 1234567890abcdef

The parser is intentionally tolerant: unknown lines return ``None``. The goal
is to surface a useful subset of events for the topology engine and reasoner
(attach / detach / parent_change / attach_failed / child_added) plus RSSI/LQI
samples when present.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

_EUI = r"[0-9a-fA-F]{16}"

# Leading timestamp variants we accept. All are anchored to start-of-line.
_TS_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:?\d{2})?)\s+"),
    re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})[,.](?P<ms>\d{3})\s+"),
    # Time-only format from OTBR daemon logs: HH:MM:SS.mmm (e.g., "16:39:34.573")
    re.compile(r"^(?P<ts>\d{2}:\d{2}:\d{2})\.(?P<ms>\d{3})\s+"),
)

# Event signatures. Each yields a partial canonical event; the caller fills ts.
_EVENT_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # === Real MeshForwarder patterns from OTBR daemon logs ===
    # "Failed to send IPv6 UDP msg ... to:0x9c00 ... error:ChannelAccessFailure ..."
    (re.compile(r"Failed\s+to\s+send\s+IPv6\s+UDP\s+msg.*?to:(?P<to_id>0x[0-9a-f]+|[0-9a-f]{16}).*?error:(?P<error>\w+)", re.I),
     "link_failure"),
    # "Dropping (reassembly queue) ... error:ReassemblyTimeout ..."
    (re.compile(r"Dropping\s+\(reassembly\s+queue\).*?error:(?P<error>\w+)", re.I),
     "reassembly_timeout"),
    # "Handle transmit done failed: ChannelAccessFailure"
    (re.compile(r"Handle\s+transmit\s+done\s+failed.*?:\s*(?P<error>\w+)", re.I),
     "transmit_failure"),
    # "Failed to process Link Accept: Security"
    (re.compile(r"Failed\s+to\s+process\s+Link\s+Accept.*?:\s*(?P<error>\w+)", re.I),
     "link_accept_failed"),
    # IPv6 address lines: extract EUI64 from fd29:382:eded:1:XXXX:XXXX:XXXX:XXXX
    (re.compile(r"(?:src|dst):\[fd[0-9a-f:]+?:(?P<eui64_ipv6>[0-9a-f]{4}:[0-9a-f]{4}:[0-9a-f]{4}:[0-9a-f]{4})\]", re.I),
     "node_seen"),
    
    # === Legacy OpenThread patterns (for compatibility) ===
    # "Role detached -> child" / "Role child -> router"
    (re.compile(r"Role\s+\w+\s*->\s*(?P<role>detached|child|router|leader|disabled)", re.I),
     "role_change"),
    # "Attach attempt N" — useful signal, treat as attach_attempt
    (re.compile(r"Attach attempt\s+\d+", re.I), "attach_attempt"),
    # "Parent response from <eui64>"  / "Send Parent Request"
    (re.compile(rf"Parent\s+(?:response|update)\s+(?:from|to)\s+(?P<eui64>{_EUI})", re.I),
     "parent_response"),
    # explicit attach succeeded
    (re.compile(r"Attach\s+(?:succeeded|complete)", re.I), "attach"),
    # explicit attach failed
    (re.compile(r"Attach\s+(?:failed|aborted)", re.I), "attach_failed"),
    # "Child added <eui64>" / "Add Child <eui64>"
    (re.compile(rf"(?:Child\s+added|Add\s+Child)\D+(?P<eui64>{_EUI})", re.I),
     "child_added"),
    # "Child <eui64> removed" / "Child timeout expired, ext_addr=<eui64>"
    (re.compile(rf"Child\s+(?:timeout\s+expired,\s+ext_addr=|removed[: ]+)\s*(?P<eui64>{_EUI})", re.I),
     "child_removed"),
    # "Detached from parent <eui64>"
    (re.compile(rf"Detached\s+from\s+parent\s+(?P<eui64>{_EUI})", re.I),
     "detach"),
    # Generic "ext_addr=<eui64>" so we can at least anchor the node identity
    (re.compile(rf"ext_addr=(?P<eui64>{_EUI})", re.I), "node_seen"),
]

# RSSI / LQI extractors (applied independently after main match)
_RSSI_RE = re.compile(r"rss(?:i)?[:= ]\s*(?P<rssi>-?\d+(?:\.\d+)?)", re.I)  # Supports decimal: -75.5
_LQI_RE = re.compile(r"lqi[:= ]\s*(?P<lqi>\d+)", re.I)
_PARENT_RE = re.compile(rf"parent[:= ]\s*(?P<parent>{_EUI})", re.I)
_IPV6_EUI64_RE = re.compile(r"fd[0-9a-f:]+?:(?P<eui64_ipv6>[0-9a-f]{4}:[0-9a-f]{4}:[0-9a-f]{4}:[0-9a-f]{4})", re.I)


@dataclass(frozen=True)
class ParsedEvent:
    ts: str
    eui64: str | None
    type: str
    parent_eui64: str | None = None
    rssi: int | None = None
    lqi: int | None = None
    raw: str = ""

    def to_storage_kwargs(self) -> dict[str, Any]:
        """Shape suitable for ``SQLiteStore.insert_event``."""
        return {
            "eui64": self.eui64 or "0000000000000000",
            "type": self.type,
            "ts": self.ts,
            "parent_eui64": self.parent_eui64,
            "rssi": self.rssi,
            "lqi": self.lqi,
            "payload": {"source": "otbr", "raw": self.raw[:300]},
        }


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _extract_ts(line: str) -> tuple[str, str]:
    """Return (iso_ts, remainder). Falls back to now() if no leading ts."""
    for pat in _TS_PATTERNS:
        m = pat.match(line)
        if not m:
            continue
        raw = m.group("ts")
        try:
            # Handle time-only format (HH:MM:SS from OTBR daemon logs)
            if len(raw) <= 8:  # Just HH:MM:SS
                today = datetime.now(tz=UTC).date()
                ms = m.group("ms") if "ms" in m.groupdict() else None
                ms_str = f".{ms}" if ms else ""
                normalised = f"{today}T{raw}{ms_str}"
            else:
                # Normalise "YYYY-MM-DD HH:MM:SS" → "YYYY-MM-DDTHH:MM:SS"
                normalised = raw.replace(" ", "T") if "T" not in raw and " " in raw else raw
                # Append milliseconds if separately captured
                try:
                    ms = m.group("ms")
                    if ms:
                        normalised = f"{normalised}.{ms}"
                except IndexError:
                    pass
            if normalised.endswith("Z"):
                normalised = normalised[:-1] + "+00:00"
            dt = datetime.fromisoformat(normalised)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC).isoformat(), line[m.end():]
        except ValueError:
            return _now_iso(), line[m.end():]
    return _now_iso(), line


def parse_line(line: str) -> ParsedEvent | None:
    """Parse a single OTBR/openthread log line. Returns None if unrecognised."""
    if not line or not line.strip():
        return None
    ts, body = _extract_ts(line)
    for pat, etype in _EVENT_PATTERNS:
        m = pat.search(body)
        if not m:
            continue
        gd = m.groupdict()
        eui = gd.get("eui64")
        
        # Try IPv6 EUI64 extraction (format: fd29:382:eded:1:XXXX:XXXX:XXXX:XXXX)
        if not eui:
            ipv6_match = _IPV6_EUI64_RE.search(body)
            if ipv6_match:
                ipv6_eui_part = ipv6_match.group("eui64_ipv6")
                # Convert "c6b7:7f58:e5ac:eed4" to "c6b77f58e5aceed4"
                eui = ipv6_eui_part.replace(":", "").lower()
        
        # For role_change / attach_* there may be no eui64 in the line; try
        # the fallback "ext_addr=" anywhere else in the body.
        if not eui:
            extra = re.search(rf"ext_addr=({_EUI})", body, re.I)
            if extra:
                eui = extra.group(1)
        
        # Try extracting from "to:" field (short address or EUI64)
        if not eui and "to_id" in gd:
            to_id = gd.get("to_id", "").lower()
            # If it's a short address like "0x9c00", normalize to 16-char format
            if to_id.startswith("0x"):
                to_id = to_id[2:].zfill(16)
            if len(to_id) <= 16:
                eui = to_id.zfill(16)
        
        rssi_m = _RSSI_RE.search(body)
        lqi_m = _LQI_RE.search(body)
        par_m = _PARENT_RE.search(body)
        
        # Parse RSSI, handling both integer and decimal values
        rssi = None
        if rssi_m:
            try:
                rssi = int(float(rssi_m.group("rssi")))  # Convert "-75.5" → -75
            except ValueError:
                rssi = None
        
        return ParsedEvent(
            ts=ts,
            eui64=(eui.lower() if eui else None),
            type=etype,
            parent_eui64=(par_m.group("parent").lower() if par_m else None),
            rssi=rssi,
            lqi=int(lqi_m.group("lqi")) if lqi_m else None,
            raw=line.rstrip("\n"),
        )
    return None


def parse_lines(lines: list[str]) -> list[ParsedEvent]:
    out: list[ParsedEvent] = []
    for line in lines:
        ev = parse_line(line)
        if ev is not None:
            out.append(ev)
    return out
