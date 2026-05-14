#!/usr/bin/env python3
"""Quick parser smoke check against a few real OTBR log lines.

This is a local developer helper, not part of the automated test suite.
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_SRC = REPO_ROOT / "addons" / "thread-observability" / "app" / "src"
sys.path.insert(0, str(APP_SRC))

from thread_observability.pipeline.otbr_parser import parse_line


TEST_LOGS = [
    "16:39:34.573 [N] MeshForwarder-:     dst:[fd29:382:eded:1:c6b7:7f58:e5ac:eed4]:59163",
    "16:39:36.566 [W] P-RadioSpinel-: Handle transmit done failed: ChannelAccessFailure",
    "16:39:36.566 [N] MeshForwarder-: Failed to send IPv6 UDP msg, len:90, chksum:0ef6, ecn:no, to:0x9c00, sec:yes, error:ChannelAccessFailure, prio:low, radio:15.4",
    "16:39:36.572 [N] MeshForwarder-: Dropping (reassembly queue) IPv6 UDP msg, len:1237, chksum:2e36, ecn:no, sec:yes, error:ReassemblyTimeout, prio:normal, rss:-76.0, radio:15.4",
    "16:43:29.920 [W] Mle-----------: Failed to process Link Accept: Security",
]

EXTENDED_LOGS = """16:39:36.566 [N] MeshForwarder-: Failed to send IPv6 UDP msg, len:90, chksum:0ef6, ecn:no, to:0x9c00, sec:yes, error:ChannelAccessFailure, prio:low, radio:15.4
16:39:36.566 [N] MeshForwarder-:     src:[fd29:382:eded:1:c6b7:7f58:e5ac:eed4]:59163
16:39:36.566 [N] MeshForwarder-:     dst:[fd29:382:eded:1:54df:dc70:b9b2:1106]:5540
16:39:36.572 [N] MeshForwarder-: Dropping (reassembly queue) IPv6 UDP msg, len:1237, chksum:2e36, ecn:no, sec:yes, error:ReassemblyTimeout, prio:normal, rss:-76.0, radio:15.4
16:39:36.572 [N] MeshForwarder-:     src:[fd29:382:eded:1:5b97:ece0:ad98:dfd]:5540
16:39:36.572 [N] MeshForwarder-:     dst:[fd29:382:eded:1:c6b7:7f58:e5ac:eed4]:59163
16:40:25.493 [N] MeshForwarder-: Failed to send IPv6 UDP msg, len:282, chksum:fb20, ecn:no, to:0xd000, sec:yes, error:ChannelAccessFailure, prio:low, radio:15.4
16:40:27.342 [N] MeshForwarder-: Failed to send IPv6 UDP msg, len:90, chksum:df17, ecn:no, to:0x9c00, sec:yes, error:NoAck, prio:low, radio:15.4
16:43:29.920 [W] Mle-----------: Failed to process Link Accept: Security"""


def main() -> None:
    print("Testing parser with real OTBR logs:\n")
    for idx, log_line in enumerate(TEST_LOGS, start=1):
        print(f"Log {idx}: {log_line[:80]}...")
        event = parse_line(log_line)
        if event:
            print(f"  parsed: type={event.type}, eui64={event.eui64}, rssi={event.rssi}")
        else:
            print("  no match")
        print()

    print("\n" + "=" * 60)
    print("Extended test: Multiple logs in sequence\n")

    parsed_count = 0
    event_types: dict[str, int] = {}
    rssi_values: list[int] = []
    for line in EXTENDED_LOGS.splitlines():
        event = parse_line(line)
        if not event:
            continue
        parsed_count += 1
        event_types[event.type] = event_types.get(event.type, 0) + 1
        if event.rssi is not None:
            rssi_values.append(event.rssi)
        print(f"parsed {event.type:20} | eui64={event.eui64 or 'None':16} | rssi={event.rssi}")

    print("\nSummary:")
    print(f"  Total lines parsed: {len(EXTENDED_LOGS.splitlines())}")
    print(f"  Events extracted: {parsed_count}")
    print(f"  Event types: {event_types}")
    print(f"  RSSI samples: {rssi_values}")


if __name__ == "__main__":
    main()