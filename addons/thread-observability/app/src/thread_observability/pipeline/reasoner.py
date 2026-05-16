"""Deterministic Thread anomaly reasoner.

Scans the SQLite event stream and opens/closes issues via the issues
table. All rules are deterministic and side-effect-isolated: the only
mutation is ``open_issue`` / ``close_issue`` calls on the store.

Rules (v1):

* ``parent_churn`` (warn) — a node emitted >= 3 ``parent_change`` events
  within the last :data:`PARENT_CHURN_WINDOW_MIN` minutes.
* ``attach_failures`` (warn) — a node emitted >= 2 ``attach_failed``
  events within the last :data:`ATTACH_FAIL_WINDOW_MIN` minutes.
* ``offline_node`` (crit) — a node has not been seen for at least
  :data:`OFFLINE_THRESHOLD_MIN` minutes despite having been seen before.

Each run also auto-closes issues whose triggering condition no longer
holds (e.g. churn dropped below threshold, node came back online).
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

import json

from ..storage.sqlite_store import SQLiteStore, get_store
from ..utils.datetime import to_iso_utc

PARENT_CHURN_WINDOW_MIN = 30
PARENT_CHURN_THRESHOLD = 3

ATTACH_FAIL_WINDOW_MIN = 15
ATTACH_FAIL_THRESHOLD = 2

OFFLINE_THRESHOLD_MIN = 30

# v0.9.43 — Tier 2 #2.
# A "re-attach storm" is what the Foyer Light case looked like in
# production: the same EUI keeps showing up in NeighborTable rows
# across multiple reporters, with its link frame counter resetting
# each cycle. The single-reporter-noise floor is high (a child
# legitimately re-attaches occasionally), so we only fire when at
# least two distinct reporters witness it — that cross-check is
# what makes the rule specific to the partition-wide identity bug
# rather than a flaky child.
RE_ATTACH_STORM_WINDOW_MIN = 30
RE_ATTACH_STORM_MIN_EVENTS = 2
RE_ATTACH_STORM_MIN_REPORTERS = 2

# v0.9.43 — Tier 2 #1. ``mesh_disagreement`` compares the latest
# Matter cluster-53 self-counter (``nodes.tx_total_count``) against
# the most-recent OTBR ``MGMT_DIAG_GET`` witness for the same target.
# A %-delta over the threshold is the operator-visible signal that
# the router and the BR disagree about how much traffic the router
# has actually sent.
MESH_DISAGREEMENT_PCT_THRESHOLD = 25.0
# Snapshots must be observed within this window of each other to be
# considered comparable. Otherwise we're comparing apples to a stale
# orange and the %-delta is meaningless.
MESH_DISAGREEMENT_MAX_AGE_MIN = 30

# v0.9.44 (Tier 3): observer-suppression grace.
# When an observer-side disruption (our addon, OTBR, Matter Server)
# closes, downstream issues can still fire for a few cycles before
# stale data clears. We extend the suppression window past
# ``ended_at`` by this grace so those tail-fires get annotated too.
OBSERVER_SUPPRESSION_GRACE_SEC = 90


# Issue detection is paused pending a redesign of the rule set. See
# tracking issue #5 ("Redesign issue definitions") and #4 (placeholder
# implementation). The previous rules largely restated state already
# visible elsewhere and biased AI consumers toward specific diagnostic
# paths that were not always correct. Until new rules ship that pass
# the bar described in #5, ``run_reasoner`` is a no-op: it closes any
# residual open issues on first call so the table doesn't leak stale
# rows, then returns a paused-status summary.
#
# The full rule body below is intentionally retained so the redesign
# can re-enable rules incrementally without re-implementing plumbing.
ISSUES_PAUSED = True
ISSUES_PAUSED_NOTE = (
    "Issue detection is paused pending redesign. See tracking issue #5. "
    "No issues will be reported until the new rule set lands."
)


def run_reasoner(
    *,
    now: datetime | None = None,
    store: SQLiteStore | None = None,
) -> dict[str, Any]:
    """Run all rules once and reconcile open issues.

    Returns a summary dict with the lists of newly-opened, still-open and
    auto-closed issue ids per rule.
    """
    s = store or get_store()
    now_dt = now or datetime.now(tz=UTC)

    if ISSUES_PAUSED:
        # Close any leftover open issues so the table doesn't leak
        # stale rows while the rules are paused. This is idempotent:
        # subsequent calls find nothing to close and return immediately.
        residual_closed: list[int] = []
        for issue in s.list_active_issues():
            try:
                if s.close_issue(int(issue["id"])):
                    residual_closed.append(int(issue["id"]))
            except Exception:  # noqa: BLE001
                pass
        return {
            "status": "paused",
            "note": ISSUES_PAUSED_NOTE,
            "opened": [],
            "closed": residual_closed,
            "skipped": [],
            "computed_at": now_dt.isoformat(),
        }

    opened: list[int] = []
    closed: list[int] = []
    skipped: list[int] = []

    # ---- gather raw inputs in one lock ----
    churn_window = to_iso_utc(now_dt - timedelta(minutes=PARENT_CHURN_WINDOW_MIN))
    attach_window = to_iso_utc(now_dt - timedelta(minutes=ATTACH_FAIL_WINDOW_MIN))
    offline_cutoff = to_iso_utc(now_dt - timedelta(minutes=OFFLINE_THRESHOLD_MIN))
    re_attach_window = to_iso_utc(now_dt - timedelta(minutes=RE_ATTACH_STORM_WINDOW_MIN))
    mesh_disagree_cutoff = to_iso_utc(
        now_dt - timedelta(minutes=MESH_DISAGREEMENT_MAX_AGE_MIN)
    )

    with s._lock:  # noqa: SLF001
        churn_rows = s._conn.execute(  # noqa: SLF001
            "SELECT eui64, COUNT(*) AS c FROM events"
            " WHERE type = 'parent_change' AND ts >= ?"
            " GROUP BY eui64",
            (churn_window,),
        ).fetchall()

        attach_rows = s._conn.execute(  # noqa: SLF001
            "SELECT eui64, COUNT(*) AS c FROM events"
            " WHERE type = 'attach_failed' AND ts >= ?"
            " GROUP BY eui64",
            (attach_window,),
        ).fetchall()

        node_rows = s._conn.execute(  # noqa: SLF001
            "SELECT eui64, last_seen FROM nodes WHERE last_seen IS NOT NULL"
        ).fetchall()

        # v0.9.43 — re_attach_storm raw input.
        # We pull each event's payload JSON and let Python parse out
        # neighbor + reporter; doing it in SQL would require either
        # JSON1 (not guaranteed on all Python sqlite builds) or
        # generated columns we don't have.
        re_attach_rows = s._conn.execute(  # noqa: SLF001
            "SELECT eui64, payload_json FROM events"
            " WHERE type = 're_attached_node' AND ts >= ?",
            (re_attach_window,),
        ).fetchall()

        # v0.9.43 — mesh_disagreement raw input. Pull each router's
        # self-reported MAC TX counter and its most recent OTBR
        # second-witness, joined on EUI. We use a window function so
        # this is one round-trip instead of N. Wrapped defensively:
        # the table only exists on schemas >= v14, and a cold-start
        # SQLite without it should not break the reasoner.
        try:
            mesh_rows = s._conn.execute(  # noqa: SLF001
                """
                WITH latest_otbr AS (
                    SELECT target_eui64, mac_tx_total, observed_at,
                           ROW_NUMBER() OVER (
                               PARTITION BY target_eui64
                               ORDER BY observed_at DESC, id DESC
                           ) AS rn
                    FROM otbr_diagnostics
                    WHERE observed_at >= ?
                )
                SELECT n.eui64, n.tx_total_count, n.diag_updated_at,
                       o.mac_tx_total, o.observed_at AS otbr_observed_at
                FROM nodes n
                JOIN latest_otbr o
                  ON o.target_eui64 = n.eui64 AND o.rn = 1
                WHERE n.tx_total_count IS NOT NULL
                  AND o.mac_tx_total IS NOT NULL
                  AND n.diag_updated_at >= ?
                """,
                (mesh_disagree_cutoff, mesh_disagree_cutoff),
            ).fetchall()
        except Exception:  # noqa: BLE001
            mesh_rows = []

    churn_counts: Counter[str] = Counter({r["eui64"]: int(r["c"]) for r in churn_rows})
    attach_counts: Counter[str] = Counter({r["eui64"]: int(r["c"]) for r in attach_rows})
    offline_nodes = {r["eui64"]: r["last_seen"] for r in node_rows if r["last_seen"] < offline_cutoff}

    # ---- aggregate re_attach events per (neighbor) with distinct reporters ----
    # ``neighbor_eui64`` is the device that re-attached (the subject of the
    # issue); ``reporter_eui64`` is who witnessed the counter reset. The
    # cross-reporter check is what filters out a single flapping link
    # and keeps the alarm specific to identity churn.
    re_attach_by_neighbor: dict[str, dict[str, Any]] = {}
    for row in re_attach_rows:
        try:
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        except Exception:  # noqa: BLE001
            payload = {}
        if not isinstance(payload, dict):
            continue
        neighbor = payload.get("neighbor_eui64") or row["eui64"]
        reporter = payload.get("reporter_eui64")
        if not neighbor:
            continue
        bucket = re_attach_by_neighbor.setdefault(
            neighbor, {"count": 0, "reporters": set()}
        )
        bucket["count"] += 1
        if reporter:
            bucket["reporters"].add(reporter)

    # ---- aggregate mesh_disagreement per router ----
    # We compute the relative delta against the larger of the two
    # counters so a target near zero doesn't divide-by-zero or produce
    # absurd percentages. A negative delta (BR sees fewer than the
    # router claims) is just as interesting as positive, so we use abs.
    mesh_disagreements: dict[str, dict[str, Any]] = {}
    for row in mesh_rows:
        eui = row["eui64"]
        self_count = row["tx_total_count"]
        otbr_count = row["mac_tx_total"]
        if not isinstance(self_count, int) or not isinstance(otbr_count, int):
            continue
        denom = max(self_count, otbr_count)
        if denom <= 0:
            continue
        pct = abs(self_count - otbr_count) * 100.0 / denom
        if pct >= MESH_DISAGREEMENT_PCT_THRESHOLD:
            mesh_disagreements[eui] = {
                "self_tx_total": self_count,
                "otbr_tx_total": otbr_count,
                "delta_pct": round(pct, 2),
                "self_observed_at": row["diag_updated_at"],
                "otbr_observed_at": row["otbr_observed_at"],
            }

    active = s.list_active_issues()
    active_by_key: dict[tuple[str, str | None], dict[str, Any]] = {
        (i["kind"], i.get("eui64")): i for i in active
    }

    def _check_suppression(
        since: str | None, until: str | None
    ) -> list[dict[str, Any]]:
        """Return observer events overlapping [since, until + grace].

        Tier 3: a candidate issue is annotated (and ``crit`` downgraded
        to ``warn``) when an observer-side disruption overlaps its
        trigger window. We extend the upper bound by
        ``OBSERVER_SUPPRESSION_GRACE_SEC`` so an issue that fires in the
        seconds immediately after a restart still picks up the prior
        outage as context.

        Returns an empty list (no suppression) when the inputs are not
        usable (missing timestamps, lookup errors).
        """
        if not since:
            return []
        try:
            upper_dt = (
                datetime.fromisoformat(until) if until else now_dt
            ) + timedelta(seconds=OBSERVER_SUPPRESSION_GRACE_SEC)
            return s.list_observer_events_in_window(
                since=since, until=upper_dt.isoformat()
            )
        except Exception:  # noqa: BLE001
            return []

    def _emit(
        kind: str,
        severity: str,
        eui64: str | None,
        evidence: dict[str, Any],
        *,
        trigger_since: str | None = None,
        trigger_until: str | None = None,
    ) -> None:
        # Tier 3 suppression: annotate + downgrade when an observer-side
        # disruption overlaps the trigger window. We never drop the
        # issue — that would lose a real outage that coincides with a
        # routine restart. Downgrading ``crit`` → ``warn`` is enough
        # to keep noise out of pager-grade alerts while preserving the
        # record.
        suppressors = _check_suppression(trigger_since, trigger_until)
        if suppressors:
            evidence = {
                **evidence,
                "suppressed_by": [
                    {
                        "id": ev["id"],
                        "source": ev["source"],
                        "kind": ev["kind"],
                        "started_at": ev["started_at"],
                        "ended_at": ev.get("ended_at"),
                    }
                    for ev in suppressors
                ],
            }
            if severity == "crit":
                severity = "warn"
        issue_id = s.open_issue(kind=kind, severity=severity, eui64=eui64, evidence=evidence)
        if (kind, eui64) in active_by_key:
            skipped.append(issue_id)
        else:
            opened.append(issue_id)

    # ---- parent_churn ----
    seen_keys: set[tuple[str, str | None]] = set()
    for eui, count in churn_counts.items():
        if count >= PARENT_CHURN_THRESHOLD:
            seen_keys.add(("parent_churn", eui))
            _emit(
                "parent_churn",
                "warn",
                eui,
                {
                    "count": count,
                    "window_minutes": PARENT_CHURN_WINDOW_MIN,
                    "threshold": PARENT_CHURN_THRESHOLD,
                },
                trigger_since=churn_window,
            )

    # ---- attach_failures ----
    for eui, count in attach_counts.items():
        if count >= ATTACH_FAIL_THRESHOLD:
            seen_keys.add(("attach_failures", eui))
            _emit(
                "attach_failures",
                "warn",
                eui,
                {
                    "count": count,
                    "window_minutes": ATTACH_FAIL_WINDOW_MIN,
                    "threshold": ATTACH_FAIL_THRESHOLD,
                },
                trigger_since=attach_window,
            )

    # ---- offline_node ----
    # Trigger window for suppression spans from ``last_seen`` (when we
    # last had ground truth for this node) to now. This is the most
    # important suppression case: an addon restart between last_seen
    # and now is precisely the false-positive we want to annotate.
    for eui, last_seen in offline_nodes.items():
        seen_keys.add(("offline_node", eui))
        _emit(
            "offline_node",
            "crit",
            eui,
            {"last_seen": last_seen, "threshold_minutes": OFFLINE_THRESHOLD_MIN},
            trigger_since=last_seen,
        )

    # ---- re_attach_storm ----
    # Fires when the same neighbor has re-attached at least N times in
    # the window AND been witnessed by at least M distinct reporters.
    # The cross-reporter requirement is what makes this a partition-
    # wide identity signal rather than a single-link flap.
    for neighbor, bucket in re_attach_by_neighbor.items():
        reporters = bucket["reporters"]
        if (
            bucket["count"] >= RE_ATTACH_STORM_MIN_EVENTS
            and len(reporters) >= RE_ATTACH_STORM_MIN_REPORTERS
        ):
            seen_keys.add(("re_attach_storm", neighbor))
            _emit(
                "re_attach_storm",
                "warn",
                neighbor,
                {
                    "count": bucket["count"],
                    "distinct_reporters": sorted(reporters),
                    "window_minutes": RE_ATTACH_STORM_WINDOW_MIN,
                    "threshold_events": RE_ATTACH_STORM_MIN_EVENTS,
                    "threshold_reporters": RE_ATTACH_STORM_MIN_REPORTERS,
                },
                trigger_since=re_attach_window,
            )

    # ---- mesh_disagreement ----
    for eui, evidence in mesh_disagreements.items():
        seen_keys.add(("mesh_disagreement", eui))
        # Compare the two observation timestamps to pick the earlier as
        # the suppression-window start.
        self_ts = evidence.get("self_observed_at")
        otbr_ts = evidence.get("otbr_observed_at")
        candidates = [t for t in (self_ts, otbr_ts) if t]
        trigger = min(candidates) if candidates else None
        _emit(
            "mesh_disagreement",
            "warn",
            eui,
            {
                **evidence,
                "threshold_pct": MESH_DISAGREEMENT_PCT_THRESHOLD,
                "max_age_minutes": MESH_DISAGREEMENT_MAX_AGE_MIN,
            },
            trigger_since=trigger,
        )

    # ---- wrong_network (v0.9.46) ----
    # If multiple non-phantom nodes report differing extended_pan_ids,
    # the minority is on stale Thread credentials — typically a device
    # re-commissioned while a stale dataset was still cached on HA.
    # This is a credentials problem, not an RF/partition-fragmentation
    # problem, so it deserves its own issue kind.
    try:
        all_nodes = s.list_nodes()
    except Exception:  # noqa: BLE001
        all_nodes = []
    epid_to_nodes: dict[str, list[dict]] = {}
    for n in all_nodes:
        if n.get("status") == "phantom":
            continue
        epid = n.get("extended_pan_id")
        if not epid:
            continue
        epid_to_nodes.setdefault(epid, []).append(n)
    if len(epid_to_nodes) >= 2:
        # Modal = the extended_pan_id with the most members.
        modal_epid, modal_members = max(
            epid_to_nodes.items(), key=lambda kv: len(kv[1])
        )
        modal_name = next(
            (n.get("network_name") for n in modal_members if n.get("network_name")),
            None,
        )
        for epid, members in epid_to_nodes.items():
            if epid == modal_epid:
                continue
            for n in members:
                eui = n.get("eui64")
                if not eui:
                    continue
                seen_keys.add(("wrong_network", eui))
                _emit(
                    "wrong_network",
                    "warn",
                    eui,
                    {
                        "node_extended_pan_id": epid,
                        "node_network_name": n.get("network_name"),
                        "modal_extended_pan_id": modal_epid,
                        "modal_network_name": modal_name,
                        "modal_member_count": len(modal_members),
                        "minority_member_count": len(members),
                    },
                )

    # ---- auto-close issues whose trigger no longer holds ----
    managed_kinds = {
        "parent_churn",
        "attach_failures",
        "offline_node",
        "re_attach_storm",
        "mesh_disagreement",
        "wrong_network",
    }
    for (kind, eui), issue in active_by_key.items():
        if kind not in managed_kinds:
            continue
        if (kind, eui) in seen_keys:
            continue
        if s.close_issue(int(issue["id"])):
            closed.append(int(issue["id"]))

    # ---- v0.9.45: auto-close stale ``partition_split`` issues ----
    # ``partition_split`` is *opened* by the matter_discovery stage (it
    # has the full per-router partition evidence). The reasoner runs
    # every tick regardless, so it's the right owner of the close-on-
    # resolve path: if the current live topology shows only one
    # partition (or none), any still-open partition_split issue is
    # stale and should close. This makes the reasoner the single
    # source of truth for issue *lifecycle* even when discovery
    # closing the issue itself silently failed (observed live as a
    # partition_split that resolved in topology but stayed open in
    # the issues table).
    try:
        from . import topology as topology_mod  # noqa: PLC0415

        topo = topology_mod.build_topology(store=s)
        live_partitions = topo.get("partitions") or []
        if len(live_partitions) <= 1:
            for (kind, eui), issue in active_by_key.items():
                if kind != "partition_split":
                    continue
                if s.close_issue(int(issue["id"])):
                    closed.append(int(issue["id"]))
    except Exception:  # noqa: BLE001
        # Topology can fail in unit tests that stub the store; the
        # close path is best-effort.
        pass

    return {
        "ran_at": to_iso_utc(now_dt),
        "opened": opened,
        "still_open": skipped,
        "closed": closed,
        "rules": {
            "parent_churn": {
                "window_minutes": PARENT_CHURN_WINDOW_MIN,
                "threshold": PARENT_CHURN_THRESHOLD,
            },
            "attach_failures": {
                "window_minutes": ATTACH_FAIL_WINDOW_MIN,
                "threshold": ATTACH_FAIL_THRESHOLD,
            },
            "offline_node": {"threshold_minutes": OFFLINE_THRESHOLD_MIN},
            "re_attach_storm": {
                "window_minutes": RE_ATTACH_STORM_WINDOW_MIN,
                "threshold_events": RE_ATTACH_STORM_MIN_EVENTS,
                "threshold_reporters": RE_ATTACH_STORM_MIN_REPORTERS,
            },
            "mesh_disagreement": {
                "threshold_pct": MESH_DISAGREEMENT_PCT_THRESHOLD,
                "max_age_minutes": MESH_DISAGREEMENT_MAX_AGE_MIN,
            },
            "wrong_network": {
                "trigger": "node extended_pan_id differs from modal mesh value",
            },
        },
    }
