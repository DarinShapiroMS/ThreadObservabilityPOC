from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from fastapi import APIRouter


def create_assessment_router(*, get_config_fn, get_store_fn) -> APIRouter:
    router = APIRouter()

    @router.get("/v1/assessment/state")
    def assessment_state() -> dict[str, object]:
        try:
            from ...services.assessment.scheduler import AssessmentScheduler, ScheduleConfig

            cfg = get_config_fn().assessment
            sched = AssessmentScheduler(
                store=get_store_fn(),
                config=ScheduleConfig(
                    enabled=cfg.enabled,
                    probation_interval_minutes=cfg.probation_interval_minutes,
                    probation_checks=cfg.probation_checks,
                    relaxing_initial_hours=cfg.relaxing_initial_hours,
                    relaxing_max_hours=cfg.relaxing_max_hours,
                    heightened_initial_minutes=cfg.heightened_initial_minutes,
                    heightened_max_hours=cfg.heightened_max_hours,
                    engaged_interval_minutes=cfg.engaged_interval_minutes,
                    engaged_decay_minutes=cfg.engaged_decay_minutes,
                    daily_budget_calls=cfg.daily_budget_calls,
                ),
            )
            snap = sched.snapshot()
            return {
                "enabled": snap.enabled,
                "state": snap.state,
                "current_interval_seconds": snap.current_interval_seconds,
                "next_check_at": snap.next_assessment_at,
                "last_check_at": snap.last_assessment_at,
                "last_verdict": snap.reason,
                "calls_today": snap.budget_calls_used,
                "daily_budget": snap.daily_budget_calls,
                "probation_checks_remaining": max(
                    0, cfg.probation_checks - snap.consecutive_ok
                ),
                "reason": snap.reason,
            }
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    def _assessment_scheduler():
        from ...services.assessment.scheduler import AssessmentScheduler, ScheduleConfig

        cfg = ScheduleConfig.from_dict(get_config_fn().assessment.model_dump())
        return AssessmentScheduler(store=get_store_fn(), config=cfg)

    def _assessment_engine():
        from ...services.assessment.engine import AssessmentEngine

        cfg = get_config_fn().assessment
        return AssessmentEngine(
            store=get_store_fn(),
            context_recent_findings_default=cfg.context_recent_findings_default,
            context_recent_findings_by_model=cfg.context_recent_findings_by_model,
        )

    def _assessment_result_payload(result) -> dict[str, object]:
        return {
            "envelope": result.envelope.to_dict(),
            "finding_id": result.finding_id,
            "finding_key": result.finding_key,
            "dedup_hit": result.dedup_hit,
            "parse_attempts": result.parse_attempts,
            "duration_seconds": result.duration_seconds,
            "cleared_count": result.cleared_count,
            "suppressed": result.suppressed,
        }

    @router.get("/v1/assessment/findings")
    def assessment_findings(state: str = "open", limit: int = 50) -> dict[str, object]:
        try:
            rows = get_store_fn().list_assessment_findings(state=state, limit=limit)
            return {"findings": rows, "count": len(rows)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "findings": []}

    @router.get("/v1/assessment/history")
    def assessment_history(limit: int = 20, offset: int = 0) -> dict[str, object]:
        try:
            safe_limit = max(1, min(int(limit), 100))
            safe_offset = max(0, int(offset))
            rows = get_store_fn().list_assessment_runs(
                limit=safe_limit + 1,
                offset=safe_offset,
            )
            return {
                "runs": rows[:safe_limit],
                "count": len(rows[:safe_limit]),
                "limit": safe_limit,
                "offset": safe_offset,
                "has_more": len(rows) > safe_limit,
            }
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "runs": []}

    @router.post("/v1/assessment/run-now")
    def assessment_run_now(payload: dict[str, object] | None = None) -> dict[str, object]:
        try:
            scheduler = _assessment_scheduler()
            decision = scheduler.decide(force=True)
            decision_payload = {
                "should_run": decision.should_run,
                "reason": decision.reason,
                "next_run_at": decision.next_run_at,
                "state": decision.state,
                "budget_exhausted": decision.budget_exhausted,
            }
            if not decision.should_run:
                return {"ok": False, "decision": decision_payload}

            result = asyncio.run(_assessment_engine().run_once(extra_context=payload))
            snapshot = scheduler.record_assessment(verdict=result.envelope.verdict)
            return {
                "ok": True,
                "decision": decision_payload,
                "result": _assessment_result_payload(result),
                "schedule": snapshot.as_dict(),
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    @router.post("/v1/assessment/findings/{finding_id}/dismiss")
    def assessment_dismiss(
        finding_id: int, payload: dict[str, object] | None = None
    ) -> dict[str, object]:
        try:
            suppress_seconds = int((payload or {}).get("suppress_seconds") or 86400)
            row = get_store_fn().dismiss_assessment_finding(
                finding_id, suppress_seconds=suppress_seconds
            )
            if row is None:
                return {"error": f"finding {finding_id} not found"}
            return {"ok": True, "finding": row}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @router.post("/v1/assessment/findings/{finding_id}/feedback")
    def assessment_feedback(
        finding_id: int, payload: dict[str, object]
    ) -> dict[str, object]:
        try:
            from ...services.assessment import feedback as feedback_mod

            outcome = str((payload or {}).get("outcome") or "").strip()
            notes = (payload or {}).get("notes")
            notes_str = str(notes) if notes is not None else None
            result = feedback_mod.mark_outcome(
                finding_id=finding_id,
                outcome=outcome,
                notes=notes_str,
                store=get_store_fn(),
            )
            return {"ok": True, "result": result}
        except LookupError as exc:
            return {"error": str(exc)}
        except ValueError as exc:
            return {"error": str(exc)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @router.get("/v1/assessment/quality")
    def assessment_quality(since_hours: int = 168) -> dict[str, object]:
        try:
            from datetime import timedelta

            from ...services.assessment import feedback as feedback_mod

            since = (datetime.now(UTC) - timedelta(hours=since_hours)).isoformat()
            return feedback_mod.quality_summary(since=since, store=get_store_fn())
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    return router

