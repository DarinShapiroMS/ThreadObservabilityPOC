"""Background Diagnostics — proactive AI assessment of Thread mesh health.

This sub-package implements the Phase 4 sprint (issues #18-#22):

* ``scheduler``  — adaptive cadence state machine + daily budget cap.
* ``engine``     — evidence-gathering + verdict-envelope validation;
  pluggable LLM bridge (delegates to whatever conversation agent the
  HA install has configured).
* ``feedback``   — outcome capture + quality metrics aggregation.

Nothing here owns an LLM key. The engine speaks to HA's
``conversation.process`` service (or a test-injectable stub).
"""

from __future__ import annotations

from .scheduler import (
    AssessmentScheduler,
    ScheduleConfig,
    ScheduleSnapshot,
    SchedulerDecision,
)

__all__ = [
    "AssessmentScheduler",
    "ScheduleConfig",
    "ScheduleSnapshot",
    "SchedulerDecision",
]
