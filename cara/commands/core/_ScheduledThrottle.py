"""A scheduled tick the concurrency gate refused is retried within its tick.

The scheduler runs every job INLINE through its middleware (no queue, no
broker). ``ConcurrencyLimited`` answers a full ceiling with
``ConcurrencyExceeded`` — "re-raise so the queue runner retries" — but inline
there is no queue runner: the fire was simply lost. Live 2026-11-01 one hour
of an Amazon soak dropped 15 fires (feeds sweep x7, notification drain x4,
MCF polls x4) while the fleet's two Amazon slots were busy; each was counted a
plain failure, ``ScheduledTaskMissed`` could not see them, and nothing was
retried.

Now a refused tick waits for a slot with a short exponential backoff, bounded
by a budget (:data:`_DEFAULT_BUDGET_SECONDS`, at most half an interval job's
period so a retry never collides with its own next fire). Every refused fire
is counted on ``scheduled_task_throttles_total{task, result}`` —
``recovered`` when a retry got a slot, ``dropped`` when the budget ran out
(then ``ConcurrencyExceeded`` propagates and the driver records the fire as
``outcome="throttled"``, not ``failure``).

Private to the scheduler commands, like ``_ScheduleRegistration``: callers
bind the module object and reach its underscore names.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable
from typing import Any

from cara.facades import Log
from cara.observability import MetricsBase
from cara.queues import ConcurrencyExceeded

#: The longest a refused tick keeps waiting for a slot.
_DEFAULT_BUDGET_SECONDS = 60.0
#: The first wait after a refusal; doubles per refusal (the gate itself has
#: already slept its own ``retry_delay`` before refusing).
_FIRST_BACKOFF_SECONDS = 2.0


def _throttle_budget_seconds(interval_seconds: float | None) -> float:
    """The retry budget of one entry: half an interval job's period, never
    more than :data:`_DEFAULT_BUDGET_SECONDS`."""
    if interval_seconds and interval_seconds > 0:
        return min(_DEFAULT_BUDGET_SECONDS, float(interval_seconds) / 2.0)
    return _DEFAULT_BUDGET_SECONDS


def _count(task: str, result: str) -> None:
    with contextlib.suppress(AttributeError, TypeError, ValueError):
        MetricsBase.scheduled_task_throttles_total.labels(task=task, result=result).inc()


def _run_with_throttle_retry(
    task: str,
    run_once: Callable[[], Any],
    *,
    budget_seconds: float = _DEFAULT_BUDGET_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Any:
    """Run ``run_once`` (one inline pass through the job's middleware),
    retrying it while the concurrency gate refuses and the budget lasts."""
    deadline = clock() + max(0.0, float(budget_seconds))
    backoff = _FIRST_BACKOFF_SECONDS
    refused = 0
    while True:
        try:
            result = run_once()
        except ConcurrencyExceeded:
            refused += 1
            remaining = deadline - clock()
            if remaining <= 0:
                _count(task, "dropped")
                Log.warning(
                    "Scheduled job '%s' dropped: the concurrency gate refused it "
                    "%s time(s) within its %.0fs retry budget.",
                    task,
                    refused,
                    budget_seconds,
                    category="scheduler",
                )
                raise
            sleep(min(backoff, remaining))
            backoff *= 2
            continue
        if refused:
            _count(task, "recovered")
        return result
