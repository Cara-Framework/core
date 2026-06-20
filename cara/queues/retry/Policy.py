"""Single source of truth for the framework's default retry policy.

Both consumer paths derive their attempt budget + backoff schedule from
here so they can never silently drift:

* the production worker — ``QueueWorkCommand.JobProcessor``
* the legacy driver loop — ``AMQPDriver.consume`` (deprecated)

…as does the publisher-side ``AMQPDriver.retry``. Previously each kept a
hand-copied ``DEFAULT_MAX_ATTEMPTS`` / ``DEFAULT_RETRY_BACKOFF_SECONDS``
that the surrounding comments could only *ask* future maintainers to keep
"in lockstep" — a silent-divergence bug waiting to happen.

A job class still overrides per-job by declaring ``max_attempts`` and/or
``retry_backoff`` (a list of per-attempt delays in seconds) at the class
level.
"""

from __future__ import annotations

# Max delivery attempts before a job is dead-lettered.
DEFAULT_MAX_ATTEMPTS = 3

# Per-attempt backoff in seconds, indexed by (attempt - 1); attempts past
# the end of the tuple reuse the last entry. 1/5/30 covers the fastest
# realistic recovery windows (DB connection drop, broker reconnect,
# gateway 5xx) without holding a poisoned message in flight long enough to
# back the queue up. Pre-policy the consumer nacked every failure straight
# to the DLX, so a single transient hiccup lost the job permanently.
DEFAULT_RETRY_BACKOFF_SECONDS = (1, 5, 30)

# Fractional ± jitter applied to each retry delay. Without it, N workers
# that all failed on the same downstream blip would retry on the same
# second and recreate the spike that caused the failure; 25% spread smears
# the recovery wave while staying inside the schedule's intent.
DEFAULT_RETRY_JITTER_FRACTION = 0.25

__all__ = [
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_RETRY_BACKOFF_SECONDS",
    "DEFAULT_RETRY_JITTER_FRACTION",
]
