"""Single source of truth for the framework's default retry policy.

The production worker derives its attempt budget + backoff schedule here so
job classes and the worker never restate the defaults.

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

# Max THROTTLE re-deliveries before a job is dead-lettered. A throttle
# ("the job never got a slot") deliberately does not burn the delivery
# budget above, otherwise healthy jobs would DLQ purely from losing the
# slot lottery — but "does not count" was implemented as "never advances",
# so a throttled job re-queued itself forever at a frozen 1s backoff,
# appending two DB writes and a broker publish to the durable outbox every
# cycle with no terminal signal to operators. The throttle lane therefore
# gets its OWN finite budget, indexed by its OWN counter, and reuses
# DEFAULT_RETRY_BACKOFF_SECONDS so a saturated key escalates 1s → 5s → 30s
# instead of hot-spinning. 50 attempts ≈ 25 minutes at the 30s ceiling:
# generous enough that a transient capacity dip never dead-letters, finite
# enough that sustained starvation reaches the DLQ where it is visible.
# A job overrides per-job with a class-level ``max_throttle_attempts``.
DEFAULT_MAX_THROTTLE_ATTEMPTS = 50

# Fractional ± jitter applied to each retry delay. Without it, N workers
# that all failed on the same downstream blip would retry on the same
# second and recreate the spike that caused the failure; 25% spread smears
# the recovery wave while staying inside the schedule's intent.
DEFAULT_RETRY_JITTER_FRACTION = 0.25

__all__ = [
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_THROTTLE_ATTEMPTS",
    "DEFAULT_RETRY_BACKOFF_SECONDS",
    "DEFAULT_RETRY_JITTER_FRACTION",
]
