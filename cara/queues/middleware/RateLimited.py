"""Rate limiting middleware for queue jobs.

Handle methods are async-aware: if ``next_fn(job)`` returns a coroutine it is
awaited inside the middleware, so ``try/except/finally`` blocks observe the
real execution outcome (and not just the coroutine object).

Usage:
    class MyJob(ShouldQueue, Queueable):
        def middleware(self):
            return [RateLimited(max_attempts=10, decay_seconds=60)]
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from collections.abc import Callable
from typing import Any

from cara.context import ExecutionContext
from cara.exceptions import ServiceUnavailableException
from cara.facades import Log
from cara.queues.contracts.JobThrottledException import JobThrottledException
from cara.queues.middleware._SharedLimitOutage import _SharedLimitOutage
from cara.rates import Limit, RateLimitDecision, attempt_rate_limit

# The in-process bucket: the limit's fallback while the shared store cannot
# answer (see ``RateLimited.handle``). Every admitted run is recorded here on
# both paths, so a fallback starts from this process's real recent runs
# instead of an empty bucket that would hand the process a fresh budget.
_rate_buckets: dict = {}
_rate_lock = threading.Lock()
_rate_sweep_counter: int = 0
_outage = _SharedLimitOutage("Job rate limit")

# Sweep stale keys every N operations. Without this, the in-process
# dict only ever grows — a long-running worker that sees a stream of
# unique rate keys (per-keyword, per-URL, per-tenant) accumulates an
# empty bucket per key forever. Found during scenario 4 load test.
_RATE_SWEEP_EVERY = 500


def _sweep_rate_buckets_locked(now: float) -> None:
    """Drop empty/stale buckets. Caller must hold ``_rate_lock``.

    A bucket is dead when none of its timestamps are within the
    longest decay window we've seen. We don't track per-key
    decay_seconds (callers can pass different values for the same
    rate_key), so use a generous 24h ceiling — enough to reclaim
    abandoned keys, short enough to bound the dict size in practice.
    """
    cutoff = now - 86400  # 24h
    dead = [k for k, ts in _rate_buckets.items() if not ts or ts[-1] < cutoff]
    for k in dead:
        _rate_buckets.pop(k, None)


async def _call_next(next_fn: Callable, job) -> Any:
    """Invoke next_fn and await if it returns a coroutine."""
    result = next_fn(job)
    if asyncio.iscoroutine(result):
        return await result
    return result


def _shared_bucket_key(rate_key: str) -> str:
    """The rate-authority bucket ``RateLimited(key=rate_key)`` spends."""
    return f"queue:rate_limited:{rate_key}"


class RateLimited:
    """Refuse a run when the key has spent ``max_attempts`` runs per ``decay_seconds``.

    The budget is ONE budget for every worker process: each run spends from
    the shared rate authority (``cara.rates``, an atomic GCRA cell in the
    cache), keyed by ``key`` or the job's class name. It used to be a list in
    each process's memory, so N worker processes ran N times the limit — and a
    limit that guards a marketplace API's quota did not hold.

    "``max_attempts`` per ``decay_seconds``" is a ``Limit``'s meaning: a key
    may run ``max_attempts`` times back to back, then one run every
    ``decay_seconds / max_attempts`` seconds as the bucket refills. A refused
    job is told when the next run fits, not when a whole window ends.
    """

    def __init__(
        self, max_attempts: int = 60, decay_seconds: int = 60, key: str | None = None
    ):
        self.max_attempts = max_attempts
        self.decay_seconds = decay_seconds
        self.key = key

    async def handle(self, job, next_fn: Callable):
        rate_key = self.key or job.__class__.__name__
        limit = Limit(self.max_attempts, self.decay_seconds).by(
            _shared_bucket_key(rate_key)
        )
        decision = await self._spend_shared(rate_key, limit)

        with _rate_lock:
            now = time.time()

            # Periodic sweep — keeps ``_rate_buckets`` bounded over the
            # life of a long-running worker. Per-key decay only prunes
            # entries when that key is touched again; one-shot keys
            # (per-keyword/URL/tenant) would otherwise live forever.
            #
            # It runs BEFORE this call takes its bucket: the sweep pops every
            # empty bucket, and run after ``setdefault`` it popped the one
            # just created (or just pruned empty), so the hit below landed in
            # a list the dict no longer held and the limit let one extra run
            # through.
            global _rate_sweep_counter
            _rate_sweep_counter += 1
            if _rate_sweep_counter >= _RATE_SWEEP_EVERY:
                _rate_sweep_counter = 0
                _sweep_rate_buckets_locked(now)

            bucket = _rate_buckets.setdefault(rate_key, [])
            # Prune expired hits
            bucket[:] = [t for t in bucket if now - t < self.decay_seconds]

            if decision is None:
                if len(bucket) >= self.max_attempts:
                    oldest = min(bucket)
                    self._refuse(
                        rate_key,
                        max(1, int(self.decay_seconds - (now - oldest)) + 1),
                    )
            elif not decision.allowed:
                self._refuse(rate_key, max(1, decision.retry_after))

            bucket.append(now)

        return await _call_next(next_fn, job)

    @staticmethod
    async def _spend_shared(rate_key: str, limit: Limit) -> RateLimitDecision | None:
        """The shared authority's decision; ``None`` when it cannot answer.

        ``None`` sends this one call to the in-process bucket. Neither failing
        alternative is acceptable for a job limit: refusing every job would
        stop all work of the class for the length of a cache outage (the
        HTTP throttle answers 503 instead, which a client retries in a
        second; a job would be shelved for its whole backoff), and letting
        runs through uncounted lifts the limit entirely. The process's own
        bucket holds the limit at what it was before the budget was shared —
        per process — until the store answers again.
        """
        try:
            # A synchronous Redis EVAL: off the event loop, so a slow cache
            # stalls this job only and never blocks the worker's other jobs.
            decision = await ExecutionContext.run_in_thread(attempt_rate_limit, limit)
        except ServiceUnavailableException as exc:
            _outage.failed(rate_key, exc.__cause__ or exc)
            return None
        _outage.answered()
        return decision

    def _refuse(self, rate_key: str, retry_after: int) -> None:
        with contextlib.suppress(ImportError, RuntimeError):
            Log.warning(
                "Job %s rate limited (%s/%ss)",
                rate_key,
                self.max_attempts,
                self.decay_seconds,
                category="cara.queue.middleware",
            )
        # The job did NOT run. Pre-fix this returned ``None``, which
        # the worker cannot tell from a successful handler (``None``
        # is the normal success return — see ``Bus._run_sync``), so it
        # settled the delivery as ``completed`` and ACKed: with two
        # jobs over one limit, one was silently discarded and
        # REPORTED AS DONE. Raising the throttle signal routes it
        # through the starvation lane instead — frozen failure budget,
        # escalating backoff, ``throttled`` in the ledger.
        raise JobThrottledException(
            f"Job {rate_key} rate limited ({self.max_attempts}/{self.decay_seconds}s)",
            key=rate_key,
            retry_after=retry_after,
        )
