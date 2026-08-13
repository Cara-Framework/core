"""Rate limiting + overlap protection middleware for queue jobs.

Handle methods are async-aware: if ``next_fn(job)`` returns a coroutine it is
awaited inside the middleware, so ``try/except/finally`` blocks observe the
real execution outcome (and not just the coroutine object).

Usage:
    class MyJob(ShouldQueue, Queueable):
        def middleware(self):
            return [
                RateLimited(max_attempts=10, decay_seconds=60),
                WithoutOverlapping(key="my-job-key", expire_after=300),
            ]
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from collections.abc import Callable
from typing import Any

from cara.facades import Log
from cara.queues.contracts.JobThrottledException import JobThrottledException

_rate_buckets: dict = {}
_rate_lock = threading.Lock()
_rate_sweep_counter: int = 0

_overlap_locks: dict = {}
_overlap_lock = threading.Lock()
_overlap_sweep_counter: int = 0

# Sweep stale keys every N operations. Without this, the in-process
# dicts only ever grow — a long-running worker that sees a stream of
# unique rate keys (per-keyword, per-URL, per-tenant) accumulates an
# empty bucket per key forever. Found during scenario 4 load test.
_RATE_SWEEP_EVERY = 500
_OVERLAP_SWEEP_EVERY = 500


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


def _sweep_overlap_locks_locked(now: float) -> None:
    """Drop locks whose ``expire_after`` window has long-since passed.

    Per-key ``expire_after`` is not stored; we use a 24h ceiling for
    the sweep — well past any reasonable lock TTL. Live locks held by
    in-flight jobs use ``time.time()`` timestamps within the last few
    minutes and won't be touched.
    """
    cutoff = now - 86400  # 24h
    dead = [k for k, ts in _overlap_locks.items() if ts < cutoff]
    for k in dead:
        _overlap_locks.pop(k, None)


async def _call_next(next_fn: Callable, job) -> Any:
    """Invoke next_fn and await if it returns a coroutine."""
    result = next_fn(job)
    if asyncio.iscoroutine(result):
        return await result
    return result


class RateLimited:
    """Skip execution when more than ``max_attempts`` runs happen in ``decay_seconds``."""

    def __init__(
        self, max_attempts: int = 60, decay_seconds: int = 60, key: str | None = None
    ):
        self.max_attempts = max_attempts
        self.decay_seconds = decay_seconds
        self.key = key

    async def handle(self, job, next_fn: Callable):
        rate_key = self.key or job.__class__.__name__

        with _rate_lock:
            now = time.time()
            bucket = _rate_buckets.setdefault(rate_key, [])
            # Prune expired hits
            bucket[:] = [t for t in bucket if now - t < self.decay_seconds]

            # Periodic sweep — keeps ``_rate_buckets`` bounded over the
            # life of a long-running worker. Per-key decay only prunes
            # entries when that key is touched again; one-shot keys
            # (per-keyword/URL/tenant) would otherwise live forever.
            global _rate_sweep_counter
            _rate_sweep_counter += 1
            if _rate_sweep_counter >= _RATE_SWEEP_EVERY:
                _rate_sweep_counter = 0
                _sweep_rate_buckets_locked(now)

            if len(bucket) >= self.max_attempts:
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
                oldest = min(bucket)
                raise JobThrottledException(
                    f"Job {rate_key} rate limited ({self.max_attempts}/"
                    f"{self.decay_seconds}s)",
                    key=rate_key,
                    retry_after=max(1, int(self.decay_seconds - (now - oldest)) + 1),
                )

            bucket.append(now)

        return await _call_next(next_fn, job)
