"""Exception throttling middleware for queue jobs.

Tracks exception frequency per key and temporarily skips jobs after max failures
within a decay window. Useful for gracefully degrading when downstream services
are unavailable (e.g., AI API rate limits, database failures).

Async-aware: when ``next_fn(job)`` returns a coroutine it is awaited inside the
middleware, so exceptions surface during execution (not just coroutine creation).

Usage:
    class MyJob(ShouldQueue, Queueable):
        def middleware(self):
            return [ThrottlesExceptions(max_exceptions=3, decay_seconds=60, retry_after=300)]
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import threading
import time
from collections.abc import Callable
from typing import Any

from cara.context import ExecutionContext
from cara.facades import Cache, Log
from cara.queues.contracts import JobThrottledException
from cara.queues.middleware._SharedLimitOutage import _SharedLimitOutage

# The in-process failure history and gates: the fallback while the shared
# store cannot answer (see ``ThrottlesExceptions._shared_failure``). They are
# kept on both paths, so a fallback starts from what this process has seen
# rather than from a clean slate that would reopen a shut gate.
_exception_buckets: dict = {}
_throttle_gates: dict = {}
_exception_lock = threading.Lock()
_outage = _SharedLimitOutage("Job exception throttle")


async def _call_next(next_fn: Callable, job) -> Any:
    """Invoke next_fn and await if it returns a coroutine."""
    result = next_fn(job)
    if asyncio.iscoroutine(result):
        return await result
    return result


def _says_nothing_about_downstream(exc: BaseException) -> bool:
    """Whether ``exc`` is an outcome this gate must not count as a failure.

    The gate exists to stand a job class down while what it calls is broken.
    Two families of exception are no evidence of that:

    * a throttle (``is_throttle``) — a gate below refused the run or the
      idempotency layer put it back; the body did not fail, it did not run.
      Three ``ConcurrencyExceeded`` in a minute used to shut this gate for
      ``retry_after`` although nothing downstream had answered at all;
    * a permanent, per-job outcome (``do_not_retry``) — a cancellation, an
      entity that no longer exists. It will fail identically on any retry and
      says nothing about the next job of the class.
    """
    return bool(getattr(exc, "is_throttle", False) or getattr(exc, "do_not_retry", False))


def _failures_key(throttle_key: str) -> str:
    """The cache counter holding a key's failures in the current window."""
    return f"cara:throttles-exceptions:failures:{throttle_key}"


def _gate_key(throttle_key: str) -> str:
    """The cache key holding the instant a key's shut gate reopens."""
    return f"cara:throttles-exceptions:gate:{throttle_key}"


class ThrottlesExceptions:
    """Throttle jobs after max exceptions within a decay window.

    When ``max_exceptions`` failures occur within ``decay_seconds``, the job
    raises :class:`JobThrottledException` until ``retry_after`` seconds have
    passed since the failure that shut the gate.

    The failure count and the gate are SHARED by every worker process: each
    failure increments one atomic cache counter whose window opens at the
    first failure, and the gate is one cache entry holding the instant it
    reopens. They
    used to live in each process's memory, so one process stood down while its
    siblings kept hammering the broken downstream, and N processes needed N
    times the failures to stand the class down at all.

    Every stamp is the moment the failure HAPPENED. Stamping the moment the
    failing job started made the gate ``retry_after`` minus the body's own
    runtime: a scrape that hung 180s before failing shut a 300s gate for 120s,
    and one that ran longer than ``retry_after`` never shut it at all.
    """

    def __init__(
        self,
        max_exceptions: int = 3,
        decay_seconds: int = 60,
        retry_after: int = 300,
        key: str | None = None,
    ):
        self.max_exceptions = max_exceptions
        self.decay_seconds = decay_seconds
        self.retry_after = retry_after
        self.key = key

    async def handle(self, job, next_fn: Callable):
        throttle_key = self.key or job.__class__.__name__

        remaining = await self._seconds_until_gate_opens_anywhere(throttle_key)
        if remaining > 0:
            # The caller is told when the gate REOPENS, never the gate's whole
            # length: the worker treats ``retry_after`` as the floor of the
            # redelivery delay, so a job arriving one second before the gate
            # opened was sent away for the full ``retry_after`` again.
            wait = max(1, math.ceil(remaining))
            with contextlib.suppress(ImportError, RuntimeError):
                Log.warning(
                    "Job %s throttled (%s/%ss, retry in %ss)",
                    throttle_key,
                    self.max_exceptions,
                    self.decay_seconds,
                    wait,
                    category="cara.queue.middleware",
                )
            raise JobThrottledException(
                f"Job {throttle_key} throttled",
                key=throttle_key,
                retry_after=wait,
            )

        try:
            result = await _call_next(next_fn, job)
        except Exception as exc:
            if _says_nothing_about_downstream(exc):
                raise
            failed_at = time.time()
            with _exception_lock:
                bucket = _exception_buckets.setdefault(throttle_key, [])
                bucket.append(failed_at)
                bucket[:] = [t for t in bucket if failed_at - t < self.decay_seconds]
                shut_here = len(bucket) >= self.max_exceptions
                if shut_here:
                    _throttle_gates[throttle_key] = failed_at
            shut_shared = await self._shared_failure(throttle_key, failed_at)
            gate_shut = shut_here if shut_shared is None else shut_shared
            if gate_shut:
                with contextlib.suppress(ImportError, RuntimeError):
                    Log.warning(
                        "Job %s throttled after %s exceptions in %ss",
                        throttle_key,
                        self.max_exceptions,
                        self.decay_seconds,
                        category="cara.queue.middleware",
                    )
            raise

        # Success — prune the bucket.
        finished_at = time.time()
        with _exception_lock:
            bucket = _exception_buckets.get(throttle_key)
            if bucket is not None:
                bucket[:] = [t for t in bucket if finished_at - t < self.decay_seconds]

        return result

    async def _seconds_until_gate_opens_anywhere(self, throttle_key: str) -> float:
        """Seconds the key's shared gate stays shut; this process's own gate
        when the shared store cannot answer."""
        try:
            # Synchronous cache I/O: off the event loop, so a slow cache
            # stalls this job only.
            reopens_at = await ExecutionContext.run_in_thread(
                Cache.get, _gate_key(throttle_key)
            )
        except Exception as exc:  # noqa: BLE001 — any store fault falls back below
            _outage.failed(throttle_key, exc)
            with _exception_lock:
                return self._seconds_until_gate_opens(throttle_key, time.time())
        _outage.answered()
        if reopens_at is None:
            return 0.0
        try:
            # The instant is the shutting worker's clock, read on this one's:
            # hosts kept in step by NTP differ by milliseconds, far below the
            # one-second granularity the refusal is reported in.
            return max(float(reopens_at) - time.time(), 0.0)
        except TypeError, ValueError:
            return 0.0

    async def _shared_failure(self, throttle_key: str, failed_at: float) -> bool | None:
        """Count one failure against the shared budget; whether it shut the gate.

        ``None`` when the shared store cannot answer: the caller then goes by
        this process's own count, recorded just before. Neither failing
        alternative fits this gate — treating an outage as "shut" would stand
        every job class down for a cache fault, and treating it as "open"
        would stop counting failures while the downstream may be the very
        thing that is broken. The per-process count is what the gate was
        before it was shared.
        """
        try:
            shut = await ExecutionContext.run_in_thread(
                self._count_shared_failure, throttle_key, failed_at
            )
        except Exception as exc:  # noqa: BLE001 — any store fault falls back
            _outage.failed(throttle_key, exc)
            return None
        _outage.answered()
        if shut:
            # Mirrored here, so an outage that starts while the gate is shut
            # does not reopen it for this process.
            with _exception_lock:
                _throttle_gates[throttle_key] = failed_at
        return shut

    def _count_shared_failure(self, throttle_key: str, failed_at: float) -> bool:
        """Count the failure in the key's shared window; whether it shut the gate.

        The window is an atomic cache counter whose TTL is set by the FIRST
        failure only (``Cache.increment`` never extends it), so "N failures
        within ``decay_seconds``" holds however the N are spaced. A GCRA
        bucket was tried and rejected here: failures at or below its refill
        pace never emptied it, so a breaker of 3 per 300s never tripped on a
        failure every 100s — looser than the breaker it replaced.

        The failure that reaches N shuts the gate for every worker and clears
        the counter, so the first failure after it reopens starts a new count.
        Failures still landing from runs in flight when it shut count toward
        that new window.
        """
        failures_key = _failures_key(throttle_key)
        count = Cache.increment(
            failures_key, 1, ttl=max(1, math.ceil(self.decay_seconds))
        )
        if count < self.max_exceptions:
            return False
        Cache.put(
            _gate_key(throttle_key),
            failed_at + self.retry_after,
            math.ceil(self.retry_after) + 1,
        )
        Cache.forget(failures_key)
        return True

    def _seconds_until_gate_opens(self, throttle_key: str, now: float) -> float:
        """Seconds this process's gate for the key stays shut; ``0`` when open.

        An expired gate is cleared with its failure history, so the key starts
        counting afresh. Call with ``_exception_lock`` held.
        """
        gate_time = _throttle_gates.get(throttle_key)
        if gate_time is None:
            return 0.0
        remaining = self.retry_after - (now - gate_time)
        if remaining > 0:
            return remaining
        _throttle_gates.pop(throttle_key, None)
        _exception_buckets.pop(throttle_key, None)
        return 0.0
