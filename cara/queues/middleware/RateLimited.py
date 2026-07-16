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
                try:
                    from cara.facades import Log

                    Log.warning("Job %s rate limited (%s/%ss)", rate_key, self.max_attempts, self.decay_seconds, category='cara.queue.middleware')
                except ImportError:
                    pass
                return None

            bucket.append(now)

        return await _call_next(next_fn, job)


class WithoutOverlapping:
    """Ensure only one instance of a job runs at a time for the given key.

    Uses Redis (via the ``Cache`` facade) when available so the lock is
    effective across worker processes and pods — the in-memory dict
    only protects against overlap inside a single worker process and a
    multi-pod deploy can still double-fire. Falls back to the
    process-local dict when Cache isn't bootable (tests, CLI without
    full container).

    The Redis path uses a SET-NX with TTL (atomic key creation) so two
    workers racing on the same lock_key are guaranteed to have exactly
    one acquirer; the other gets a cache hit and skips. TTL == ``expire_after``
    so a crashed worker can't pin the lock forever.
    """

    REDIS_KEY_PREFIX = "cara:overlap:"

    def __init__(self, key: str | None = None, expire_after: int = 300):
        self.key = key
        self.expire_after = expire_after

    async def handle(self, job, next_fn: Callable):
        lock_key = self.key or job.__class__.__name__
        redis_key = f"{self.REDIS_KEY_PREFIX}{lock_key}"

        cache = self._resolve_cache()
        if cache is not None:
            # Owner-fenced distributed lock: acquire with a UNIQUE per-run
            # ``{pid}:{uuid}`` owner token (atomic SET-NX) and release with
            # ``forget_if(key, owner)`` (atomic compare-and-delete). The old
            # ``add("1")`` + bare ``forget(key)`` had NO owner fence, so a job
            # that overran its TTL (the lock then lapsed and a peer re-acquired)
            # would delete the peer's freshly-acquired lock on ``finally`` —
            # letting a second copy run concurrently (double-fire on the
            # user-facing sweeps this guards). The TTL is also sized ABOVE the
            # job's own ``timeout`` so the lock can't lapse mid-run in the first
            # place (see ``_effective_ttl``).
            owner = self._new_owner()
            ttl = self._effective_ttl(job)
            if not self._try_acquire(cache, redis_key, owner, ttl):
                self._log_skip(lock_key)
                return None
            try:
                return await _call_next(next_fn, job)
            finally:
                with contextlib.suppress(
                    OSError, ConnectionError, TimeoutError, RuntimeError
                ):
                    cache.forget_if(redis_key, owner)
            return None  # unreachable, satisfies type-checkers

        # Cache facade isn't available — fall back to the process-local dict.
        with _overlap_lock:
            now = time.time()
            existing = _overlap_locks.get(lock_key)
            if existing is not None and now - existing < self.expire_after:
                self._log_skip(lock_key)
                return None
            _overlap_locks[lock_key] = now

            # Periodic sweep — same shape as the rate-bucket sweep
            # above. Without it, the fallback path leaks one entry
            # per unique lock_key over the worker's lifetime. The
            # try/finally pop() below catches successful completions,
            # but a job that exits via ``return None`` from inside the
            # ``with _overlap_lock`` block (the "skipped" branch
            # above) leaves no entry to pop. The sweep covers the
            # rare case where a non-cleaned entry survives.
            global _overlap_sweep_counter
            _overlap_sweep_counter += 1
            if _overlap_sweep_counter >= _OVERLAP_SWEEP_EVERY:
                _overlap_sweep_counter = 0
                _sweep_overlap_locks_locked(now)

        try:
            return await _call_next(next_fn, job)
        finally:
            with _overlap_lock:
                _overlap_locks.pop(lock_key, None)

    @staticmethod
    def _resolve_cache():
        """Resolve the Cache facade lazily. Returns None when the
        application container is not yet bound (early boot, isolated
        unit tests) so the middleware is still usable in those paths."""
        try:
            from cara.facades import Cache

            # Probe with a benign call — if the underlying store isn't
            # connected (Redis down, no driver registered), fall back
            # to the in-memory implementation rather than failing the job.
            Cache.has("__cara_overlap_probe__")
            return Cache
        except (ImportError, ConnectionError, TimeoutError, OSError, RuntimeError):
            return None

    # Buffer added to the job's per-attempt ``timeout`` when sizing the lock
    # TTL, mirroring the idempotency base lock (max(JOB_LOCK_TTL, timeout+300)).
    _TTL_BUFFER_S = 300

    @staticmethod
    def _new_owner() -> str:
        """Unique per-run lock owner token (matches CacheLock's scheme)."""
        import os
        import uuid

        return f"{os.getpid()}:{uuid.uuid4().hex}"

    def _effective_ttl(self, job) -> int:
        """Lock TTL that outlasts the job so the guard never lapses mid-run.

        ``expire_after`` is the caller's floor; the job's own per-attempt
        ``timeout`` (its hard-kill window) is the real upper bound on runtime,
        so the lock must live at least ``timeout + buffer``. Pre-fix
        ``expire_after`` alone (e.g. 900s on a heavy sweep that can run longer)
        let the lock TTL-expire mid-run so a second copy fired."""
        job_timeout = int(getattr(job, "timeout", 0) or 0)
        return max(int(self.expire_after), job_timeout + self._TTL_BUFFER_S)

    def _try_acquire(self, cache, redis_key: str, owner: str, ttl: int) -> bool:
        """Atomic owner-fenced acquire via ``add`` (SET-NX) storing OUR owner
        token so ``forget_if`` can later release only our own lock. Falls back
        to ``has`` + ``put`` only for drivers without ``add`` (a narrow TOCTOU
        acceptable for those non-Redis fakes). A cache/Redis blip is treated as
        'held' (skip), matching the prior best-effort behaviour."""
        add = getattr(cache, "add", None)
        if callable(add):
            try:
                return bool(add(redis_key, owner, ttl))
            except (OSError, ConnectionError, TimeoutError, RuntimeError):
                return False

        # Fallback path — has + put. Subject to a TOCTOU window between
        # check and write; acceptable degradation when running on a
        # non-Redis driver (in-memory Cache fakes, etc.).
        try:
            if cache.has(redis_key):
                return False
            cache.put(redis_key, owner, ttl)
            return True
        except (OSError, ConnectionError, TimeoutError, RuntimeError):
            return False

    @staticmethod
    def _log_skip(lock_key: str) -> None:
        try:
            from cara.facades import Log

            Log.debug("Job %s skipped (overlapping)", lock_key, category='cara.queue.middleware')
        except ImportError:
            pass
