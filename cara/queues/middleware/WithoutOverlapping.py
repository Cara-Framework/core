"""Canonical definition of ``WithoutOverlapping``."""

from __future__ import annotations

import contextlib
import os
import time
import uuid
from collections.abc import Callable

from cara.facades import Cache, Log
from cara.queues.contracts.JobThrottledException import JobThrottledException

from .RateLimited import (
    _OVERLAP_SWEEP_EVERY,
    _call_next,
    _overlap_lock,
    _overlap_locks,
    _overlap_sweep_counter,
    _sweep_overlap_locks_locked,
)


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
                # A peer holds the lock, so this copy did NOT run. Returning
                # ``None`` made the worker settle it as ``completed`` — the
                # overlapping copy was DISCARDED and reported as done, which
                # is the exact double-work-prevention this class exists for
                # inverted into silent work LOSS. Come back after the peer's
                # lease could have expired.
                raise JobThrottledException(
                    f"Job {lock_key} skipped: another copy holds the lock",
                    key=lock_key,
                    retry_after=max(1, int(ttl)),
                )
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
                raise JobThrottledException(
                    f"Job {lock_key} skipped: another copy holds the lock",
                    key=lock_key,
                    retry_after=max(1, int(self.expire_after - (now - existing)) + 1),
                )
            _overlap_locks[lock_key] = now

            # Periodic sweep — same shape as the rate-bucket sweep
            # above. Without it, the fallback path leaks one entry
            # per unique lock_key over the worker's lifetime. The
            # try/finally pop() below catches successful completions,
            # but the "skipped" branch above raises out of the
            # ``with _overlap_lock`` block without ever installing an
            # entry of its own, so it leaves nothing to pop. The sweep
            # covers the rare case where a non-cleaned entry survives.
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
            # Probe with a benign call — if the underlying store isn't
            # connected (Redis down, no driver registered), fall back
            # to the in-memory implementation rather than failing the job.
            Cache.has("__cara_overlap_probe__")
            return Cache
        except ImportError, ConnectionError, TimeoutError, OSError, RuntimeError:
            return None

    # Buffer added to the job's per-attempt ``timeout`` when sizing the lock
    # TTL, mirroring the idempotency base lock (max(JOB_LOCK_TTL, timeout+300)).
    _TTL_BUFFER_S = 300

    @staticmethod
    def _new_owner() -> str:
        """Unique per-run lock owner token (matches CacheLock's scheme)."""
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
            except OSError, ConnectionError, TimeoutError, RuntimeError:
                return False

        # Fallback path — has + put. Subject to a TOCTOU window between
        # check and write; acceptable degradation when running on a
        # non-Redis driver (in-memory Cache fakes, etc.).
        try:
            if cache.has(redis_key):
                return False
            cache.put(redis_key, owner, ttl)
            return True
        except OSError, ConnectionError, TimeoutError, RuntimeError:
            return False

    @staticmethod
    def _log_skip(lock_key: str) -> None:
        with contextlib.suppress(ImportError):
            Log.debug(
                "Job %s skipped (overlapping)", lock_key, category="cara.queue.middleware"
            )
