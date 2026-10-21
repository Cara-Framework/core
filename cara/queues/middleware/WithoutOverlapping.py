"""Canonical definition of ``WithoutOverlapping``."""

from __future__ import annotations

import builtins
import contextlib
import os
import threading
import time
import uuid
from collections.abc import Callable

from cara.cache import cache_backend_fault_types
from cara.exceptions import MissingContainerBindingException
from cara.facades import Log
from cara.queues.contracts.JobThrottledException import JobThrottledException

from .RateLimited import _call_next

_overlap_locks: dict = {}
_overlap_lock = threading.Lock()
_overlap_sweep_counter: int = 0

# Sweep stale keys every N operations. Without this, the in-process
# dict only ever grows — a long-running worker that sees a stream of
# unique lock keys accumulates an entry per key forever.
_OVERLAP_SWEEP_EVERY = 500


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


class WithoutOverlapping:
    """Ensure only one instance of a job runs at a time for the given key.

    Uses Redis (via the bound ``cache`` service) when available so the lock
    is effective across worker processes and pods — the in-memory dict
    only protects against overlap inside a single worker process and a
    multi-pod deploy can still double-fire. Falls back to the
    process-local dict only when there is NO cache binding (tests, CLI
    without full container).

    The Redis path uses a SET-NX with TTL (atomic key creation) so two
    workers racing on the same lock_key are guaranteed to have exactly
    one acquirer; the other gets a cache hit and skips. TTL == ``expire_after``
    so a crashed worker can't pin the lock forever.

    A backend FAULT is not absence (DOCTRINE §9 — fail closed): a Redis
    outage during acquire throttles the job like a held lock, because
    degrading to the process-local dict would let every pod run its own
    copy. An outage during release never replaces the job's real outcome;
    the lock TTL reclaims the key.
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
                # A peer holds the lock (or the backend could not say it
                # does not), so this copy did NOT run. Returning
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
                # A raise out of this ``finally`` would REPLACE the body's
                # outcome: a job that finished would settle as failed and run
                # again. The lock carries a TTL, so a lost release only delays
                # the next copy.
                try:
                    cache.forget_if(redis_key, owner)
                except cache_backend_fault_types() as exc:
                    self._log_backend_fault(redis_key, "release", exc)

        # No cache binding at all — fall back to the process-local dict.
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

            # Periodic sweep — same shape as ``RateLimited``'s
            # rate-bucket sweep. Without it, the fallback path leaks one entry
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
        """The bound cache service, or None when there is none to bind.

        None means ABSENCE only — the application container is not yet bound
        (early boot, isolated unit tests) — so the middleware stays usable on
        the process-local path. It used to probe Redis here and treat a
        failed probe as absence too, which silently dropped the cross-pod
        lock during an outage; the acquire itself is now the only probe and
        a fault there throttles (see ``_try_acquire``)."""
        try:
            return builtins.app().make("cache")
        except (
            ImportError,
            RuntimeError,
            AttributeError,
            MissingContainerBindingException,
        ):
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
        acceptable for those non-Redis fakes). A backend fault is treated as
        'held' (throttle, not the failure budget) and logged at WARNING so an
        outage-driven throttle loop is visible."""
        add = getattr(cache, "add", None)
        if callable(add):
            try:
                return bool(add(redis_key, owner, ttl))
            except cache_backend_fault_types() as exc:
                self._log_backend_fault(redis_key, "acquire", exc)
                return False

        # Fallback path — has + put. Subject to a TOCTOU window between
        # check and write; acceptable degradation when running on a
        # non-Redis driver (in-memory Cache fakes, etc.).
        try:
            if cache.has(redis_key):
                return False
            cache.put(redis_key, owner, ttl)
            return True
        except cache_backend_fault_types() as exc:
            self._log_backend_fault(redis_key, "acquire", exc)
            return False

    @staticmethod
    def _log_skip(lock_key: str) -> None:
        with contextlib.suppress(ImportError):
            Log.debug(
                "Job %s skipped (overlapping)", lock_key, category="cara.queue.middleware"
            )

    @staticmethod
    def _log_backend_fault(redis_key: str, phase: str, exc: BaseException) -> None:
        with contextlib.suppress(ImportError):
            Log.warning(
                "overlap lock %s for %s failed — cache backend unavailable: %s",
                phase,
                redis_key,
                exc,
                category="cara.queue.middleware",
                exc_info=exc,
            )
