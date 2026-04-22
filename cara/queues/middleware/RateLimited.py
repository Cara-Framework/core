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

import asyncio
import threading
import time
from typing import Any, Callable, Optional


_rate_buckets: dict = {}
_rate_lock = threading.Lock()

_overlap_locks: dict = {}
_overlap_lock = threading.Lock()


async def _call_next(next_fn: Callable, job) -> Any:
    """Invoke next_fn and await if it returns a coroutine."""
    result = next_fn(job)
    if asyncio.iscoroutine(result):
        return await result
    return result


class RateLimited:
    """Skip execution when more than ``max_attempts`` runs happen in ``decay_seconds``."""

    def __init__(self, max_attempts: int = 60, decay_seconds: int = 60, key: Optional[str] = None):
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

            if len(bucket) >= self.max_attempts:
                try:
                    from cara.facades import Log

                    Log.warning(
                        f"Job {rate_key} rate limited "
                        f"({self.max_attempts}/{self.decay_seconds}s)",
                        category="cara.queue.middleware",
                    )
                except Exception:
                    pass
                return None

            bucket.append(now)

        return await _call_next(next_fn, job)


class WithoutOverlapping:
    """Ensure only one instance of a job runs at a time for the given key."""

    def __init__(self, key: Optional[str] = None, expire_after: int = 300):
        self.key = key
        self.expire_after = expire_after

    async def handle(self, job, next_fn: Callable):
        lock_key = self.key or job.__class__.__name__

        with _overlap_lock:
            now = time.time()
            existing = _overlap_locks.get(lock_key)
            if existing is not None and now - existing < self.expire_after:
                try:
                    from cara.facades import Log

                    Log.debug(
                        f"Job {lock_key} skipped (overlapping)",
                        category="cara.queue.middleware",
                    )
                except Exception:
                    pass
                return None
            _overlap_locks[lock_key] = now

        try:
            return await _call_next(next_fn, job)
        finally:
            with _overlap_lock:
                _overlap_locks.pop(lock_key, None)
