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

import asyncio
import threading
import time
from typing import Any, Callable, Optional


_exception_buckets: dict = {}
_throttle_gates: dict = {}
_exception_lock = threading.Lock()


async def _call_next(next_fn: Callable, job) -> Any:
    """Invoke next_fn and await if it returns a coroutine."""
    result = next_fn(job)
    if asyncio.iscoroutine(result):
        return await result
    return result


class ThrottlesExceptions:
    """Throttle jobs after max exceptions within a decay window.

    Records exception timestamps per key. When ``max_exceptions`` failures occur
    within ``decay_seconds``, the job is skipped (returns None) until
    ``retry_after`` seconds have passed since the throttle gate activated.
    """

    def __init__(
        self,
        max_exceptions: int = 3,
        decay_seconds: int = 60,
        retry_after: int = 300,
        key: Optional[str] = None,
    ):
        self.max_exceptions = max_exceptions
        self.decay_seconds = decay_seconds
        self.retry_after = retry_after
        self.key = key

    async def handle(self, job, next_fn: Callable):
        throttle_key = self.key or job.__class__.__name__
        now = time.time()

        with _exception_lock:
            gate_time = _throttle_gates.get(throttle_key)
            if gate_time is not None:
                if now - gate_time < self.retry_after:
                    try:
                        from cara.facades import Log

                        Log.warning(
                            f"Job {throttle_key} throttled "
                            f"({self.max_exceptions}/{self.decay_seconds}s, "
                            f"retry in {self.retry_after}s)",
                            category="cara.queue.middleware",
                        )
                    except ImportError:
                        pass
                    return None
                # Gate expired — reset tracking.
                _throttle_gates.pop(throttle_key, None)
                _exception_buckets.pop(throttle_key, None)

            _exception_buckets.setdefault(throttle_key, [])

        try:
            result = await _call_next(next_fn, job)
        except Exception:
            with _exception_lock:
                bucket = _exception_buckets.setdefault(throttle_key, [])
                bucket.append(now)
                bucket[:] = [t for t in bucket if now - t < self.decay_seconds]

                if len(bucket) >= self.max_exceptions:
                    _throttle_gates[throttle_key] = now
                    try:
                        from cara.facades import Log

                        Log.warning(
                            f"Job {throttle_key} throttled after "
                            f"{self.max_exceptions} exceptions in {self.decay_seconds}s",
                            category="cara.queue.middleware",
                        )
                    except ImportError:
                        pass
            raise

        # Success — prune the bucket.
        with _exception_lock:
            bucket = _exception_buckets.get(throttle_key)
            if bucket is not None:
                bucket[:] = [t for t in bucket if now - t < self.decay_seconds]

        return result
