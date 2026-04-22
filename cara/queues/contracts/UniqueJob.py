"""UniqueJob contract — prevents duplicate job dispatches.

Jobs implementing this contract will be deduplicated based on their unique_id().
If a job with the same unique_id is already pending or processing, the new dispatch
is silently dropped.

Locks are Cache-backed (Redis/distributed) for multi-worker support, with an
in-memory fallback for single-process or early-boot scenarios.

Usage:
    class RefreshProductJob(ShouldQueue, Queueable, UniqueJob):
        def __init__(self, product_id):
            self.product_id = product_id

        def unique_id(self) -> str:
            return f"refresh_product_{self.product_id}"

        @property
        def unique_for(self) -> int:
            return 3600  # seconds — lock expires after 1 hour
"""

import time
import threading
from typing import Optional


# Fallback in-memory lock store (for single-process or boot scenarios)
_unique_locks = {}
_unique_lock = threading.Lock()


class UniqueJob:
    """Mixin for jobs that should only run one instance at a time."""

    def unique_id(self) -> str:
        """Return a unique identifier for this job instance.
        Must be overridden by subclasses.
        """
        raise NotImplementedError("UniqueJob must implement unique_id()")

    @property
    def unique_for(self) -> int:
        """How long the uniqueness lock should be held (seconds). Default: 3600."""
        return 3600

    @classmethod
    def is_unique_locked(cls, unique_id: str) -> bool:
        """Check if a job with this unique_id is already locked.

        Tries Cache-backed lock first, falls back to in-memory for boot scenarios.
        """
        cache_key = f"unique_job:{unique_id}"

        # Try Cache first (distributed)
        try:
            from cara.facades import Cache
            if Cache.get(cache_key) is not None:
                return True
        except ImportError:
            pass

        # Fallback: in-memory lock
        with _unique_lock:
            if unique_id in _unique_locks:
                lock_time, ttl = _unique_locks[unique_id]
                if time.time() - lock_time < ttl:
                    return True
                # Lock expired, remove it
                del _unique_locks[unique_id]
            return False

    @classmethod
    def acquire_unique_lock(cls, unique_id: str, ttl: int = 3600) -> bool:
        """Try to acquire a uniqueness lock. Returns True if acquired.

        Uses Cache.add() for atomic distributed locking, falls back to
        in-memory threading lock for boot scenarios.
        """
        cache_key = f"unique_job:{unique_id}"

        # Try Cache first (atomic add = acquire if not exists)
        try:
            from cara.facades import Cache
            # Cache.add returns True if key was not set (= acquired lock)
            if Cache.add(cache_key, "1", ttl):
                return True
            # Lock already exists in Cache
            return False
        except ImportError:
            pass

        # Fallback: in-memory lock
        with _unique_lock:
            if unique_id in _unique_locks:
                lock_time, lock_ttl = _unique_locks[unique_id]
                if time.time() - lock_time < lock_ttl:
                    return False  # Already locked
                # Expired lock, replace it
            _unique_locks[unique_id] = (time.time(), ttl)
            return True

    @classmethod
    def release_unique_lock(cls, unique_id: str) -> None:
        """Release a uniqueness lock.

        Removes from Cache (distributed) and in-memory fallback.
        """
        cache_key = f"unique_job:{unique_id}"

        # Try Cache first
        try:
            from cara.facades import Cache
            Cache.forget(cache_key)
        except ImportError:
            pass

        # Also remove from in-memory fallback
        with _unique_lock:
            _unique_locks.pop(unique_id, None)

    @classmethod
    def cleanup_expired_locks(cls) -> int:
        """Remove expired locks from in-memory store. Returns count removed.

        Note: Cache-backed locks are auto-expired by TTL, no cleanup needed.
        """
        now = time.time()
        removed = 0
        with _unique_lock:
            expired = [
                uid for uid, (lock_time, ttl) in _unique_locks.items()
                if now - lock_time >= ttl
            ]
            for uid in expired:
                del _unique_locks[uid]
                removed += 1
        return removed
