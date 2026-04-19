"""UniqueJob contract — prevents duplicate job dispatches.

Jobs implementing this contract will be deduplicated based on their unique_id().
If a job with the same unique_id is already pending or processing, the new dispatch
is silently dropped.

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


# Simple in-memory lock store (for single-process workers)
# Production should use Redis, but this works for single-worker setups
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
        """Check if a job with this unique_id is already locked."""
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
        """Try to acquire a uniqueness lock. Returns True if acquired."""
        with _unique_lock:
            if unique_id in _unique_locks:
                lock_time, lock_ttl = _unique_locks[unique_id]
                if time.time() - lock_time < lock_ttl:
                    return False  # Already locked
                # Expired lock
            _unique_locks[unique_id] = (time.time(), ttl)
            return True
    
    @classmethod
    def release_unique_lock(cls, unique_id: str) -> None:
        """Release a uniqueness lock."""
        with _unique_lock:
            _unique_locks.pop(unique_id, None)
    
    @classmethod
    def cleanup_expired_locks(cls) -> int:
        """Remove expired locks. Returns count of removed locks."""
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
