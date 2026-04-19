"""Rate limiting middleware for queue jobs.

Limits how many times a job can run within a time window.

Usage:
    class MyJob(ShouldQueue, Queueable):
        def middleware(self):
            return [RateLimited(max_attempts=10, decay_seconds=60)]
"""

import threading
import time
from typing import Callable, Optional


_rate_buckets = {}
_rate_lock = threading.Lock()

_overlap_locks = {}
_overlap_lock = threading.Lock()


class RateLimited:
    """Rate limit job execution."""
    
    def __init__(self, max_attempts: int = 60, decay_seconds: int = 60, key: Optional[str] = None):
        self.max_attempts = max_attempts
        self.decay_seconds = decay_seconds
        self.key = key
    
    def handle(self, job, next_fn: Callable):
        """Execute middleware logic."""
        rate_key = self.key or job.__class__.__name__
        
        with _rate_lock:
            now = time.time()
            if rate_key not in _rate_buckets:
                _rate_buckets[rate_key] = []
            
            # Remove expired entries
            _rate_buckets[rate_key] = [
                t for t in _rate_buckets[rate_key]
                if now - t < self.decay_seconds
            ]
            
            if len(_rate_buckets[rate_key]) >= self.max_attempts:
                try:
                    from cara.facades import Log
                    Log.warning(f"Job {rate_key} rate limited ({self.max_attempts}/{self.decay_seconds}s)")
                except ImportError:
                    pass
                # Skip execution
                return None
            
            _rate_buckets[rate_key].append(now)
        
        return next_fn(job)


class WithoutOverlapping:
    """Prevent overlapping job execution.
    
    Usage:
        class MyJob(ShouldQueue, Queueable):
            def middleware(self):
                return [WithoutOverlapping(key="my-job-key", expire_after=300)]
    """
    
    def __init__(self, key: Optional[str] = None, expire_after: int = 300):
        self.key = key
        self.expire_after = expire_after
    
    def handle(self, job, next_fn: Callable):
        lock_key = self.key or job.__class__.__name__
        
        with _overlap_lock:
            now = time.time()
            if lock_key in _overlap_locks:
                lock_time = _overlap_locks[lock_key]
                if now - lock_time < self.expire_after:
                    try:
                        from cara.facades import Log
                        Log.debug(f"Job {lock_key} skipped (overlapping)")
                    except ImportError:
                        pass
                    return None
            _overlap_locks[lock_key] = now
        
        try:
            result = next_fn(job)
            return result
        finally:
            with _overlap_lock:
                _overlap_locks.pop(lock_key, None)
