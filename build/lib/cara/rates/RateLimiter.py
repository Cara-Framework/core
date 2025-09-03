"""
Fixed-Window Rate Limiter for the Cara framework.

This module implements a fixed-window rate limiting algorithm using the cache system, enforcing
request limits per key within a time window.
"""

import time
from typing import Tuple

from cara.facades import Cache
from cara.rates.contracts import RateLimit


class RateLimiter(RateLimit):
    """
    Fixed‐window rate limiter.

    Uses the 'cache' to store per‐key counts.
    """

    driver_name = "fixed"

    def __init__(self, application, options: dict):
        """
        Args:
            application: the IoC container / application instance
            options: dict containing:
                - limit: int, max hits per window
                - window_seconds: int, length of window in seconds
                - cache_prefix: str, prefix for all counter keys
        """
        self.application = application
        self.limit = options.get("limit", 60)
        self.window = options.get("window_seconds", 60)
        self.prefix = options.get("cache_prefix", "rate_")

    def attempt(self, key: str) -> Tuple[bool, int, int]:
        """
        Record one attempt.

        Returns (allowed, remaining, reset_in).
        """
        now = int(time.time())
        cache_key = f"{self.prefix}{key}"
        # Retrieve current count or zero
        current = Cache.get(cache_key, {"count": 0, "expires_at": 0})
        count = current.get("count", 0)
        expires_at = current.get("expires_at", now + self.window)

        # If the window expired, start a new window
        if now >= expires_at:
            count = 0
            expires_at = now + self.window

        # Increment
        count += 1

        # Determine if allowed
        allowed = count <= self.limit
        remaining = max(self.limit - count, 0)
        reset_in = max(expires_at - now, 0)

        # Store back with TTL = window remaining
        # (we store both count and expires_at so we know when to reset)
        Cache.put(
            cache_key,
            {"count": count, "expires_at": expires_at},
            ttl=reset_in,
        )

        return allowed, remaining, reset_in

    def reset(self, key: str) -> None:
        """Immediately reset this key's counter."""
        cache_key = f"{self.prefix}{key}"
        # Simply remove it
        Cache.forget(cache_key)
