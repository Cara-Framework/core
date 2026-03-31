"""
Fixed-Window Rate Limiter for the Cara framework.

This module implements a fixed-window rate limiting algorithm using the cache system, enforcing
request limits per key within a time window. It supports named limiters (Laravel-style)
for flexible per-user, per-endpoint rate limiting.
"""

import time
from typing import Callable, Optional, Tuple

from cara.facades import Cache
from cara.rates.contracts import RateLimit


class Limit:
    """
    Represents a rate limit configuration.
    
    Provides builder pattern methods to configure rate limits with custom keys and responses.
    Inspired by Laravel's Limit class for flexible rate limiting definitions.
    """

    def __init__(self, max_attempts: int = 60, decay_minutes: int = 1):
        """
        Initialize a rate limit.

        Args:
            max_attempts: Maximum number of requests allowed in the decay window
            decay_minutes: Time window in minutes
        """
        self.max_attempts = max_attempts
        self.decay_minutes = decay_minutes
        self._key = None
        self._response = None

    @classmethod
    def per_minute(cls, max_attempts: int) -> "Limit":
        """Create a rate limit for a 1-minute window."""
        return cls(max_attempts=max_attempts, decay_minutes=1)

    @classmethod
    def per_hour(cls, max_attempts: int) -> "Limit":
        """Create a rate limit for a 1-hour window."""
        return cls(max_attempts=max_attempts, decay_minutes=60)

    @classmethod
    def per_day(cls, max_attempts: int) -> "Limit":
        """Create a rate limit for a 24-hour window."""
        return cls(max_attempts=max_attempts, decay_minutes=1440)

    @classmethod
    def none(cls) -> "Limit":
        """Create an unlimited rate limit (no rate limiting)."""
        return cls(max_attempts=0, decay_minutes=0)

    def by(self, key: str) -> "Limit":
        """
        Set the rate limit key (e.g., user ID, IP address, endpoint).
        
        Args:
            key: Unique identifier for this rate limit
            
        Returns:
            self for method chaining
        """
        self._key = key
        return self

    def response(self, callback: Callable) -> "Limit":
        """
        Set a custom response handler for when rate limit is exceeded.
        
        Args:
            callback: Function to call when rate limited
            
        Returns:
            self for method chaining
        """
        self._response = callback
        return self


class RateLimiter(RateLimit):
    """
    Fixed‐window rate limiter with named limiter support.

    Uses the 'cache' to store per‐key counts and supports named rate limiters
    for flexible per-user, per-endpoint configuration.
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
        self._limiters = {}  # Named limiter definitions (name -> callback)

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

    def for_(self, name: str, callback: Callable) -> "RateLimiter":
        """
        Register a named rate limiter with a callback.
        
        The callback receives a request object and should return a Limit object
        or list of Limit objects defining the rate limit configuration.

        Args:
            name: Unique identifier for this named limiter
            callback: Function that takes a request and returns Limit or list[Limit]

        Returns:
            self for method chaining
        """
        self._limiters[name] = callback
        return self

    def limiter(self, name: str) -> Optional[Callable]:
        """
        Get a registered named limiter callback.

        Args:
            name: Name of the limiter to retrieve

        Returns:
            The callback function or None if not found
        """
        return self._limiters.get(name)

    def resolve_limiter(self, name: str, request):
        """
        Resolve a named limiter for a given request.

        Args:
            name: Name of the registered limiter
            request: The HTTP request object

        Returns:
            Limit object or list of Limit objects, or None if limiter not found
        """
        callback = self._limiters.get(name)
        if callback:
            return callback(request)
        return None

    def reset(self, key: str) -> None:
        """Immediately reset this key's counter."""
        cache_key = f"{self.prefix}{key}"
        # Simply remove it
        Cache.forget(cache_key)
