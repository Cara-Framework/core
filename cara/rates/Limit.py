"""Limit."""

from __future__ import annotations

from collections.abc import Callable


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
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
            raise TypeError("max_attempts must be a non-negative integer")
        if isinstance(decay_minutes, bool) or not isinstance(decay_minutes, int):
            raise TypeError("decay_minutes must be a non-negative integer")
        if max_attempts < 0 or decay_minutes < 0:
            raise ValueError("rate-limit values cannot be negative")
        if (max_attempts == 0) != (decay_minutes == 0):
            raise ValueError(
                "an unlimited limit requires both max_attempts and decay_minutes to be 0"
            )
        self.max_attempts = max_attempts
        self.decay_minutes = decay_minutes
        self._key = None
        self._response = None

    @classmethod
    def per_minute(cls, max_attempts: int) -> Limit:
        """Create a rate limit for a 1-minute window."""
        return cls(max_attempts=max_attempts, decay_minutes=1)

    @classmethod
    def per_hour(cls, max_attempts: int) -> Limit:
        """Create a rate limit for a 1-hour window."""
        return cls(max_attempts=max_attempts, decay_minutes=60)

    @classmethod
    def per_day(cls, max_attempts: int) -> Limit:
        """Create a rate limit for a 24-hour window."""
        return cls(max_attempts=max_attempts, decay_minutes=1440)

    @classmethod
    def none(cls) -> Limit:
        """Create an unlimited rate limit (no rate limiting)."""
        return cls(max_attempts=0, decay_minutes=0)

    def by(self, key: str) -> Limit:
        """
        Set the rate limit key (e.g., user ID, IP address, endpoint).

        Args:
            key: Unique identifier for this rate limit

        Returns:
            self for method chaining
        """
        if not isinstance(key, str) or not key.strip():
            raise ValueError("rate-limit key must be a non-empty string")
        self._key = key.strip()
        return self

    def response(self, callback: Callable) -> Limit:
        """
        Set a custom response handler for when rate limit is exceeded.

        Args:
            callback: Function to call when rate limited

        Returns:
            self for method chaining
        """
        if not callable(callback):
            raise TypeError("rate-limit response must be callable")
        self._response = callback
        return self
