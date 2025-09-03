"""
Rate Limit Interface for the Cara framework.

This module defines the contract for rate limiter drivers, specifying the required methods for rate
limiting implementations.
"""

from abc import ABC, abstractmethod
from typing import Tuple


class RateLimit(ABC):
    """Contract for a rate‐limiter driver."""

    @abstractmethod
    def attempt(self, key: str) -> Tuple[bool, int, int]:
        """
        Record one "hit" against the given key (e.g. client IP or route).
        Returns a tuple: (allowed: bool, remaining: int, reset_in: int)

        - allowed: True if under the limit, False if over quota.
        - remaining: how many requests remain in this window (if allowed=True).
        - reset_in: number of seconds until this window resets.
        """
        ...

    @abstractmethod
    def reset(self, key: str) -> None:
        """Reset the count for the given key immediately."""
        ...
