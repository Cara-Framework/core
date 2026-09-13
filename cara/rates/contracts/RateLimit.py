"""The rate-limiter contract: spend a ``Limit``, or refill a bucket."""

from __future__ import annotations

from abc import ABC, abstractmethod

from cara.rates.Limit import Limit
from cara.rates.RateLimitDecision import RateLimitDecision


class RateLimit(ABC):
    """Contract for the rate limiter bound under ``rate``."""

    @abstractmethod
    def attempt(self, limit: Limit, *, cost: int = 1) -> RateLimitDecision:
        """Spend ``cost`` from the bucket ``limit`` names and report the decision."""
        ...

    @abstractmethod
    def reset(self, key: str) -> None:
        """Refill the bucket ``Limit.by(key)`` spends, immediately."""
        ...
