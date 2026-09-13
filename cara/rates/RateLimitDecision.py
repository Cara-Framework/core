"""RateLimitDecision — what one attempt against a Limit decided."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RateLimitDecision:
    """The answer to one spend from one bucket.

    ``remaining`` is how many more requests the key could send right now.
    ``retry_after`` is whole seconds until a refused request would fit (0 when
    this one was allowed); ``reset_after`` is whole seconds until the bucket is
    full again. Both round UP: a client told "0" and refused again was lied to.
    """

    allowed: bool
    limit: int
    period_seconds: int
    remaining: int
    retry_after: int
    reset_after: int
