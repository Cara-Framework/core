"""Cara Retry — declarative retry with exponential backoff.

Replaces ad-hoc retry loops scattered across network drivers, API clients,
and job handlers. Provides a fluent interface matching the framework style.

Usage::

    from cara.support import Retry

    # Simple retry
    result = await Retry.times(3).run(async_callable)

    # With backoff
    result = await Retry.times(3).backoff(base=2.0, jitter=0.15).run(coro_factory)

    # Retry only on specific exceptions
    result = (
        await Retry.times(3).catch(httpx.TimeoutException, httpx.ConnectError).run(fn)
    )

    # With max delay cap
    result = await Retry.times(5).backoff(base=2.0, max_delay=30.0).run(fn)

    # Synchronous version
    result = Retry.times(3).run_sync(callable)
"""

from __future__ import annotations

from .RetryBuilder import RetryBuilder


class Retry:
    """Static facade for building retry configurations."""

    @staticmethod
    def times(attempts: int) -> RetryBuilder:
        """Create a retry builder with the given max attempts."""
        return RetryBuilder(max_attempts=attempts)

    @staticmethod
    def backoff(
        base: float = 2.0, *, jitter: float = 0.15, max_delay: float = 60.0
    ) -> RetryBuilder:
        """Create a retry builder with backoff configuration (default 3 attempts)."""
        return RetryBuilder().backoff(base, jitter=jitter, max_delay=max_delay)
