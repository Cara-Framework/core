"""Cara Retry — declarative retry with exponential backoff.

Replaces ad-hoc retry loops scattered across scraping drivers, API clients,
and job handlers. Provides a fluent interface matching the framework style.

Usage::

    from cara.support import Retry

    # Simple retry
    result = await Retry.times(3).run(async_callable)

    # With backoff
    result = await Retry.times(3).backoff(base=2.0, jitter=0.15).run(coro_factory)

    # Retry only on specific exceptions
    result = await Retry.times(3).catch(httpx.TimeoutException, httpx.ConnectError).run(fn)

    # With max delay cap
    result = await Retry.times(5).backoff(base=2.0, max_delay=30.0).run(fn)

    # Synchronous version
    result = Retry.times(3).run_sync(callable)
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any


class RetryBuilder:
    """Fluent builder for configuring retry behavior."""

    def __init__(self, max_attempts: int = 3) -> None:
        self._max_attempts = max_attempts
        self._backoff_base: float = 1.0
        self._backoff_jitter: float = 0.15
        self._max_delay: float = 60.0
        self._catch_types: tuple[type[BaseException], ...] = (Exception,)
        self._on_retry: Callable[[int, BaseException], None] | None = None

    def times(self, attempts: int) -> RetryBuilder:
        """Set maximum retry attempts (total calls = attempts + 1)."""
        self._max_attempts = attempts
        return self

    def backoff(
        self,
        base: float = 2.0,
        *,
        jitter: float = 0.15,
        max_delay: float = 60.0,
    ) -> RetryBuilder:
        """Configure exponential backoff: delay = base^attempt * (1 ± jitter)."""
        self._backoff_base = base
        self._backoff_jitter = jitter
        self._max_delay = max_delay
        return self

    def catch(self, *exception_types: type[BaseException]) -> RetryBuilder:
        """Only retry on these exception types (default: all Exception)."""
        self._catch_types = exception_types
        return self

    def on_retry(self, callback: Callable[[int, BaseException], None]) -> RetryBuilder:
        """Register a callback invoked before each retry (for logging)."""
        self._on_retry = callback
        return self

    def _compute_delay(self, attempt: int) -> float:
        delay = self._backoff_base ** attempt
        delay *= 1.0 + random.uniform(-self._backoff_jitter, self._backoff_jitter)
        return min(delay, self._max_delay)

    async def run(self, callback: Callable[[], Awaitable[Any]]) -> Any:
        """Execute the async callable with retry logic."""
        last_exc: BaseException | None = None

        for attempt in range(self._max_attempts + 1):
            try:
                return await callback()
            except self._catch_types as exc:
                last_exc = exc
                if attempt >= self._max_attempts:
                    raise
                if self._on_retry:
                    self._on_retry(attempt + 1, exc)
                await asyncio.sleep(self._compute_delay(attempt + 1))

        raise last_exc  # type: ignore[misc]

    def run_sync(self, callback: Callable[[], Any]) -> Any:
        """Execute the synchronous callable with retry logic."""
        last_exc: BaseException | None = None

        for attempt in range(self._max_attempts + 1):
            try:
                return callback()
            except self._catch_types as exc:
                last_exc = exc
                if attempt >= self._max_attempts:
                    raise
                if self._on_retry:
                    self._on_retry(attempt + 1, exc)
                time.sleep(self._compute_delay(attempt + 1))

        raise last_exc  # type: ignore[misc]


class Retry:
    """Static facade for building retry configurations."""

    @staticmethod
    def times(attempts: int) -> RetryBuilder:
        """Create a retry builder with the given max attempts."""
        return RetryBuilder(max_attempts=attempts)

    @staticmethod
    def backoff(base: float = 2.0, *, jitter: float = 0.15, max_delay: float = 60.0) -> RetryBuilder:
        """Create a retry builder with backoff configuration (default 3 attempts)."""
        return RetryBuilder().backoff(base, jitter=jitter, max_delay=max_delay)
