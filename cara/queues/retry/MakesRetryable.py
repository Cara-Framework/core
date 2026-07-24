"""Retryable job mixin — exponential backoff retry logic.

Laravel-style trait that provides configurable retry behavior for queue
jobs. Jobs wrap their main work in ``wrap_with_retry`` to automatically
retry on transient exceptions with exponential backoff::

    class MyJob(MakesRetryable, BaseJob):
        async def handle(self):
            await self.wrap_with_retry(self._do_work)

        async def _do_work(self): ...
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from cara.configuration import config
from cara.facades import Log


class MakesRetryable:
    """Exponential backoff retry mixin for queue jobs.

    Class-level attributes can be overridden per-subclass. Runtime
    config keys (``jobs.retry_max_attempts``, ``jobs.retry_base_delay``,
    ``jobs.retry_backoff_multiplier``) take precedence when present.

    Extend ``RETRYABLE_EXCEPTIONS`` in subclasses to narrow or broaden
    what triggers a retry vs immediate failure.
    """

    MAX_RETRY_ATTEMPTS: int = 3
    BASE_RETRY_DELAY: float = 2.0
    RETRY_BACKOFF_MULTIPLIER: float = 2.0
    RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = (
        ConnectionError,
        TimeoutError,
        asyncio.TimeoutError,
        OSError,
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._retry_attempt = 0

    @classmethod
    def _retry_max_attempts(cls) -> int:
        return int(config("jobs.retry_max_attempts", 3))

    @classmethod
    def _retry_base_delay(cls) -> float:
        return float(config("jobs.retry_base_delay", 2.0))

    @classmethod
    def _retry_backoff_multiplier(cls) -> float:
        return float(config("jobs.retry_backoff_multiplier", 2.0))

    async def wrap_with_retry(
        self,
        callback: Callable[[], Awaitable[Any]],
        *,
        max_attempts: int | None = None,
        base_delay: float | None = None,
    ) -> Any:
        """Run ``callback`` with exponential backoff retry logic.

        Args:
            callback: Async callable that performs the job body.
            max_attempts: Override for max retry attempts.
            base_delay: Override for base delay in seconds.

        Returns:
            Callback return value on success.

        Raises:
            Exception: The last exception after all retries exhausted,
                or any non-retryable exception immediately.
        """
        attempts = (
            max_attempts
            if max_attempts is not None
            else max(self.MAX_RETRY_ATTEMPTS, self._retry_max_attempts())
            if self.MAX_RETRY_ATTEMPTS != 3
            else self._retry_max_attempts()
        )
        delay = (
            base_delay
            if base_delay is not None
            else (
                self.BASE_RETRY_DELAY
                if self.BASE_RETRY_DELAY != 2.0
                else self._retry_base_delay()
            )
        )
        backoff = (
            self.RETRY_BACKOFF_MULTIPLIER
            if self.RETRY_BACKOFF_MULTIPLIER != 2.0
            else self._retry_backoff_multiplier()
        )

        for attempt in range(attempts):
            self._retry_attempt = attempt + 1

            try:
                result = await callback()

                if attempt > 0:
                    Log.info(
                        "[Retry] %s succeeded on attempt %s",
                        self.__class__.__name__,
                        attempt + 1,
                        category="retry",
                    )

                return result

            except Exception as e:
                if not isinstance(e, self.RETRYABLE_EXCEPTIONS):
                    Log.warning(
                        "[Retry] %s encountered non-retryable exception: %s",
                        self.__class__.__name__,
                        e,
                        category="retry",
                    )
                    raise

                if attempt == attempts - 1:
                    Log.error(
                        "[Retry] %s failed after %s attempts: %s",
                        self.__class__.__name__,
                        attempts,
                        e,
                        category="retry",
                    )
                    raise

                current_delay = delay * (backoff**attempt)

                Log.warning(
                    "[Retry] %s attempt %s/%s failed: %s, retrying in %ss",
                    self.__class__.__name__,
                    attempt + 1,
                    attempts,
                    e,
                    current_delay,
                    category="retry",
                )

                await asyncio.sleep(current_delay)

        raise RuntimeError("Unexpected exit from retry loop")

    @property
    def retry_attempt(self) -> int:
        """Current retry attempt number (1-based)."""
        return self._retry_attempt


__all__ = ["MakesRetryable"]
