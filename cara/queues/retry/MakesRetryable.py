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
import random
from collections.abc import Awaitable, Callable
from typing import Any

from cara.configuration import config
from cara.facades import Log
from cara.queues.retry.Policy import DEFAULT_RETRY_JITTER_FRACTION

# Database-driver connection drops are transient by nature, and psycopg2 is
# the framework's own Postgres driver (``cara.eloquent.connections``), so
# its transient classes belong in the DEFAULT retryable set. The import is
# guarded — psycopg2 is deliberately absent from cara's dependencies, so a
# non-Postgres install legitimately lacks it — and fails SILENT to the base
# tuple: framework import may precede logging boot, and the absence is not
# an error.
try:  # pragma: no cover - import guard
    from psycopg2 import InterfaceError as _PgInterfaceError
    from psycopg2 import OperationalError as _PgOperationalError

    _PG_TRANSIENT_EXCEPTIONS: tuple[type[Exception], ...] = (
        _PgOperationalError,
        _PgInterfaceError,
    )
except Exception:
    _PG_TRANSIENT_EXCEPTIONS = ()


class MakesRetryable:
    """Exponential backoff retry mixin for queue jobs.

    Resolution order for every knob, in BOTH directions: an explicit
    ``wrap_with_retry`` keyword wins, then a class-level attribute set by
    the subclass, then the runtime config key
    (``jobs.retry_max_attempts``, ``jobs.retry_base_delay``,
    ``jobs.retry_backoff_multiplier``).

    ``None`` is the "unset" sentinel and it is load-bearing. The knobs used
    to carry concrete defaults (3 / 2.0 / 2.0) and the resolver recognised
    an override by comparing each attribute against a hand-written copy of
    that same default — so a subclass that deliberately NARROWED its budget
    to ``MAX_RETRY_ATTEMPTS = 1`` because its body is a non-idempotent
    external call resolved to ``max(1, 3)`` and issued that call three
    times. Silently, and exactly opposite to what its author wrote. The
    sentinel also removes the trap where changing a default here would
    reclassify every subclass that had pinned the old value explicitly.

    Extend ``RETRYABLE_EXCEPTIONS`` in subclasses to narrow or broaden
    what triggers a retry vs immediate failure. psycopg2's transient
    classes (``OperationalError`` / ``InterfaceError``) are included
    automatically when the driver is installed.
    """

    MAX_RETRY_ATTEMPTS: int | None = None
    BASE_RETRY_DELAY: float | None = None
    RETRY_BACKOFF_MULTIPLIER: float | None = None

    #: Per-subclass jitter spread, mirroring ``AMQPDriver``'s
    #: ``retry_jitter_fraction`` hook. 0 disables the spread; the default
    #: is imported from the retry policy SSOT, never restated.
    retry_jitter_fraction: float = DEFAULT_RETRY_JITTER_FRACTION

    RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = (
        ConnectionError,
        TimeoutError,
        asyncio.TimeoutError,
        OSError,
        *_PG_TRANSIENT_EXCEPTIONS,
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
        attempts = int(
            max_attempts
            if max_attempts is not None
            else self.MAX_RETRY_ATTEMPTS
            if self.MAX_RETRY_ATTEMPTS is not None
            else self._retry_max_attempts()
        )
        delay = float(
            base_delay
            if base_delay is not None
            else self.BASE_RETRY_DELAY
            if self.BASE_RETRY_DELAY is not None
            else self._retry_base_delay()
        )
        backoff = float(
            self.RETRY_BACKOFF_MULTIPLIER
            if self.RETRY_BACKOFF_MULTIPLIER is not None
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

                current_delay = self._jittered(delay * (backoff**attempt))

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

    def _jittered(self, delay: float) -> float:
        """Spread one backoff delay by ±``retry_jitter_fraction``.

        ``Policy.DEFAULT_RETRY_JITTER_FRACTION`` owns the number and the
        reason for it: N workers that all failed on the same downstream blip
        would otherwise retry on the same second and recreate the spike that
        caused the failure. The queue-republish path
        (``AMQPDriver._apply_retry_jitter``) has applied that spread for
        years; this in-job path did not, so a fleet retrying through the
        mixin marched into the recovering dependency in lockstep. The
        fraction is IMPORTED from the policy module — a second copy of the
        number is the drift this framework keeps paying for (§5).

        The spread is clamped to 0.9 so a misconfigured fraction can neither
        double the wait nor push it below zero.
        """
        if delay <= 0:
            return 0.0
        try:
            fraction = float(self.retry_jitter_fraction)
        except TypeError, ValueError:
            fraction = DEFAULT_RETRY_JITTER_FRACTION
        if fraction <= 0:
            return delay
        fraction = min(fraction, 0.9)
        swing = delay * fraction
        return max(delay + random.uniform(-swing, swing), 0.0)

    @property
    def retry_attempt(self) -> int:
        """Current retry attempt number (1-based)."""
        return self._retry_attempt


__all__ = ["MakesRetryable"]
