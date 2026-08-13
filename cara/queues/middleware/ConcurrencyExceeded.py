"""Canonical definition of ``ConcurrencyExceeded``."""

from __future__ import annotations

from cara.exceptions import CaraException


class ConcurrencyExceeded(CaraException):
    """Raised when concurrency limit is exceeded.

    The queue runner should requeue the job with a delay. This exception
    does NOT count against max_attempts since it's a transient throttle,
    not a job failure.

    The ``is_throttle`` class-attribute is the load-bearing signal —
    ``JobProcessor._requeue_with_delay`` reads it via ``getattr`` to
    suppress the normal ``attempts += 1`` write when republishing.
    """

    is_throttle: bool = True
