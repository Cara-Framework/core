"""Canonical definition of ``JobThrottledException``."""

from __future__ import annotations

from cara.exceptions import QueueException


class JobThrottledException(QueueException):
    """Raised when middleware skips job execution due to throttling.

    ``is_throttle`` opts this exception into the worker's STARVATION budget
    (``throttle_attempts`` / ``max_throttle_attempts``) instead of the FAILURE
    budget (``attempts`` / ``max_attempts``) — the lane
    ``JobProcessor._requeue_with_delay`` documents as the one "future throttle
    classes opt in for free".

    Cara's own throttle exception never opted in. A throttled job therefore
    spent the failure budget: three attempts at 1s/5s/30s, all of them inside
    a gate whose default ``retry_after`` is 300s, so a perfectly healthy job
    dead-lettered roughly six seconds after it was first throttled.
    ``ConcurrencyExceeded`` (``cara/queues/middleware/ConcurrencyLimited.py``)
    has always declared this; the two throttle signals now settle the same way.
    """

    is_throttle: bool = True

    def __init__(
        self,
        message: str = "Job was throttled",
        key: str | None = None,
        retry_after: int | None = None,
    ):
        super().__init__(message)
        self.key = key
        self.retry_after = retry_after
