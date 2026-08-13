"""IdempotencyOverlapException."""

from __future__ import annotations

from .QueueException import QueueException


class IdempotencyOverlapException(QueueException):
    """A durable job must run after the current owner releases its lease.

    This is a queue throttle, not an execution failure: the callback never
    ran, so the worker must redeliver without consuming the job's failure
    budget.
    """

    is_throttle = True
