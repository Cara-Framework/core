"""
Queue Exception Type for the Cara framework.

This module defines exception types related to queue operations.

``QueueDriverLibraryNotFoundException`` carries the queue-specific name in
its own definition rather than being a barrel alias over a second
``DriverLibraryNotFoundException``. Three modules used to declare that
short name; the barrels bound the scheduling one and aliased the rest, so
which class a call site got depended on the import path it happened to
use. One short name, one home, no aliases.
"""

from __future__ import annotations

from .Base import CaraException


class QueueConfigurationException(CaraException):
    """Raised when the 'queue' configuration is missing or invalid."""

    pass


class QueueDriverLibraryNotFoundException(CaraException):
    """Raised when a required third‐party library for a queue driver is missing."""

    pass


class QueueException(CaraException):
    """General exception for queue processing errors."""

    pass


class IdempotencyOverlapException(QueueException):
    """A durable job must run after the current owner releases its lease.

    This is a queue throttle, not an execution failure: the callback never
    ran, so the worker must redeliver without consuming the job's failure
    budget.
    """

    is_throttle = True


__all__ = [
    "IdempotencyOverlapException",
    "QueueConfigurationException",
    "QueueDriverLibraryNotFoundException",
    "QueueException",
]
