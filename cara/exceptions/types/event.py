"""
Event Exception Type for the Cara framework.

This module defines exception types related to event operations.
"""

from __future__ import annotations

from .base import CaraException


class ListenerNotFoundException(CaraException):
    """Thrown if you attempt to dispatch an event with no registered listeners."""

    pass


class EventNameConflictException(CaraException):
    """Thrown if two different Event classes share the same name()."""

    pass


class EventDispatchCycleException(CaraException):
    """Thrown when a listener re-dispatches an event already in flight.

    Without this guard, a listener that fires the same event it is
    handling — directly or transitively — recurses until the Python
    stack overflows. The dispatcher tracks the chain of in-flight
    event names per asyncio task and raises before recursing back into
    one that is already on the stack.
    """

    pass


__all__ = [
    "ListenerNotFoundException",
    "EventNameConflictException",
    "EventDispatchCycleException",
]
