"""
Event Exception Type for the Cara framework.

This module defines exception types related to event operations.
"""

from .base import CaraException


class ListenerNotFoundException(CaraException):
    """Thrown if you attempt to dispatch an event with no registered listeners."""

    pass


class EventNameConflictException(CaraException):
    """Thrown if two different Event classes share the same name()."""

    pass
