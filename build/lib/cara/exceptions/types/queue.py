"""
Queue Exception Type for the Cara framework.

This module defines exception types related to queue operations.
"""

from .base import CaraException


class DriverNotRegisteredException(CaraException):
    """Raised when a requested queue driver has not been registered."""

    pass


class QueueConfigurationException(CaraException):
    """Raised when the 'queue' configuration is missing or invalid."""

    pass


class DriverLibraryNotFoundException(CaraException):
    """Raised when a required third‐party library for a queue driver is missing."""

    pass


class QueueException(CaraException):
    """General exception for queue processing errors."""

    pass
