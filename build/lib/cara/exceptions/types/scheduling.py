"""
Scheduling Exception Type for the Cara framework.

This module defines exception types related to scheduling operations.
"""

from .base import CaraException


class SchedulingConfigurationException(CaraException):
    """Raised when the 'scheduling' configuration is missing or invalid."""

    pass


class DriverLibraryNotFoundException(CaraException):
    """Raised when a required third‐party library for a scheduling driver is missing."""

    pass


class SchedulingException(CaraException):
    """General exception for scheduling processing errors."""

    pass
