"""
Cache Exception Type for the Cara framework.

This module defines exception types related to cache operations.
"""

from .base import CaraException


class CacheConfigurationException(CaraException):
    """Raised when required cache config is missing or invalid."""

    pass


class DriverNotRegisteredException(CaraException):
    """Raised when trying to fetch a driver that was never registered."""

    pass
