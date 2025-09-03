"""
Storage Exception Type for the Cara framework.

This module defines exception types related to storage operations.
"""

from .base import CaraException


class StorageException(CaraException):
    pass


class StorageConfigurationException(CaraException):
    pass


class KeyNotFoundException(CaraException):
    pass


class DriverNotRegisteredException(CaraException):
    pass
