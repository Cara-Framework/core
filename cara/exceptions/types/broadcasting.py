"""
Broadcasting Exceptions.

All broadcasting-related exceptions for the Cara framework.
"""

from .base import CaraException


class BroadcastingException(CaraException):
    """Base exception for all broadcasting-related errors."""

    pass


class BroadcastingConfigurationException(BroadcastingException):
    """Exception thrown when broadcasting configuration is invalid."""

    pass


class BroadcastingDriverNotFoundException(BroadcastingException):
    """Exception thrown when a broadcasting driver is not found."""

    pass


class BroadcastingConnectionException(BroadcastingException):
    """Exception thrown when a broadcasting connection fails."""

    pass


class BroadcastingChannelException(BroadcastingException):
    """Exception thrown when channel operations fail."""

    pass
