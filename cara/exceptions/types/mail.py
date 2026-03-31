"""
Mail-related exceptions for the Cara framework.
"""

from .base import CaraException


class MailException(CaraException):
    """Base exception for mail-related errors."""

    pass


class MailConfigurationException(MailException):
    """Raised when mail configuration is invalid or missing."""

    pass


class MailDriverException(MailException):
    """Raised when mail driver encounters an error."""

    pass


class MailSendException(MailException):
    """Raised when mail sending fails."""

    pass
