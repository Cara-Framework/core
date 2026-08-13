"""MailConfigurationException."""

from __future__ import annotations

from .MailException import MailException


class MailConfigurationException(MailException):
    """Raised when mail configuration is invalid or missing."""

    pass
