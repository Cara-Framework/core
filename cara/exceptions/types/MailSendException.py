"""MailSendException."""

from __future__ import annotations

from .MailException import MailException


class MailSendException(MailException):
    """Raised when mail sending fails."""

    pass
