"""MailDriverException."""

from __future__ import annotations

from .MailException import MailException


class MailDriverException(MailException):
    """Raised when mail driver encounters an error."""

    pass
