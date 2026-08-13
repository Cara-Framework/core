"""BroadcastingDriverNotFoundException."""

from __future__ import annotations

from .BroadcastingException import BroadcastingException


class BroadcastingDriverNotFoundException(BroadcastingException):
    """Exception thrown when a broadcasting driver is not found."""

    pass
