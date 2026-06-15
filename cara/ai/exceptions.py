"""Exceptions raised by the Cara AI subsystem."""

from __future__ import annotations

from cara.exceptions import CaraException


class AIException(CaraException):
    """Base class for AI client failures."""


class AIResponseError(AIException):
    """Raised when an AI response cannot be parsed or is malformed/empty."""
