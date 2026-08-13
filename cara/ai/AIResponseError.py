"""AIResponseError."""

from __future__ import annotations

from .AIException import AIException


class AIResponseError(AIException):
    """Raised when an AI response cannot be parsed or is malformed/empty."""
