"""AIException."""

from __future__ import annotations

from cara.exceptions import CaraException


class AIException(CaraException):
    """Base class for AI client failures."""
