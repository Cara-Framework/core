"""MiddlewareException."""

from __future__ import annotations

from .CaraException import CaraException


class MiddlewareException(CaraException):
    """Base for all middleware-related exceptions."""

    pass
