"""AppException."""

from __future__ import annotations

from .CaraException import CaraException


class AppException(CaraException):
    """Base for high-level "app" errors."""

    pass
