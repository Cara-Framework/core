"""MiddlewareNotFoundException."""

from __future__ import annotations

from .MiddlewareException import MiddlewareException


class MiddlewareNotFoundException(MiddlewareException):
    """Thrown when a middleware alias does not resolve to a class."""

    pass
