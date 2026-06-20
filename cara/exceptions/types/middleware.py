"""Exceptions related to Cara's middleware system."""

from __future__ import annotations

from .Base import CaraException


class MiddlewareException(CaraException):
    """Base for all middleware-related exceptions."""

    pass


class MiddlewareNotFoundException(MiddlewareException):
    """Thrown when a middleware alias does not resolve to a class."""

    pass


__all__ = [
    "MiddlewareException",
    "MiddlewareNotFoundException",
]
