"""Loader / module-loading-related exceptions."""

from __future__ import annotations

from .Base import CaraException


class LoaderException(CaraException):
    """Base for loader-related failures."""

    pass


class LoaderNotFoundException(LoaderException):
    """Raised when a loader cannot be found or fails to load."""

    pass


__all__ = [
    "LoaderException",
    "LoaderNotFoundException",
]
