"""Loader / module-loading-related exceptions."""

from .base import CaraException


class LoaderException(CaraException):
    """Base for loader-related failures."""

    pass


class LoaderNotFoundException(LoaderException):
    """Raised when a loader cannot be found or fails to load."""

    pass
