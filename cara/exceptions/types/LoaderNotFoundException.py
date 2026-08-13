"""LoaderNotFoundException."""

from __future__ import annotations

from .LoaderException import LoaderException


class LoaderNotFoundException(LoaderException):
    """Raised when a loader cannot be found or fails to load."""

    pass
