"""LoaderException."""

from __future__ import annotations

from .CaraException import CaraException


class LoaderException(CaraException):
    """Base for loader-related failures."""

    pass
