"""EventNameConflictException."""

from __future__ import annotations

from .CaraException import CaraException


class EventNameConflictException(CaraException):
    """Thrown if two different Event classes share the same name()."""

    pass
