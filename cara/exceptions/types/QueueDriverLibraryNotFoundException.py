"""QueueDriverLibraryNotFoundException."""

from __future__ import annotations

from .CaraException import CaraException


class QueueDriverLibraryNotFoundException(CaraException):
    """Raised when a required third‐party library for a queue driver is missing."""

    pass
