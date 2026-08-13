"""DriverLibraryNotFoundException."""

from __future__ import annotations

from .CaraException import CaraException


class DriverLibraryNotFoundException(CaraException):
    """Raised when a required third‐party library for a scheduling driver is missing."""

    pass
