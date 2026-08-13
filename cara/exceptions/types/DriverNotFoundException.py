"""DriverNotFoundException."""

from __future__ import annotations

from .ModelException import ModelException


class DriverNotFoundException(ModelException):
    """Thrown when a database driver cannot be found."""

    pass
