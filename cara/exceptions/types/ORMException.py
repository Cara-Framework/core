"""ORMException."""

from __future__ import annotations

from .CaraException import CaraException


class ORMException(CaraException):
    """Base for every ORM/database error, including migrations and connections."""

    pass
