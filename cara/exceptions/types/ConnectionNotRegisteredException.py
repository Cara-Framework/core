"""ConnectionNotRegisteredException."""

from __future__ import annotations

from .ORMException import ORMException


class ConnectionNotRegisteredException(ORMException):
    """Exception raised when a database connection is not registered."""

    pass
