"""QueryException."""

from __future__ import annotations

from .ModelException import ModelException


class QueryException(ModelException):
    """Thrown when a SQL query fails (syntax, constraint, etc.)."""

    pass
