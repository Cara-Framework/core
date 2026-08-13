"""ModelException."""

from __future__ import annotations

from .ORMException import ORMException


class ModelException(ORMException):
    """Base for model/query errors."""

    pass
