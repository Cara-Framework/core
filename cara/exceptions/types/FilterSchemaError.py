"""Filter-schema declaration exception (``cara.filtering``)."""

from __future__ import annotations

__all__ = ["FilterSchemaError"]

from .CaraException import CaraException


class FilterSchemaError(CaraException, ValueError):
    """A tree schema or field DECLARATION is internally inconsistent.

    Always an app-authoring bug (a select field without options, an
    entity field without a prefix…), so it answers 500, never 4xx.
    """

    def __init__(self, message: str = "Invalid filter schema declaration"):
        super().__init__(message)
