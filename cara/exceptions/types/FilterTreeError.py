"""Filter-tree payload exception (``cara.filtering``)."""

from __future__ import annotations

__all__ = ["FilterTreeError"]

from .CaraException import CaraException


class FilterTreeError(CaraException, ValueError):
    """A ``filters`` payload failed structural or vocabulary validation.

    Client-caused: the tree referenced an unknown field, an unsupported
    operator, or malformed values. Message text is path-precise
    (``filters[2].v[0]: …``) and safe to surface. Keeps ``ValueError``
    as a second base so ``except ValueError`` call sites keep working.
    """

    is_http_exception = True
    status_code = 422

    def __init__(self, message: str = "Invalid filter expression"):
        super().__init__(message)
