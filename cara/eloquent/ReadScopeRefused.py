"""``ReadScopeRefused``: a read a read scope cannot answer truthfully."""

from __future__ import annotations

from cara.exceptions import QueryException


class ReadScopeRefused(QueryException, RuntimeError):
    """A read statement the scope cannot answer truthfully: it reads the
    database clock under a fixed clock, or a relation the scope refuses.

    Rooted in the ORM's query taxonomy (it refuses a query); ``RuntimeError``
    stays a second base for callers that catch the driver-level failure."""


__all__ = ["ReadScopeRefused"]
