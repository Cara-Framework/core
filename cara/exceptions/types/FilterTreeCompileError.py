"""Filter-tree compilation exception (``cara.filtering``)."""

from __future__ import annotations

__all__ = ["FilterTreeCompileError"]

from .CaraException import CaraException


class FilterTreeCompileError(CaraException, RuntimeError):
    """A validated tree could not be compiled to SQL.

    Always an app wiring bug (an unresolved entity id, a missing ctx
    alias) — the request must fail loudly rather than run wider than
    the user asked.
    """

    def __init__(self, message: str = "Filter tree compilation failed"):
        super().__init__(message)
