"""Canonical definition of ``UnknownDeclaredError``."""

from __future__ import annotations

from cara.exceptions import CaraException


class UnknownDeclaredError(CaraException, RuntimeError):
    """An ``@raises(...)`` docstring names an exception with no known status.

    A typo, or an exception that states no ``status_code``, would publish a
    contract missing the very answer the declaration was written for, so the
    build fails instead (the ``UnknownDeclaredResource`` rule).
    """
