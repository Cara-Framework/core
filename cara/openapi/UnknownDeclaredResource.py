"""Canonical definition of ``UnknownDeclaredResource``."""

from __future__ import annotations

from cara.exceptions import CaraException


class UnknownDeclaredResource(CaraException, RuntimeError):
    """An ``@resource(...)`` docstring names a resource that does not exist.

    In the taxonomy (§9) so ``except CaraException`` around spec generation
    catches it; ``RuntimeError`` stays as a SECOND base for the craft command
    that already treats a RuntimeError as "fail this build, print the message".
    """
