"""Canonical definition of ``ConflictingErrorStatus``."""

from __future__ import annotations

from cara.exceptions import CaraException


class ConflictingErrorStatus(CaraException, RuntimeError):
    """One discriminator was emitted with two different HTTP statuses.

    In the taxonomy (§9), ``RuntimeError`` kept as a SECOND base for the
    build command that classifies a RuntimeError as a reportable failure.
    """
