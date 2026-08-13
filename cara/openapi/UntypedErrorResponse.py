"""Canonical definition of ``UntypedErrorResponse``."""

from __future__ import annotations

from cara.exceptions import CaraException


class UntypedErrorResponse(CaraException, RuntimeError):
    """An HTTP error body was emitted without a machine-readable ``type``.

    Same dual inherit, same reason, as ``ConflictingErrorStatus`` above.
    """
