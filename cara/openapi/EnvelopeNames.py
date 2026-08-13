"""Canonical definition of ``EnvelopeNames``."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EnvelopeNames:
    """Component-schema names the envelope refers to.

    Applications supply the component bodies themselves; only the names are
    needed here, to build the ``$ref`` on each operation.
    """

    meta: str = "_Meta"
    cursor_meta: str = "_CursorMeta"
    error: str = "ApiErrorBody"
