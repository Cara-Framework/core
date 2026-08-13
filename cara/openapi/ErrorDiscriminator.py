"""Canonical definition of ``ErrorDiscriminator``."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ErrorDiscriminator:
    """One error ``type`` a client can branch on, and where it comes from."""

    type: str
    status: int | None
    source: str
