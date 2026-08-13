"""Canonical definition of ``ControllerResponse``."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, order=True)
class ControllerResponse:
    """One statically observed response variant for a controller action."""

    status: int
    kind: str
