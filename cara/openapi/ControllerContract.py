"""Canonical definition of ``ControllerContract``."""

from __future__ import annotations

from dataclasses import dataclass

from .ControllerResponse import ControllerResponse


@dataclass(frozen=True, slots=True)
class ControllerContract:
    """Request validators and response variants used by one routed action."""

    requests: tuple[str, ...]
    responses: tuple[ControllerResponse, ...]
