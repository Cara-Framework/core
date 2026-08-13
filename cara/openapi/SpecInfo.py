"""Canonical definition of ``SpecInfo``."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SpecInfo:
    """The ``info`` block and document version of a generated spec."""

    title: str
    description: str
    version: str = "1.0.0"
    openapi_version: str = "3.0.3"
