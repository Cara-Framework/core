"""Canonical definition of ``Spy``."""

from __future__ import annotations

from typing import Any

from .MockBuilder import _MockBase


class Spy(_MockBase):
    """Permissive mock — any attribute is a no-op recorder."""

    def __init__(self, contract: type[Any] | None = None) -> None:
        super().__init__(contract)
        self._strict = False
