"""Canonical definition of ``Mock``."""

from __future__ import annotations

from typing import Any

from .MockBuilder import _MockBase


class Mock(_MockBase):
    """Strict mock — undeclared methods raise."""

    def __init__(self, contract: type[Any] | None = None) -> None:
        super().__init__(contract)
        self._strict = True
