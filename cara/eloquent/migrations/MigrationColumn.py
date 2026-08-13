"""Canonical migration snapshot definition."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Any

_COMPARED_ATTRS = (
    "type",
    "length",
    "precision",
    "scale",
    "nullable",
    "unique",
    "index",
)


@dataclass
class MigrationColumn:
    """A parsed column definition — the same shape for model + migration sides."""

    name: str
    type: str = "string"
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    nullable: bool = False
    unique: bool = False
    index: bool = False
    default: Any = None
    has_default: bool = False
    # The verbatim ``table.<...>`` source line (migration side) so ``down()`` can
    # recreate a removed column losslessly. Empty for model-derived columns
    # (the generator renders those from the structured attrs).
    raw_line: str = ""

    def signature(self) -> tuple:
        """Identity used for ALTER + RENAME detection (the reliably-parsed
        attrs only — see ``_COMPARED_ATTRS``)."""
        return tuple(getattr(self, a) for a in _COMPARED_ATTRS) + (
            self.default_signature(),
        )

    def default_signature(self) -> tuple[bool, Any]:
        """Canonical default value across model objects and parsed source."""
        if not self.has_default:
            return False, None
        value = self.default
        if isinstance(value, str):
            source = value.strip()
            try:
                value = ast.literal_eval(source)
            except SyntaxError, ValueError:
                value = source
        if isinstance(value, (dict, list, set, tuple)):
            value = repr(value)
        return True, value
