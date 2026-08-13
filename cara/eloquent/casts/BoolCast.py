"""Canonical definition of ``BoolCast``."""

from __future__ import annotations

from .BaseCast import BaseCast


class BoolCast(BaseCast):
    """Cast to boolean.

    Bare ``bool(value)`` is dangerously wrong for string inputs — every
    non-empty string is truthy, so ``bool("0")``, ``bool("false")``, and
    ``bool("False")`` all evaluate to ``True``. Importers that write
    string-shaped truthiness (``"0"`` / ``"1"``, ``"true"`` / ``"false"``)
    into boolean columns silently flipped to ``True`` regardless of the
    intended value.

    ``None`` stays ``None`` (NULL semantics — same as ``IntCast``).
    """

    _TRUE_TOKENS = frozenset({"true", "1", "yes", "y", "t", "on"})
    _FALSE_TOKENS = frozenset({"false", "0", "no", "n", "f", "off", ""})

    def _coerce(self, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value == 1 if value in {0, 1} else None
        if isinstance(value, str):
            token = value.strip().lower()
            if token in self._TRUE_TOKENS:
                return True
            if token in self._FALSE_TOKENS:
                return False
            return None
        return None

    def get(self, value):
        """Get as boolean."""
        return self._coerce(value)

    def set(self, value):
        """Set as boolean."""
        return self._coerce(value)
