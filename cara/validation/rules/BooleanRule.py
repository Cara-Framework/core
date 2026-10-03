"""
Boolean Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is a boolean.
"""

from __future__ import annotations

from typing import Any

from cara.support import safe_bool
from cara.validation.rules.BaseRule import BaseRule


class BooleanRule(BaseRule):
    """Validates that a value is a boolean or boolean-like string.

    The vocabulary is ``cara.support.Coercion.safe_bool``'s — the same one
    ``Request.boolean()`` converts with. The rule validates but does not
    coerce (``validated()`` keeps the caller's spelling), so a handler must
    read the flag through ``request.boolean(key)`` / ``safe_bool``: plain
    truthiness reads a validated ``"false"`` as ``True``.
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        return safe_bool(value) is not None

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        return f"'{field}' must be a boolean value."
