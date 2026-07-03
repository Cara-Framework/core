"""Dict (JSON object) rule. Usage: ``dict``.

Validates that a decoded request value is a mapping — the body-parsed
counterpart of ``array`` (list) and ``json`` (encoded string). Laravel
calls the equivalent shape check ``array`` because PHP arrays are maps;
in Python the two shapes are distinct types, so they are distinct rules.
"""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class DictRule(BaseRule):
    """Validates that a value is a dict (JSON object)."""

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None:
            return False  # parity with ArrayRule — nullable short-circuits first
        return isinstance(value, dict)

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be an object."
