"""Size rule (length for strings/arrays, exact value for numbers).

Usage: ``size:5``.
"""

from __future__ import annotations

from typing import Any

from cara.validation.MessageFormatter import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class SizeRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        raw = params.get("size")
        if raw is None or value is None:
            return False
        try:
            target = int(raw)
        except TypeError, ValueError:
            return False

        chain = params.get("_rules", ())
        # Numeric mode: exact equality.
        if "integer" in chain or "numeric" in chain:
            return self.as_number(value) == target
        # Length-based for strings, lists, dicts.
        if hasattr(value, "__len__"):
            return len(value) == target
        # Default to numeric comparison — exact, no float() overflow.
        return self.as_number(value) == target

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be exactly {params.get('size', '')}."
