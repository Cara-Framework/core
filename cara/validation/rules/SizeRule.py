"""Size rule (length for strings/arrays, exact value for numbers).

Usage: ``size:5``.
"""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class SizeRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        raw = params.get("size")
        if raw is None or value is None:
            return False
        try:
            target = int(raw)
        except (TypeError, ValueError):
            return False

        chain = params.get("_rules", ())
        # Numeric mode: exact equality.
        if "integer" in chain or "numeric" in chain:
            try:
                return float(value) == target
            except (TypeError, ValueError):
                return False
        # Length-based for strings, lists, dicts.
        if hasattr(value, "__len__"):
            return len(value) == target
        # Default to numeric comparison.
        try:
            return float(value) == target
        except (TypeError, ValueError):
            return False

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be exactly {params.get('size', '')}."
