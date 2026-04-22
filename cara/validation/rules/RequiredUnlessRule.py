"""
RequiredUnless Validation Rule.

Field is required unless another field equals a given value.
Usage: ``required_unless:other_field,value``.
"""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class RequiredUnlessRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        raw = params.get("required_unless") or params.get("requiredunless")
        if not raw:
            return True
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) < 2:
            return True
        other_field, expected = parts[0], parts[1]
        data = params.get("_data", {})
        if str(data.get(other_field)) == expected:
            return True
        if value is None:
            return False
        if isinstance(value, str) and value.strip() == "":
            return False
        return True

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} field is required unless the given condition is met."
