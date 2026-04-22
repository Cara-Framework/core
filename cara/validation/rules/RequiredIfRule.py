"""
RequiredIf Validation Rule.

Field becomes required when another field equals a given value.
Usage: ``required_if:other_field,value`` (e.g. ``required_if:type,paid``).
"""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class RequiredIfRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        raw = params.get("required_if") or params.get("requiredif")
        if not raw:
            return True
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) < 2:
            return True
        other_field, expected = parts[0], parts[1]
        data = params.get("_data", {})
        if str(data.get(other_field)) != expected:
            return True
        # required-if: field must be present and not empty
        if value is None:
            return False
        if isinstance(value, str) and value.strip() == "":
            return False
        return True

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        raw = params.get("required_if") or params.get("requiredif", "")
        parts = [p.strip() for p in raw.split(",")]
        other = parts[0] if parts else ""
        return f"The {attr.lower()} field is required when {other} equals the given value."
