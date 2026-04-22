"""Different rule. Usage: ``different:other_field``."""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class DifferentRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        other = params.get("different")
        if not other:
            return True
        data = params.get("_data", {})
        return value != data.get(other)

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        other = params.get("different", "")
        return f"The {attr.lower()} and {other} must be different."
