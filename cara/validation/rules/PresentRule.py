"""Present rule (key must exist; value can be empty). Usage: ``present``."""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class PresentRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        data = params.get("_data", {})
        # Value may be None or '' but the KEY must be present in payload.
        return field in data

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} field must be present."
