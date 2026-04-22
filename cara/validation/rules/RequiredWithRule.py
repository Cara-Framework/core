"""
RequiredWith Validation Rule.

Field becomes required when ANY of the listed fields are present.
Usage: ``required_with:field1,field2,...``.
"""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


def _is_present(v) -> bool:
    if v is None:
        return False
    if isinstance(v, str) and v.strip() == "":
        return False
    return True


class RequiredWithRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        raw = params.get("required_with") or params.get("requiredwith")
        if not raw:
            return True
        others = [p.strip() for p in raw.split(",")]
        data = params.get("_data", {})
        if not any(_is_present(data.get(o)) for o in others):
            return True
        return _is_present(value)

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        raw = params.get("required_with") or params.get("requiredwith", "")
        return f"The {attr.lower()} field is required when {raw} is present."
