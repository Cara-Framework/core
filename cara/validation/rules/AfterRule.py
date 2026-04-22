"""After-date rule. Usage: ``after:2020-01-01`` or ``after:other_field``."""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule
from cara.validation.rules.DateRule import _parse_date


class AfterRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        target = params.get("after")
        if not target or value is None:
            return False
        data = params.get("_data", {})
        compare = _parse_date(data.get(target)) or _parse_date(target)
        v = _parse_date(value)
        if compare is None or v is None:
            return False
        return v > compare

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a date after {params.get('after', '')}."
