"""Greater-than-or-equal comparison rule.

Usage: ``gte:10`` or ``gte:other_field``.
"""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


def _to_number(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class GteRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        threshold = params.get("gte")
        if threshold is None or value is None:
            return False
        data = params.get("_data", {})
        other = data.get(threshold)
        compare_to = _to_number(threshold) if other is None else _to_number(other)
        val = _to_number(value)
        if compare_to is None or val is None:
            return False
        return val >= compare_to

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be greater than or equal to {params.get('gte', '')}."
