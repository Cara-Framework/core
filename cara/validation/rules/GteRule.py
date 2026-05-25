"""Greater-than-or-equal comparison rule.

Usage: ``gte:10`` or ``gte:other_field``.
"""

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


def _to_number(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class GteRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        threshold = params.get("gte")
        if threshold is None or value is None:
            return False
        data = params.get("_data", {})
        other = data.get(threshold)
        # See ``LteRule.validate`` for the full rationale — same
        # absent-referenced-field guard, mirrored on the upper side
        # of a range. Without this an upper-bound-only payload
        # (``?max_price=100`` with no ``min_price``) failed the
        # canonical ``max_price: gte:min_price`` cross-field guard.
        if other is None and _to_number(threshold) is None:
            return True
        compare_to = _to_number(threshold) if other is None else _to_number(other)
        val = _to_number(value)
        if compare_to is None or val is None:
            return False
        return val >= compare_to

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be greater than or equal to {params.get('gte', '')}."
