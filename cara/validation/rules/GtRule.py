"""Greater-than comparison rule.

Usage: ``gt:10`` or ``gt:other_field``.
"""

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


def _to_number(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class GtRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        threshold = params.get("gt")
        if threshold is None or value is None:
            return False
        # Threshold can be a literal number or a field name.
        data = params.get("_data", {})
        other = data.get(threshold)
        # See ``LteRule.validate`` / ``LtRule.validate`` for the full
        # rationale — mirrored absent-referenced-field guard. Without
        # this an upper-bound-only payload (``?max_price=100`` with
        # no ``min_price``) failed any ``max_price: gt:min_price``
        # cross-field guard. The literal-numeric threshold form
        # (``gt:10``) still falls through to the numeric comparison
        # below because ``_to_number("10")`` returns 10.0.
        if other is None and _to_number(threshold) is None:
            return True
        compare_to = _to_number(threshold) if other is None else _to_number(other)
        val = _to_number(value)
        if compare_to is None or val is None:
            return False
        return val > compare_to

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be greater than {params.get('gt', '')}."
