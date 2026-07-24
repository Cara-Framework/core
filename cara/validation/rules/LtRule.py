"""Less-than comparison rule. Usage: ``lt:10`` or ``lt:field``."""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


def _to_number(v):
    try:
        return float(v)
    except TypeError, ValueError:
        return None


class LtRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        threshold = params.get("lt")
        if threshold is None or value is None:
            return False
        data = params.get("_data", {})
        other = data.get(threshold)
        # Mirror the absent-referenced-field guard ``LteRule`` /
        # ``GteRule`` already carry. When ``threshold`` is a field-
        # name reference and the referenced field is absent from the
        # payload, treat the cross-field check as a no-op (passes).
        # Pre-fix the rule returned ``False`` here, which broke every
        # single-bound query using the strict-comparison form
        # (``min_price: lt:max_price`` with no ``max_price``).
        # Differentiated from a malformed rule (threshold isn't a
        # number AND isn't a present field name) by checking
        # ``_to_number(threshold)``: a literal numeric threshold
        # still falls through to the numeric comparison below.
        if other is None and _to_number(threshold) is None:
            return True
        compare_to = _to_number(threshold) if other is None else _to_number(other)
        val = _to_number(value)
        if compare_to is None or val is None:
            return False
        return val < compare_to

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be less than {params.get('lt', '')}."
