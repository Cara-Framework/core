"""After-or-equal date rule. Usage: ``after_or_equal:2020-01-01`` or ``after_or_equal:other_field``."""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule
from cara.validation.rules.DateRule import _parse_date


class AfterOrEqualRule(BaseRule):
    """Validates that a date is after or equal to a given date/field."""

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        target = params.get("after_or_equal")
        if not target or value is None:
            return False
        data = params.get("_data", {})
        compare = _parse_date(data.get(target)) or _parse_date(target)
        v = _parse_date(value)
        if compare is None or v is None:
            return False
        try:
            return v >= compare
        except TypeError:
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a date after or equal to {params.get('after_or_equal', '')}."
