"""After-date rule. Usage: ``after:2020-01-01`` or ``after:other_field``."""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule
from cara.validation.rules.DateRule import _parse_date


class AfterRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        target = params.get("after")
        if not target or value is None:
            return False
        data = params.get("_data", {})
        compare = _parse_date(data.get(target)) or _parse_date(target)
        v = _parse_date(value)
        if compare is None or v is None:
            return False
        # Naive-vs-aware comparison raises ``TypeError`` in Python;
        # treat that as a validation miss so the rule emits the
        # standard 422 message instead of letting the exception
        # propagate to a 500. See ``BeforeRule`` for the same
        # rationale.
        try:
            return v > compare
        except TypeError:
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a date after {params.get('after', '')}."
