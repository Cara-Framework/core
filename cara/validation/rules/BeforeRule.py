"""Before-date rule. Usage: ``before:2030-01-01`` or ``before:other_field``."""

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule
from cara.validation.rules.DateRule import _parse_date


class BeforeRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        target = params.get("before")
        if not target or value is None:
            return False
        data = params.get("_data", {})
        compare = _parse_date(data.get(target)) or _parse_date(target)
        v = _parse_date(value)
        if compare is None or v is None:
            return False
        # Raw ``<`` raises ``TypeError`` when one side is naive and
        # the other is timezone-aware (the API caller submitted
        # ``"2026-01-01T00:00:00"`` against a target carrying an
        # offset, or vice versa). Validation is the layer that
        # converts bad input into a clean 422 — letting the
        # ``TypeError`` propagate turns a malformed timezone field
        # into a 500. Catch the comparison failure and return False
        # so the standard "must be a date before X" message fires.
        try:
            return v < compare
        except TypeError:
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a date before {params.get('before', '')}."
