"""Less-than-or-equal rule. Usage: ``lte:10`` or ``lte:field``."""

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


def _to_number(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class LteRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        threshold = params.get("lte")
        if threshold is None or value is None:
            return False
        data = params.get("_data", {})
        other = data.get(threshold)
        # When ``threshold`` is a field-name reference and the
        # referenced field is absent from the payload, treat the
        # cross-field check as a no-op (passes). Pre-fix the rule
        # returned False in this case, which broke every legitimate
        # single-bound range query (``?min_price=50`` with no
        # ``max_price``) when the validator used the canonical
        # ``min_price: lte:max_price`` cross-field guard. Mirrors
        # Laravel's ``lte:<field>`` semantic — the constraint is
        # between two fields, so if one isn't there, there's nothing
        # to compare and nothing to violate. Differentiated from a
        # malformed rule (threshold isn't a number AND isn't a
        # present field name) by checking ``_to_number(threshold)``:
        # a literal numeric threshold still falls through to the
        # numeric comparison below.
        if other is None and _to_number(threshold) is None:
            return True
        compare_to = _to_number(threshold) if other is None else _to_number(other)
        val = _to_number(value)
        if compare_to is None or val is None:
            return False
        return val <= compare_to

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return (
            f"The {attr.lower()} must be less than or equal to {params.get('lte', '')}."
        )
