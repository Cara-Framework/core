"""
RequiredUnless Validation Rule.

Field is required unless another field equals a given value.
Usage: ``required_unless:other_field,value``.
"""

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule
from cara.validation.rules.RequiredIfRule import _values_match


class RequiredUnlessRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        raw = params.get("required_unless") or params.get("requiredunless")
        if not raw:
            return True
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) < 2:
            return True
        other_field, expected = parts[0], parts[1]
        data = params.get("_data", {})
        # See ``_values_match`` in RequiredIfRule for the full
        # rationale — the bare ``str(actual) == expected`` form this
        # replaces silently broke on bools (``str(False) == "false"``
        # is False because Python capitalises the string form) and
        # was case-sensitive against literal rule strings that came
        # from form-uppercased HTTP input. Sharing the helper keeps
        # the sibling cross-field rules in lockstep — if a future
        # contributor relaxes the comparison on one, the other moves
        # with it.
        if _values_match(data.get(other_field), expected):
            return True
        if value is None:
            return False
        if isinstance(value, str) and value.strip() == "":
            return False
        return True

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        raw = params.get("required_unless") or params.get("requiredunless", "")
        parts = [p.strip() for p in raw.split(",")]
        other = parts[0] if parts else ""
        expected = parts[1] if len(parts) > 1 else ""
        if other and expected:
            return (
                f"The {attr.lower()} field is required unless "
                f"{other} is '{expected}'."
            )
        return f"The {attr.lower()} field is required unless the given condition is met."
