"""
RequiredIf Validation Rule.

Field becomes required when another field equals a given value.
Usage: ``required_if:other_field,value`` (e.g. ``required_if:type,paid``).
"""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class RequiredIfRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        raw = params.get("required_if") or params.get("requiredif")
        if not raw:
            return True
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) < 2:
            return True
        other_field, expected = parts[0], parts[1]
        data = params.get("_data", {})
        if not _values_match(data.get(other_field), expected):
            return True
        # required-if: field must be present and not empty
        if value is None:
            return False
        return not (isinstance(value, str) and value.strip() == "")

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        raw = params.get("required_if") or params.get("requiredif", "")
        parts = [p.strip() for p in raw.split(",")]
        other = parts[0] if parts else ""
        expected = parts[1] if len(parts) > 1 else ""
        if expected:
            return f"The {attr.lower()} field is required when {other} is '{expected}'."
        return (
            f"The {attr.lower()} field is required when {other} equals the given value."
        )


def _values_match(actual: Any, expected: str) -> bool:
    """Compare a runtime value against the rule's literal expected
    string. The rule is parsed from a delimited string so ``expected``
    is always ``str`` (e.g. ``"false"``), while ``actual`` comes from
    the validated payload and is whatever shape the caller submitted
    (``bool``, ``int``, ``str``, …).

    Pre-fix the comparison was the bare ``str(actual) != expected``,
    which silently failed on bools and case-mismatched literals:

      * Python's ``str(False)`` returns ``'False'`` (capital ``F``);
        the canonical Laravel-style ``required_if:is_active,false``
        rule literal is lowercase, so ``str(False) == 'false'`` is
        always ``False`` and the field was never treated as required
        when ``is_active`` came in as a real bool.
      * ``required_if:role,Admin`` with payload ``role="ADMIN"`` (an
        HTML form that uppercased the value) would silently bypass
        the requirement.

    The fix normalises both sides to lowercase strings, plus a
    bool-aware fast path so ``"true"``/``"false"`` rule literals
    match Python bools directly. Numeric coercion is NOT attempted —
    ``required_if:age,18`` matching ``age=18`` (int) already worked
    via ``str(18) == "18"`` and changing the coercion shape risks
    silently matching loose inputs (``"18.0"``, ``"+18"``, …).
    """
    if isinstance(actual, bool):
        # Bool BEFORE int — Python's ``isinstance(True, int)`` is True.
        # Compare lower-cased literal.
        return str(actual).lower() == expected.strip().lower()
    if actual is None:
        return expected.lower() in {"none", "null"}
    return str(actual).strip().lower() == expected.strip().lower()
