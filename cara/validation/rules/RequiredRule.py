"""
Required Field Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is present and not empty.
"""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class RequiredRule(BaseRule):
    """Validates that a value is not None and not an empty string."""

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        """Check if value is present and not empty.

        "Empty" means: missing key (None), whitespace-only string, or
        zero-length collection (list, tuple, set, frozenset, dict,
        bytes, bytearray). Without the collection check, an attacker
        can satisfy ``required|array`` on a field by submitting ``[]``
        — the previous implementation only rejected None and empty
        strings, so empty arrays slipped past every required check on
        array-typed fields. ``frozenset`` is included because it is
        NOT a subclass of ``set`` (siblings — ``isinstance(frozenset(),
        set)`` is False), and ``bytes`` / ``bytearray`` because empty
        binary uploads otherwise looked "present" to the validator.
        """
        if value is None:
            return False
        if isinstance(value, str) and value.strip() == "":
            return False
        if (
            isinstance(value, (list, tuple, set, frozenset, dict, bytes, bytearray))
            and len(value) == 0
        ):
            return False
        return True

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        """Return default required field message."""
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} field is required."
