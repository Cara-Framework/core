"""
Min Length Validation Rule for the Cara framework.

This module provides a validation rule that checks if a string value meets a minimum length.
"""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class MinLengthRule(BaseRule):
    """Validates that a string has at least a given number of characters.

    Usage: "min_length:5"
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        """Check if string value meets minimum length requirement."""
        if value is None or not isinstance(value, str):
            return False

        min_length = params.get("min_length")
        if min_length is None:
            return False

        try:
            threshold = int(min_length)
        except (TypeError, ValueError):
            # Misconfigured rule literal — ``min_length:abc`` would
            # otherwise raise ValueError out of the validator and
            # 500 the request. Sibling ``MinRule``/``MaxRule`` log
            # + pass-through on the same shape; for a length rule
            # the safe default is to FAIL the value (the developer
            # clearly intended a constraint but typo'd the
            # threshold — letting the input through silently would
            # mask the typo for as long as no string is short enough
            # to be caught by accident).
            try:
                from cara.facades import Log

                Log.warning("MinLengthRule: non-numeric min_length parameter %s on field %s; failing value as defensive default", min_length, field, category='cara.validation')
            except ImportError:
                pass
            return False
        return len(value) >= threshold

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        """Return default minimum length validation message."""
        attribute = MessageFormatter.format_attribute_name(field)
        min_val = params.get("min_length", 0)
        return (
            f"The {attribute.lower()} field must be at least {min_val} characters long."
        )
