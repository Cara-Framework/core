"""
Alphanumeric Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value contains only alphanumeric characters.
"""

from __future__ import annotations

import re
from typing import Any

from cara.validation.rules import BaseRule


class AlphanumRule(BaseRule):
    """Validates that a value contains only alphanumeric characters."""

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None:
            return False

        if not isinstance(value, str):
            value = str(value)

        # Check if value contains only alphanumeric characters. ``fullmatch``
        # (not ``match(...$)``): ``$`` also matches before a trailing newline,
        # so "abc\n" would otherwise pass an "alphanumeric only" check.
        return bool(re.fullmatch(r"[a-zA-Z0-9]+", value))

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        return f"'{field}' must contain only alphanumeric characters."
