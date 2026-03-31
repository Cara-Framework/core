"""
Regex Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value matches a given regular expression.
"""

import re
from typing import Any, Dict
from cara.validation.rules import BaseRule


class RegexRule(BaseRule):
    """
    Validates that a string matches a given regular expression.

    Usage: "regex:^[A-Z0-9_]+$"
    """

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        raw_pattern = params.get("regex")
        if not raw_pattern or not isinstance(value, str):
            return False

        try:
            pattern = re.compile(raw_pattern)
        except re.error:
            return False

        return bool(pattern.match(value))

    def message(self, field: str, params: Dict[str, Any]) -> str:
        return f"'{field}' format is invalid."
