"""
URL Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is a valid URL.
"""

import re
from typing import Any, Dict

from cara.validation.rules import BaseRule


class URLRule(BaseRule):
    """Validates that a string is a well-formed URL."""

    _pattern = re.compile(
        r"^https?://"
        r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"
        r"localhost|"
        r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
        r"(?::\d+)?"
        r"(?:/?|[/?]\S+)$",
        re.IGNORECASE,
    )

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        if value is None or not isinstance(value, str):
            return False
        return bool(self._pattern.match(value))

    def message(self, field: str, params: Dict[str, Any]) -> str:
        return f"'{field}' must be a valid URL."
