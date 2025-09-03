"""
UUID Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is a valid UUID format.
"""

import re
from typing import Any, Dict

from cara.validation.rules import BaseRule


class UuidRule(BaseRule):
    """Validates that a value is a valid UUID format."""

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        if value is None:
            return False

        if not isinstance(value, str):
            value = str(value)

        # Check if value is a valid UUID format (with or without hyphens)
        uuid_pattern = r"^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$"
        return bool(re.match(uuid_pattern, value))

    def message(self, field: str, params: Dict[str, Any]) -> str:
        return f"'{field}' must be a valid UUID format."
