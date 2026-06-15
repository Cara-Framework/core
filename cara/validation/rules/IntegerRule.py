"""
Integer Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is an integer.
"""

from __future__ import annotations

import re
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class IntegerRule(BaseRule):
    """Validates that a value is an integer or integer string.

    String inputs must match the canonical form ``^[+-]?\\d+$``
    — no surrounding whitespace, no PEP 515 underscore separators,
    no control characters. Python's ``int()`` accepts all of those
    (``int("\\t42") == 42``, ``int("1_000") == 1000``), so a pre-fix
    ``try: int(value)`` swallowed them silently. The raw, untrusted
    string then flowed into downstream code — log lines, Prometheus
    labels, error messages — where a newline-tainted value
    corrupts the record.

    Strict shape at the rule boundary is what Laravel's ``integer``
    rule enforces too.
    """

    # Anchored shape — ``fullmatch`` removes the default-mode ``$``
    # quirk that lets ``"42\n"`` slip past ``match()`` with a
    # trailing ``$`` anchor.
    _STRICT_INT_PATTERN = re.compile(r"^[+-]?\d+$")

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        """Check if value is an integer."""
        if value is None:
            return False

        if isinstance(value, bool):
            return False

        if isinstance(value, int):
            return True

        if isinstance(value, str):
            # Strict shape check — see class docstring. ``fullmatch``
            # rejects trailing newlines that the default ``$`` would
            # otherwise allow.
            return bool(self._STRICT_INT_PATTERN.fullmatch(value))

        return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        """Return default integer validation message."""
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} field must be an integer."
