"""
In-CSV Validation Rule for the Cara framework.

This module provides a validation rule for comma-separated multi-value filter
parameters where every token must come from a fixed allowlist.
"""

from __future__ import annotations

from typing import Any

from cara.validation.rules.BaseRule import BaseRule


class InCsvRule(BaseRule):
    """
    Validates a comma-separated string where EVERY token must be in the given
    list of acceptable values — the multi-value (``IN (...)``) shape of an
    index filter parameter, e.g. ``status=draft,active``.

    A single value is the one-token case of the same shape, so a parameter can
    graduate from ``in:`` to ``in_csv:`` without breaking existing callers.

    Usage: "in_csv:apple,banana,orange"
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None:
            return False

        # Same scalar guard as InRule: a list/dict/bytes value never str()s
        # into a meaningful CSV, and an attacker-controlled ``__str__`` must
        # not run at all.
        if isinstance(value, (list, tuple, set, frozenset, dict, bytes, bytearray)):
            return False

        allowed = params.get("in_csv")
        if not allowed:
            return False
        allowlist = {token.strip() for token in allowed.split(",")}

        tokens = [token.strip() for token in str(value).split(",")]
        # An empty token (``a,,b`` or a bare ``,``) is a malformed filter,
        # not an empty one — absence is spelled by omitting the parameter.
        if any(not token for token in tokens):
            return False
        return all(token in allowlist for token in tokens)

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        allowed = params.get("in_csv", "")
        return f"'{field}' must be a comma-separated set of: {allowed}."
