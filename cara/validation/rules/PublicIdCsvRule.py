"""Prefixed-public-id CSV rule. Usage: ``public_id_csv:CHN``."""

from __future__ import annotations

from typing import Any

from cara.support import is_public_id, is_public_id_prefix
from cara.validation.rules.BaseRule import BaseRule

_COMPOSITES = (list, tuple, set, frozenset, dict, bytes, bytearray)


class PublicIdCsvRule(BaseRule):
    """Validate a non-empty CSV of canonical ``PREFIX + ULID`` ids.

    The id grammar itself lives in ``cara.support.PublicIds`` — the
    same single source the filter-tree entity fields validate through.
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None or isinstance(value, _COMPOSITES):
            return False
        prefix = str(params.get("public_id_csv") or "")
        if not is_public_id_prefix(prefix):
            return False
        tokens = str(value).split(",")
        return bool(tokens) and all(
            token == token.strip() and is_public_id(token, prefix) for token in tokens
        )

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        prefix = str(params.get("public_id_csv") or "")
        return f"'{field}' must be a comma-separated set of {prefix} public ids."
