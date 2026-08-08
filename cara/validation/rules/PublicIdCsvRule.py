"""Prefixed-public-id CSV rule. Usage: ``public_id_csv:CHN``."""

from __future__ import annotations

import re
from typing import Any

from cara.validation.rules.BaseRule import BaseRule

_PREFIX = re.compile(r"[A-Z][A-Z0-9]{1,9}")
_ULID = re.compile(r"[0-9A-HJKMNP-TV-Z]{26}")
_COMPOSITES = (list, tuple, set, frozenset, dict, bytes, bytearray)


class PublicIdCsvRule(BaseRule):
    """Validate a non-empty CSV of canonical ``PREFIX + ULID`` ids."""

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None or isinstance(value, _COMPOSITES):
            return False
        prefix = str(params.get("public_id_csv") or "")
        if _PREFIX.fullmatch(prefix) is None:
            return False
        tokens = str(value).split(",")
        return bool(tokens) and all(
            token == token.strip()
            and token.startswith(prefix)
            and _ULID.fullmatch(token[len(prefix) :]) is not None
            for token in tokens
        )

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        prefix = str(params.get("public_id_csv") or "")
        return f"'{field}' must be a comma-separated set of {prefix} public ids."
