"""
Unique Validation Rule.

Laravel's ``unique`` rule, queried through the table's MODEL rather than a
raw ``DB::table`` lookup: the model's scopes (tenancy, soft deletes) apply,
so a value another tenant holds, or only a trashed row holds, is not
"taken". Fails when a visible record already has the value (optionally
excluding one row). A database error propagates — it is an outage, not an
answer about the input.

Usage:
  ``unique:users,email``
  ``unique:users,email,5``              # ignore row with id=5
  ``unique:users,email,5,user_id``      # ignore row where user_id=5
"""

from __future__ import annotations

import re
from typing import Any

from cara.exceptions import ConfigurationException
from cara.validation.MessageFormatter import MessageFormatter
from cara.validation.rules._ModelResolution import _resolve_rule_model
from cara.validation.rules.BaseRule import BaseRule

# Only allow safe SQL identifiers: letters, digits, underscores.
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class UniqueRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        raw = params.get("unique")
        if not raw or value is None:
            return True

        parts = [p.strip() for p in raw.split(",")]
        table = parts[0]
        column = parts[1] if len(parts) > 1 else field
        ignore_value = parts[2] if len(parts) > 2 else None
        ignore_column = parts[3] if len(parts) > 3 else "id"

        # Defence-in-depth: the parameters come from developer-authored
        # request classes, and an unsafe identifier is a misconfiguration,
        # never a verdict on the input.
        for ident in (table, column, ignore_column):
            if not _SAFE_IDENTIFIER_RE.match(ident):
                raise ConfigurationException(
                    f"UniqueRule identifier '{ident}' is not a safe SQL identifier"
                )

        query = _resolve_rule_model(table).where(column, value)
        if ignore_value is not None and ignore_value != "NULL":
            query = query.where(ignore_column, "!=", ignore_value)
        return query.first() is None

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} has already been taken."
