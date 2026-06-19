"""
Unique Validation Rule.

Mirrors Laravel's ``unique`` rule. Fails when a record already exists in
the given table/column (optionally excluding a specific id).

Usage:
  ``unique:users,email``
  ``unique:users,email,5``              # ignore row with id=5
  ``unique:users,email,5,user_id``      # ignore row where user_id=5
"""

from __future__ import annotations

import re
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule

# Only allow safe SQL identifiers: letters, digits, underscores.
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class UniqueRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        raw = params.get("unique")
        if not raw or value is None:
            return True

        parts = [p.strip() for p in raw.split(",")]
        if not parts:
            return True

        table = parts[0]
        column = parts[1] if len(parts) > 1 else field
        ignore_value = parts[2] if len(parts) > 2 else None
        ignore_column = parts[3] if len(parts) > 3 else "id"

        # Defence-in-depth: reject identifiers that aren't plain
        # alphanumeric/underscore names to prevent SQL injection
        # through identifier interpolation in the raw SQL below.
        for ident in (table, column, ignore_column):
            if ident is not None and not _SAFE_IDENTIFIER_RE.match(ident):
                return False

        try:
            from cara.facades import DB

            sql = f'SELECT 1 FROM "{table}" WHERE "{column}" = %s LIMIT 1'
            sql_params = [value]
            if ignore_value is not None and ignore_value != "NULL":
                sql = f'SELECT 1 FROM "{table}" WHERE "{column}" = %s AND "{ignore_column}" != %s LIMIT 1'
                sql_params.append(ignore_value)

            rows = DB.select(sql, sql_params)
            return len(rows) == 0
        except Exception as exc:
            try:
                from cara.facades import Log

                Log.error("UniqueRule: DB query failed for %s.%s: %s: %s", table, column, exc.__class__.__name__, exc, category='cara.validation.unique', exc_info=True)
            except (ImportError, RuntimeError):
                pass
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} has already been taken."
