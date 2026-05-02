"""
Unique Validation Rule.

Mirrors Laravel's ``unique`` rule. Fails when a record already exists in
the given table/column (optionally excluding a specific id).

Usage:
  ``unique:users,email``
  ``unique:users,email,5``              # ignore row with id=5
  ``unique:users,email,5,user_id``      # ignore row where user_id=5
"""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class UniqueRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
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
                Log.error(
                    f"UniqueRule: DB query failed for {table}.{column}: "
                    f"{exc.__class__.__name__}: {exc}",
                    category="cara.validation.unique",
                )
            except Exception:
                pass
            return False

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} has already been taken."
