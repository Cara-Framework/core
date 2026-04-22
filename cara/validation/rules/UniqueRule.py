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
            from cara.eloquent import DB

            query = DB.table(table).where(column, value)
            if ignore_value is not None and ignore_value != "NULL":
                query = query.where(ignore_column, "!=", ignore_value)
            return query.first() is None
        except Exception:
            # DB errors should not silently pass; treat as failure so the
            # caller gets a validation error instead of an accidental write.
            return False

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} has already been taken."
