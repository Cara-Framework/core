"""
Enhanced Exists Validation Rule for the Cara framework.

This module provides an advanced validation rule that checks if a value exists in a database table
with support for custom messages, complex conditions, and multiple validation scenarios.
"""

from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class ExistsRule(BaseRule):
    """
    Advanced validation rule that checks if a value exists in a specified database table.

    Usage examples:
    - "exists:users,email" - Check if email exists in users table
    - "exists:users,email,active,1" - Check if email exists where active=1
    - "exists:users" - Check if value exists in 'id' column (default)
    """

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """Check if value exists in the specified table.column with optional conditions."""
        exists_params = params.get("exists")
        if not exists_params:
            return False

        # Parse parameters: table,column,condition_column,condition_value
        parts = [p.strip() for p in exists_params.split(",")]

        if len(parts) < 1:
            return False

        table = parts[0]
        column = parts[1] if len(parts) > 1 else "id"
        condition_column = parts[2] if len(parts) > 2 else None
        condition_value = parts[3] if len(parts) > 3 else None

        if not table or not value:
            return False

        try:
            # Smart model discovery based on table name
            model_class = self._discover_model(table)
            if model_class:
                try:
                    query = model_class.where(column, value)

                    # Add additional condition if provided
                    if condition_column and condition_value is not None:
                        query = query.where(condition_column, condition_value)

                    result = query.first()
                    return result is not None
                except Exception as e:
                    self._log_debug(
                        f"ExistsRule: model-based query failed for "
                        f"{model_class.__name__}.{column}: "
                        f"{e.__class__.__name__}: {e}"
                    )

            # Fallback: Try DB facade
            try:
                from cara.eloquent import DB

                query = DB.table(table).where(column, value)

                # Add additional condition if provided
                if condition_column and condition_value is not None:
                    query = query.where(condition_column, condition_value)

                result = query.first()
                return result is not None
            except Exception as e:
                self._log_debug(
                    f"ExistsRule: DB-fallback query failed for "
                    f"{table}.{column}: {e.__class__.__name__}: {e}"
                )

        except Exception as e:
            # Outer guard — should never hit. If it does, the rule is
            # broken at a deeper level than the inner blocks; log so we
            # see it in incident review instead of silently returning
            # validation failure.
            self._log_debug(
                f"ExistsRule: unexpected outer failure: "
                f"{e.__class__.__name__}: {e}"
            )

        return False

    @staticmethod
    def _log_debug(msg: str) -> None:
        """Best-effort debug log; survives when Log facade isn't yet booted."""
        try:
            from cara.facades import Log
            Log.debug(msg, category="cara.validation.exists")
        except Exception:
            pass

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        """Return default exists validation message."""
        exists_params = params.get("exists", "")
        parts = [p.strip() for p in exists_params.split(",")]

        table = parts[0] if parts else "table"
        column = parts[1] if len(parts) > 1 else "id"
        condition_column = parts[2] if len(parts) > 2 else None

        attribute = MessageFormatter.format_attribute_name(field)

        # Generate contextual messages
        if condition_column:
            return f"The selected {attribute.lower()} is not valid or not active."
        elif column == "email":
            return f"The {attribute.lower()} must be a registered email address."
        elif column == "id":
            return f"The selected {attribute.lower()} is invalid."
        else:
            return f"The selected {attribute.lower()} does not exist in our records."

    def _discover_model(self, table_name: str):
        """
        Auto-discover model class based on table name.
        Converts table_name to model class name using Laravel conventions.

        Tries both the per-file module layout (``app.models.<Name>``) and the
        aggregated package layout (``app.models`` re-exports). Also tries
        both singular- and plural-derived model names so tables like
        ``product`` (no trailing "s") and ``users`` both resolve.
        """
        try:
            candidates = {
                self._table_to_model_name(table_name),
                self._table_to_model_name_plural(table_name),
            }

            for model_name in candidates:
                if not model_name:
                    continue

                # 1. per-file layout: app.models.<Name>
                try:
                    module = __import__(f"app.models.{model_name}", fromlist=[model_name])
                    cls = getattr(module, model_name, None)
                    if cls is not None:
                        return cls
                except Exception:
                    pass

                # 2. aggregated layout: app.models re-exports
                try:
                    pkg = __import__("app.models", fromlist=[model_name])
                    cls = getattr(pkg, model_name, None)
                    if cls is not None:
                        return cls
                except Exception:
                    pass

            return None
        except Exception:
            return None

    def _table_to_model_name_plural(self, table_name: str) -> str:
        """Treat table as already-singular (no trailing 's' strip)."""
        parts = table_name.split("_")
        return "".join(word.capitalize() for word in parts)

    def _table_to_model_name(self, table_name: str) -> str:
        """Convert table name to model class name."""
        # Remove plural 's' if present
        if table_name.endswith("s"):
            singular = table_name[:-1]
        else:
            singular = table_name

        # Convert snake_case to PascalCase
        parts = singular.split("_")
        return "".join(word.capitalize() for word in parts)
