"""
Enhanced Exists Validation Rule for the Cara framework.

This module provides an advanced validation rule that checks if a value exists in a database table
with support for custom messages, complex conditions, and multiple validation scenarios.
"""

from __future__ import annotations

import re
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule

# Only allow safe SQL identifiers: letters, digits, underscores.
# Prevents SQL injection through table/column name interpolation.
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ExistsRule(BaseRule):
    """
    Advanced validation rule that checks if a value exists in a specified database table.

    Usage examples:
    - "exists:users,email" - Check if email exists in users table
    - "exists:users,email,active,1" - Check if email exists where active=1
    - "exists:users" - Check if value exists in 'id' column (default)
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
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

        # ``not value`` would treat integer ``0`` (and ``False``) as
        # "missing", but those are legitimate column values — anonymous
        # / system user rows often use ``id=0`` as a sentinel. Reject
        # only the genuinely empty inputs so the DB lookup actually
        # runs for ``value=0``.
        if not table or value is None or value == "":
            return False

        # Defence-in-depth: reject identifiers that aren't plain
        # alphanumeric/underscore names. The rule parameters originate
        # from developer-authored request classes (e.g.
        # ``"exists:users,email"``), not from user input, but
        # validating them prevents any SQL injection through identifier
        # interpolation in the raw-SQL fallback path below.
        for ident in (table, column, condition_column):
            if ident is not None and not _SAFE_IDENTIFIER_RE.match(ident):
                self._log_debug(
                    f"ExistsRule: rejected unsafe identifier '{ident}'"
                )
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

            # Fallback: Try DB facade with raw SELECT
            try:
                from cara.facades import DB

                sql = f'SELECT 1 FROM "{table}" WHERE "{column}" = %s'
                params = [value]
                if condition_column and condition_value is not None:
                    sql += f' AND "{condition_column}" = %s'
                    params.append(condition_value)
                sql += " LIMIT 1"
                rows = DB.select(sql, params)
                return len(rows) > 0
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
                f"ExistsRule: unexpected outer failure: {e.__class__.__name__}: {e}"
            )

        return False

    @staticmethod
    def _log_debug(msg: str) -> None:
        """Best-effort debug log; survives when Log facade isn't yet booted."""
        try:
            from cara.facades import Log

            Log.debug(msg, category="cara.validation.exists")
        except ImportError:
            pass

    @staticmethod
    def _log_missing_model(
        table_name: str, module_bases: list[str], candidates: set[str]
    ) -> None:
        """LOUD warning: model discovery failed → caller uses UNSCOPED raw SQL.

        A missed model is SECURITY-relevant: the ExistsRule then runs a raw
        SELECT with NO ``TenantScope`` applied, so an existence check can leak
        across tenants. This must never be silent — log via the cara ``Log``
        facade at WARNING, and if the facade isn't booted (or anything else
        goes wrong) fall back to stderr rather than swallowing the signal.
        """
        names = ", ".join(sorted(n for n in candidates if n)) or "<none>"
        pkgs = ", ".join(module_bases) or "<none>"
        msg = (
            f"ExistsRule: no model resolved for table '{table_name}' "
            f"(tried classes [{names}] in packages [{pkgs}]); falling back to "
            f"UNSCOPED raw SQL — TenantScope will NOT be applied to this "
            f"existence check."
        )
        try:
            from cara.facades import Log

            Log.warning(msg, category="cara.validation.exists")
        except Exception:
            import sys

            print(f"WARNING: {msg}", file=sys.stderr)

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        """Return default exists validation message."""
        exists_params = params.get("exists", "")
        parts = [p.strip() for p in exists_params.split(",")]

        # ``table`` resolved but currently unused by the message
        # branches below — pre-fix this line was the expression
        # ``parts[0] if parts else "table"`` with the result silently
        # discarded (statement expression, no assignment), a copy-
        # paste leftover from when an earlier message variant
        # surfaced the table name. Kept as an assignment now so the
        # value is reachable for the next maintainer who wants to
        # add a table-aware branch without re-discovering the parse.
        table = parts[0] if parts else "table"
        column = parts[1] if len(parts) > 1 else "id"
        _ = table  # explicit unused-name marker until a branch uses it
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

        Resolution order, per candidate model name (both singular- and
        plural-derived so ``product`` and ``users`` both resolve):

          1. the CONFIGURED models package first — ``ModuleManager.
             models_module()`` (defaults to ``commons.models``) — tried both
             per-file (``<pkg>.<Name>``) and aggregated (``<pkg>`` barrel
             re-export);
          2. ``app.models`` as a backward-compatible fallback (same layouts).

        Returns ``None`` only when nothing resolves in ANY package. That is a
        security-relevant outcome — the caller then drops to UNSCOPED raw SQL
        (no ``TenantScope``) — so the None path emits a LOUD warning naming the
        table via :meth:`_log_missing_model`. A silent scope-drop is forbidden.
        """
        # Configured package first (commons.models by default), then the legacy
        # app.models location. De-duplicated, order preserved.
        try:
            from cara.support import ModuleManager

            configured = ModuleManager.models_module()
        except Exception:
            configured = "commons.models"

        module_bases: list[str] = []
        for base in (configured, "commons.models", "app.models"):
            if base and base not in module_bases:
                module_bases.append(base)

        candidates = {
            self._table_to_model_name(table_name),
            self._table_to_model_name_plural(table_name),
        }

        try:
            for base in module_bases:
                for model_name in candidates:
                    if not model_name:
                        continue

                    # 1. per-file layout: <base>.<Name>
                    try:
                        module = __import__(
                            f"{base}.{model_name}", fromlist=[model_name]
                        )
                        cls = getattr(module, model_name, None)
                        if cls is not None:
                            return cls
                    except (ImportError, AttributeError):
                        pass

                    # 2. aggregated layout: <base> barrel re-exports
                    try:
                        pkg = __import__(base, fromlist=[model_name])
                        cls = getattr(pkg, model_name, None)
                        if cls is not None:
                            return cls
                    except (ImportError, AttributeError):
                        pass
        except (ImportError, AttributeError, TypeError, RuntimeError):
            pass

        # Reached only when no model resolved in ANY package → caller falls to
        # UNSCOPED raw SQL. Announce loudly; never a silent TenantScope bypass.
        self._log_missing_model(table_name, module_bases, candidates)
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
