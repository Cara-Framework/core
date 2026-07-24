"""
Enhanced Exists Validation Rule for the Cara framework.

This module provides an advanced validation rule that checks if a value exists in a database table
with support for custom messages, complex conditions, and multiple validation scenarios.
"""

from __future__ import annotations

import importlib
import re
from typing import Any

from cara.exceptions import ConfigurationException
from cara.support import ModuleManager
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
        # validating them prevents unsafe identifiers from reaching the
        # model query builder.
        for ident in (table, column, condition_column):
            if ident is not None and not _SAFE_IDENTIFIER_RE.match(ident):
                raise ConfigurationException(
                    f"ExistsRule identifier '{ident}' is not a safe SQL identifier"
                )

        model_class = self._discover_model(table)
        query = model_class.where(column, value)
        if condition_column and condition_value is not None:
            query = query.where(condition_column, condition_value)
        return query.first() is not None

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
        """Resolve one table through the configured, generated model barrel.

        The model barrel is the canonical registry: every public model is
        exported there and each model owns an explicit ``__table__``. Missing
        or ambiguous registrations are configuration errors. Raw SQL fallback
        is forbidden because it would bypass model scopes such as tenancy.
        """
        models_module = ModuleManager.models_module()
        if not isinstance(models_module, str) or not models_module:
            raise ConfigurationException("Models module is not configured")
        try:
            model_barrel = importlib.import_module(models_module)
        except ImportError as exc:
            raise ConfigurationException(
                f"Configured models module '{models_module}' is not importable"
            ) from exc

        matches = [
            candidate
            for candidate in vars(model_barrel).values()
            if isinstance(candidate, type)
            and getattr(candidate, "__table__", None) == table_name
        ]
        if len(matches) != 1:
            raise ConfigurationException(
                f"Table '{table_name}' must resolve to exactly one model in "
                f"'{models_module}'; found {len(matches)}"
            )
        return matches[0]
