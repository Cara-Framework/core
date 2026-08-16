"""Canonical definition of ``FieldDefinition``."""

from __future__ import annotations

from typing import Self


class FieldDefinition:
    """Represents a field definition in the new syntax."""

    def __init__(self, field_type, name, **kwargs):
        self.field_type = field_type
        self.name = name
        self.params = kwargs
        self._nullable = False
        self._default = None
        self._backfill_from = None
        self._unique = False
        self._index = False
        self._use_current = False
        # Foreign key properties
        self._is_foreign = False
        self._foreign_key_config = {}

    def nullable(self) -> Self:
        self._nullable = True
        return self

    def default(self, value) -> Self:
        self._default = value
        return self

    def backfill_from(self, expression: str) -> Self:
        """Declare the evolve-mode SQL expression for existing rows.

        Fresh migrations ignore it because a newly created table has no rows
        to fill. ``schema:plan`` uses it between ADD COLUMN and SET NOT NULL.
        """
        if not isinstance(expression, str) or not expression.strip():
            raise ValueError("backfill_from expression must be a non-empty string")
        self._backfill_from = expression
        return self

    def unique(self) -> Self:
        """Mark this field as unique."""
        self._unique = True
        return self

    def index(self) -> Self:
        """Index this field."""
        self._index = True
        return self

    def use_current(self) -> Self:
        """Use the database's current timestamp as this field's default."""
        self._use_current = True
        return self

    def foreign(self) -> Self:
        """Mark this field as a foreign key."""
        self._is_foreign = True
        self._foreign_key_config = {
            "field": self.name,
            "references": None,
            "on": None,
            "on_delete": None,
            "on_update": None,
        }
        return self

    def references(self, column) -> Self:
        """Set the referenced column for foreign key."""
        if self._is_foreign:
            self._foreign_key_config["references"] = column
        return self

    def on(self, table) -> Self:
        """Set the referenced table for foreign key."""
        if self._is_foreign:
            self._foreign_key_config["on"] = table
        return self

    def on_delete(self, action) -> Self:
        """Set the ON DELETE action for foreign key."""
        if self._is_foreign:
            self._foreign_key_config["on_delete"] = action
        return self

    def on_update(self, action) -> Self:
        """Set the ON UPDATE action for foreign key."""
        if self._is_foreign:
            self._foreign_key_config["on_update"] = action
        return self

    def to_dict(self):
        """Convert to the format expected by ModelDiscoverer."""
        params = self.params.copy()
        if self._nullable:
            params["nullable"] = True
        if self._default is not None:
            params["default"] = self._default
        if self._backfill_from is not None:
            params["backfill_from"] = self._backfill_from
        if self._unique:
            params["unique"] = True
        if self._index:
            params["index"] = True
        if self._use_current:
            params["use_current"] = True

        result = {"type": self.field_type, "params": params}

        # Add foreign key information if this is a foreign key
        if self._is_foreign:
            result["foreign_key"] = self._foreign_key_config

        return result
