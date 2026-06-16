from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401

from .Column import Column
from .Constraint import Constraint
from .ForeignKeyConstraint import ForeignKeyConstraint
from .Index import Index


class Table:
    def __init__(self, table):
        self.name = table
        self.added_columns = {}
        self.added_constraints = {}
        self.added_indexes = {}
        self.added_foreign_keys = {}
        self.renamed_columns = {}
        self.drop_indexes = {}
        # Columns to DROP on an ALTER. Blueprint.drop_column() delegates here,
        # and the platform's compile_alter_sql() already emits DROP COLUMN from
        # ``get_dropped_columns()`` — Table just never declared the slot/method,
        # so every migration ``down()`` that drops a column (and every
        # auto-generated update migration's rollback) raised
        # ``'Table' object has no attribute 'drop_column'``.
        self.dropped_columns = []
        self.foreign_keys = {}
        self.primary_key = None
        self.comment = None

    def drop_column(self, *columns) -> Self:
        """Queue one or more columns for ``ALTER TABLE … DROP COLUMN``."""
        for column in columns:
            if column:
                self.dropped_columns.append(column)
        return self

    def get_dropped_columns(self):
        return self.dropped_columns

    def drop_index(self, index) -> Self:
        """Queue an index for drop on an ALTER."""
        self.drop_indexes[index] = index
        return self

    def rename_column(self, old_column, new_column, data_type=None, length=None) -> Self:
        """Queue a column rename on an ALTER (platform reads new name off the
        Column, old name off the dict key)."""
        self.renamed_columns[old_column] = Column(
            new_column, data_type, length=length
        )
        return self

    def __str__(self):
        """Return table name when converted to string"""
        return self.name

    def __repr__(self):
        """Return table name for debugging"""
        return f"Table({self.name})"

    def add_column(
        self,
        name=None,
        column_type=None,
        length=None,
        values=None,
        nullable=False,
        default=None,
        signed=None,
        default_is_raw=False,
        primary=False,
        column_python_type=str,
    ):
        column = Column(
            name,
            column_type,
            length=length,
            nullable=nullable,
            values=values or [],
            default=default,
            signed=signed,
            default_is_raw=default_is_raw,
            column_python_type=column_python_type,
        )
        if primary:
            column.set_as_primary()
        self.added_columns.update({name: column})
        return column

    def add_constraint(self, name, constraint_type, columns=None):
        self.added_constraints.update(
            {
                name: Constraint(
                    name,
                    constraint_type,
                    columns=columns or [],
                )
            }
        )

    def add_foreign_key(
        self,
        column,
        table=None,
        foreign_column=None,
        name=None,
    ):
        foreign_key = ForeignKeyConstraint(
            column,
            table,
            foreign_column,
            name=name or f"{self.name}_{column}_foreign",
        )
        self.added_foreign_keys.update({column: foreign_key})

        return foreign_key

    def get_added_foreign_keys(self):
        return self.added_foreign_keys

    def get_constraint(self, name):
        return self.added_constraints[name]

    def get_added_constraints(self):
        return self.added_constraints

    def get_added_columns(self):
        return self.added_columns

    def get_renamed_columns(self):
        return self.added_columns

    def set_primary_key(self, columns) -> Self:
        self.primary_key = columns
        return self

    def add_index(self, column, name, index_type):
        self.added_indexes.update({name: Index(column, name, index_type)})

    def get_index(self, name):
        return self.added_indexes[name]

    def add_comment(self, comment) -> Self:
        self.comment = comment
        return self
