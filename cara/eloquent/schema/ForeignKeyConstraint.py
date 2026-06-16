from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401


class ForeignKeyConstraint:
    def __init__(
        self,
        column,
        foreign_table,
        foreign_column,
        name=None,
    ):
        self.column = column
        self.foreign_table = foreign_table
        self.foreign_column = foreign_column
        self.delete_action = None
        self.update_action = None
        self.constraint_name = name

    def references(self, foreign_column) -> Self:
        self.foreign_column = foreign_column
        return self

    def on(self, foreign_table) -> Self:
        self.foreign_table = foreign_table
        return self

    def on_delete(self, action) -> Self:
        self.delete_action = action
        return self

    def on_update(self, action) -> Self:
        self.update_action = action
        return self

    def name(self, name) -> Self:
        self.constraint_name = name
        return self
