from __future__ import annotations

from typing import Self

from .Column import Column
from .Table import Table


class TableDiff(Table):
    def __init__(self, name):
        self.name = name
        self.from_table = None
        self.new_name = None
        # Read by ``SQLitePlatform``/``PostgresPlatform`` when compiling an
        # ALTER, and populated by NOTHING. ``Blueprint.drop_index`` reaches
        # ``Table.drop_index``, which writes a ``drop_indexes`` dict no
        # reader ever consults, and ``Blueprint`` builds a plain ``Table``
        # rather than a ``TableDiff`` — so the whole Blueprint drop-index
        # path is a silent no-op end to end. Wiring it means moving the two
        # lists onto ``Table`` and pointing ``drop_index`` at them; that is a
        # behaviour change with no test today, not a comment.
        self.removed_indexes = []
        self.removed_unique_indexes = []
        self.added_indexes = {}
        self.added_columns = {}
        self.changed_columns = {}
        self.dropped_columns = []
        self.dropped_foreign_keys = []
        self.dropped_primary_keys = []
        self.renamed_columns = {}
        self.added_constraints = {}
        self.added_foreign_keys = {}
        self.comment = None

    def get_renamed_columns(self):
        return self.renamed_columns

    def rename_column(
        self,
        original_name,
        new_name,
        column_type=None,
        length=None,
        nullable=False,
        default=None,
    ):
        self.renamed_columns.update(
            {
                original_name: Column(
                    new_name,
                    column_type,
                    length=length,
                    nullable=nullable,
                    default=default,
                )
            }
        )

    def drop_column(self, name):
        self.dropped_columns.append(name)

    def get_dropped_columns(self):
        return self.dropped_columns

    def drop_foreign(self, name) -> Self:
        self.dropped_foreign_keys.append(name)
        return self

    def drop_primary(self, name) -> Self:
        self.dropped_primary_keys.append(name)
        return self

    def change_column(self, added_column):
        self.added_columns.pop(added_column.name)

        self.changed_columns.update({added_column.name: added_column})

    def add_comment(self, comment) -> Self:
        self.comment = comment
        return self
