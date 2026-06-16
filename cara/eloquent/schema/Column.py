from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401


class Column:
    """Used for creating or modifying columns."""

    def __init__(
        self,
        name,
        column_type,
        length=None,
        values=None,
        nullable=False,
        default=None,
        signed=None,
        default_is_raw=False,
        column_python_type=str,
    ):
        self.column_type = column_type
        self.column_python_type = column_python_type
        self.name = name
        self.length = length
        self.values = values or []
        self.is_null = nullable
        self._after = None
        self.old_column = ""
        self.default_value = default
        self._signed = signed
        self.default_is_raw = default_is_raw
        self.primary = False
        self.comment = None

    def nullable(self) -> Self:
        """
        Sets this column to be nullable.

        Returns:
            self
        """
        self.is_null = True
        return self

    def signed(self) -> Self:
        """
        Sets this column to be nullable.

        Returns:
            self
        """
        self._signed = "signed"
        return self

    def unsigned(self) -> Self:
        """
        Sets this column to be nullable.

        Returns:
            self
        """
        self._signed = "unsigned"
        return self

    def not_nullable(self) -> Self:
        """
        Sets this column to be not nullable.

        Returns:
            self
        """
        self.is_null = False
        return self

    def set_as_primary(self):
        self.primary = True

    def rename(self, column) -> Self:
        """
        Renames this column to a new name.

        Arguments:
            column {string} -- The old column name

        Returns:
            self
        """
        self.old_column = column
        return self

    def after(self, after) -> Self:
        """
        Sets the column that this new column should be created after.

        This is useful for setting the location of the new column in the table schema.

        Arguments:
            after {string} -- The column that this new column should be created after

        Returns:
            self
        """
        self._after = after
        return self

    def get_after_column(self):
        """
        Sets the column that this new column should be created after.

        This is useful for setting the location of the new column in the table schema.

        Arguments:
            after {string} -- The column that this new column should be created after

        Returns:
            self
        """
        return self._after

    def default(self, value, raw=False) -> Self:
        """
        Sets a default value for this column.

        Arguments:
            value {string} -- A default value.
            raw {bool} -- should the value be quoted

        Returns:
            self
        """
        self.default_value = value
        self.default_is_raw = raw
        return self

    def change(self) -> Self:
        """
        Sets the schema to create a modify sql statement.

        Returns:
            self
        """
        self._action = "modify"
        return self

    def use_current(self) -> Self:
        """
        Sets the column to use a current timestamp.

        Used for timestamp columns.

        Returns:
            self
        """
        self.default_value = "current"
        return self

    def add_comment(self, comment) -> Self:
        self.comment = comment
        return self
