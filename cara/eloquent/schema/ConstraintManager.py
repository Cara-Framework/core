class ConstraintManager:
    """Single Responsibility: Manages table constraints and indexes"""

    def __init__(self, table):
        self.table = table

    def add_primary_key(self, columns, name=None):
        """Add primary key constraint"""
        if not isinstance(columns, list):
            columns = [columns]

        self.table.add_constraint(
            name or f"{self.table.name}_{'_'.join(columns)}_primary",
            "primary_key",
            columns=columns,
        )
        return self

    def add_unique_constraint(self, columns, name=None):
        """Add unique constraint"""
        if not isinstance(columns, list):
            columns = [columns]

        self.table.add_constraint(
            name or f"{self.table.name}_{'_'.join(columns)}_unique",
            "unique",
            columns=columns,
        )
        return self

    def add_index(self, columns, name=None):
        """Add index"""
        if not isinstance(columns, list):
            columns = [columns]

        self.table.add_index(
            columns,
            name or f"{self.table.name}_{'_'.join(columns)}_index",
            "index",
        )
        return self

    def add_fulltext_index(self, columns, name=None):
        """Add fulltext index"""
        if not isinstance(columns, list):
            columns = [columns]

        self.table.add_constraint(
            name or f"{'_'.join(columns)}_fulltext",
            "fulltext",
            columns,
        )
        return self

    def add_foreign_key(self, column, name=None):
        """Add foreign key constraint"""
        return self.table.add_foreign_key(
            column,
            name=name or f"{self.table.name}_{column}_foreign",
        )
