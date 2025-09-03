class ForeignKeyBuilder:
    """Single Responsibility: Builds foreign key relationships"""

    def __init__(self, column_factory, constraint_manager):
        self.column_factory = column_factory
        self.constraint_manager = constraint_manager
        self._last_foreign = None

    def foreign_id(self, column):
        """Create foreign ID column with constraint"""
        self.column_factory.unsigned_big_integer(column)
        self._last_foreign = self.constraint_manager.add_foreign_key(column)
        return self

    def foreign_uuid(self, column):
        """Create foreign UUID column with constraint"""
        self.column_factory.uuid(column)
        self._last_foreign = self.constraint_manager.add_foreign_key(column)
        return self

    def foreign_id_for(self, model, column=None):
        """Create foreign key for specific model"""
        clm = column if column else model.get_foreign_key()

        if model.get_primary_key_type() == "int":
            return self.foreign_id(clm)
        else:
            return self.foreign_uuid(clm)

    def references(self, column):
        """Set referenced column"""
        if self._last_foreign:
            self._last_foreign.references(column)
        return self

    def on(self, table):
        """Set referenced table"""
        if self._last_foreign:
            self._last_foreign.on(table)
        return self

    def on_delete(self, action):
        """Set on delete action"""
        if self._last_foreign:
            self._last_foreign.on_delete(action)
        return self

    def on_update(self, action):
        """Set on update action"""
        if self._last_foreign:
            self._last_foreign.on_update(action)
        return self

    def add_foreign(self, columns, name=None):
        """Add foreign key using dot notation: from_column.to_column.table"""
        if len(columns.split(".")) != 3:
            raise Exception(
                "Wrong add_foreign argument, the structure is from_column.to_column.table"
            )

        from_column, to_column, table = columns.split(".")
        self._last_foreign = self.constraint_manager.add_foreign_key(
            from_column, name=name
        )
        return self.references(to_column).on(table)
