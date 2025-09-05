from .ColumnFactory import ColumnFactory
from .ConstraintManager import ConstraintManager
from .ForeignKeyBuilder import ForeignKeyBuilder


class Blueprint:
    """
    Single Responsibility: Coordinates table schema building using composition
    Open/Closed: Extensible through factory components
    Dependency Inversion: Depends on abstractions (factories and managers)
    """

    def __init__(
        self,
        grammar,
        table="",
        connection=None,
        platform=None,
        schema=None,
        action=None,
        default_string_length=None,
        dry=False,
    ):
        """Initialize Blueprint with composition pattern"""
        from .Table import Table

        self.grammar = grammar
        self.table = Table(table)
        self.connection = connection
        self.platform = platform
        self.action = action
        self._default_string_length = default_string_length or 255
        self.dry = dry
        self._last_column = None
        self._last_foreign = None

        # Composition: Delegate responsibilities to specialized components
        self.column_factory = ColumnFactory(self.table, self._default_string_length)
        self.constraint_manager = ConstraintManager(self.table)
        self.foreign_key_builder = ForeignKeyBuilder(
            self.column_factory, self.constraint_manager
        )

    # === Column Creation - Delegation to ColumnFactory ===

    def string(self, column, length=255, nullable=False):
        """Create string column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.string(column, length, nullable)
        return self

    def integer(self, column, length=11, nullable=False):
        """Create integer column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.integer(column, length, nullable)
        return self

    def tiny_integer(self, column, length=1, nullable=False):
        """Create tiny integer column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.tiny_integer(column, length, nullable)
        return self

    def small_integer(self, column, length=5, nullable=False):
        """Create small integer column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.small_integer(column, length, nullable)
        return self

    def medium_integer(self, column, length=7, nullable=False):
        """Create medium integer column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.medium_integer(column, length, nullable)
        return self

    def big_integer(self, column, length=32, nullable=False):
        """Create big integer column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.big_integer(column, length, nullable)
        return self

    def unsigned_integer(self, column, nullable=False):
        """Create unsigned integer column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.unsigned_integer(column, nullable)
        return self

    def increments(self, column, nullable=False):
        """Create auto-incrementing column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.increments(column, nullable)
        return self

    def tiny_increments(self, column, nullable=False):
        """Create tiny auto-incrementing column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.tiny_increments(column, nullable)
        return self

    def id(self, column="id"):
        """Create ID column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.id(column)
        return self

    def uuid(self, column, nullable=False, length=36):
        """Create UUID column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.uuid(column, nullable, length)
        return self

    def big_increments(self, column, nullable=False):
        """Create big auto-incrementing column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.big_increments(column, nullable)
        return self

    def unsigned_big_integer(self, column, length=32, nullable=False):
        """Create unsigned big integer column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.unsigned_big_integer(column, length, nullable)
        return self

    def binary(self, column, nullable=False):
        """Create binary column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.binary(column, nullable)
        return self

    def boolean(self, column, nullable=False):
        """Create boolean column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.boolean(column, nullable)
        return self

    def char(self, column, length=1, nullable=False):
        """Create char column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.char(column, length, nullable)
        return self

    def date(self, column, nullable=False):
        """Create date column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.date(column, nullable)
        return self

    def time(self, column, nullable=False):
        """Create time column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.time(column, nullable)
        return self

    def datetime(self, column, nullable=False, now=False):
        """Create datetime column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.datetime(column, nullable, now)
        return self

    def timestamp(self, column, nullable=False, now=False):
        """Create timestamp column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.timestamp(column, nullable, now)
        return self

    def timestamps(self):
        """Create timestamp columns - delegates to ColumnFactory"""
        self._last_column = self.column_factory.timestamps()
        return self

    def decimal(self, column, length=17, precision=6, nullable=False):
        """Create decimal column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.decimal(
            column, length, precision, nullable
        )
        return self

    def float(self, column, length=19, precision=4, nullable=False):
        """Create float column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.float(column, length, precision, nullable)
        return self

    def double(self, column, nullable=False):
        """Create double column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.double(column, nullable)
        return self

    def enum(self, column, options=None, nullable=False):
        """Create enum column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.enum(column, options, nullable)
        return self

    def text(self, column, length=None, nullable=False):
        """Create text column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.text(column, length, nullable)
        return self

    def tiny_text(self, column, length=None, nullable=False):
        """Create tiny text column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.tiny_text(column, length, nullable)
        return self

    def unsigned_decimal(self, column, length=17, precision=6, nullable=False):
        """Create unsigned decimal column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.unsigned_decimal(
            column, length, precision, nullable
        )
        return self

    def long_text(self, column, length=None, nullable=False):
        """Create long text column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.long_text(column, length, nullable)
        return self

    def json(self, column, nullable=False):
        """Create JSON column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.json(column, nullable)
        return self

    def jsonb(self, column, nullable=False):
        """Create JSONB column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.jsonb(column, nullable)
        return self

    def inet(self, column, length=255, nullable=False):
        """Create inet column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.inet(column, length, nullable)
        return self

    def cidr(self, column, length=255, nullable=False):
        """Create cidr column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.cidr(column, length, nullable)
        return self

    def macaddr(self, column, length=255, nullable=False):
        """Create macaddr column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.macaddr(column, length, nullable)
        return self

    def point(self, column, nullable=False):
        """Create point column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.point(column, nullable)
        return self

    def geometry(self, column, nullable=False):
        """Create geometry column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.geometry(column, nullable)
        return self

    def year(self, column, length=4, default=None, nullable=False):
        """Create year column - delegates to ColumnFactory"""
        self._last_column = self.column_factory.year(column, length, default, nullable)
        return self

    def unsigned(self, column=None, length=None, nullable=False):
        """Make column unsigned"""
        if column:
            self._last_column = self.column_factory.create_column(
                column, "integer", length=length, nullable=nullable
            ).unsigned()
        elif self._last_column:
            self._last_column.unsigned()
        return self

    def morphs(self, column, nullable=False, indexes=True):
        """Create polymorphic columns - delegates to ColumnFactory"""
        self._last_column = self.column_factory.morphs(column, nullable, indexes)
        return self

    # === Column Modifiers ===

    def default(self, value, raw=False):
        """Set default value for last column"""
        if self._last_column:
            self._last_column.default(value, raw)
        return self

    def default_raw(self, value):
        """Set raw default value for last column"""
        return self.default(value, raw=True)

    def nullable(self):
        """Make last column(s) nullable"""
        columns = (
            self._last_column
            if isinstance(self._last_column, list)
            else [self._last_column]
        )
        for column in columns:
            column.nullable()
        return self

    def comment(self, comment):
        """Add comment to last column"""
        if self._last_column:
            self._last_column.comment(comment)
        return self

    def after(self, old_column):
        """Position column after another column"""
        if self._last_column:
            self._last_column.after(old_column)
        return self

    # === Constraints - Delegation to ConstraintManager ===

    def unique(self, column=None, name=None):
        """Add unique constraint - delegates to ConstraintManager"""
        columns = column or (self._last_column.name if self._last_column else None)
        if columns:
            self.constraint_manager.add_unique_constraint(columns, name)
        return self

    def index(self, column=None, name=None):
        """Add index - delegates to ConstraintManager"""
        columns = column or (self._last_column.name if self._last_column else None)
        if columns:
            self.constraint_manager.add_index(columns, name)
        return self

    def fulltext(self, column=None, name=None):
        """Add fulltext index - delegates to ConstraintManager"""
        columns = column or (self._last_column.name if self._last_column else None)
        if columns:
            self.constraint_manager.add_fulltext_index(columns, name)
        return self

    def primary(self, column=None, name=None):
        """Add primary key - delegates to ConstraintManager"""
        columns = column or (self._last_column.name if self._last_column else None)
        if columns:
            self.constraint_manager.add_primary_key(columns, name)
        return self

    # === Foreign Keys - Delegation to ForeignKeyBuilder ===

    def add_foreign(self, columns, name=None):
        """Add foreign key using dot notation - delegates to ForeignKeyBuilder"""
        return self.foreign_key_builder.add_foreign(columns, name)

    def foreign(self, column, name=None):
        """Add foreign key constraint - delegates to ForeignKeyBuilder"""
        self._last_foreign = self.constraint_manager.add_foreign_key(column, name=name)
        return self

    def foreign_id(self, column):
        """Create foreign ID column - delegates to ForeignKeyBuilder"""
        return self.foreign_key_builder.foreign_id(column)

    def foreign_uuid(self, column):
        """Create foreign UUID column - delegates to ForeignKeyBuilder"""
        return self.foreign_key_builder.foreign_uuid(column)

    def foreign_id_for(self, model, column=None):
        """Create foreign key for model - delegates to ForeignKeyBuilder"""
        return self.foreign_key_builder.foreign_id_for(model, column)

    def references(self, column):
        """Set referenced column - delegates to ForeignKeyBuilder"""
        if hasattr(self, "_last_foreign") and self._last_foreign:
            self._last_foreign.references(column)
        else:
            self.foreign_key_builder.references(column)
        return self

    def on(self, table):
        """Set referenced table - delegates to ForeignKeyBuilder"""
        if hasattr(self, "_last_foreign") and self._last_foreign:
            self._last_foreign.on(table)
        else:
            self.foreign_key_builder.on(table)
        return self

    def on_delete(self, action):
        """Set on delete action - delegates to ForeignKeyBuilder"""
        if hasattr(self, "_last_foreign") and self._last_foreign:
            self._last_foreign.on_delete(action)
        else:
            self.foreign_key_builder.on_delete(action)
        return self

    def on_update(self, action):
        """Set on update action - delegates to ForeignKeyBuilder"""
        if hasattr(self, "_last_foreign") and self._last_foreign:
            self._last_foreign.on_update(action)
        else:
            self.foreign_key_builder.on_update(action)
        return self

    # === Special Methods ===

    def soft_deletes(self, name="deleted_at"):
        """Add soft delete column"""
        return self.timestamp(name, nullable=True)

    def table_comment(self, comment):
        """Add table comment"""
        self.table.comment = comment
        return self

    def rename(self, old_column, new_column, data_type, length=None):
        """Rename column"""
        self.table.rename_column(old_column, new_column, data_type, length)
        return self

    def drop_column(self, *columns):
        """Drop columns"""
        for column in columns:
            self.table.drop_column(column)
        return self

    def drop_index(self, index):
        """Drop index"""
        if isinstance(index, list):
            index = "_".join(index)
        self.table.drop_index(index)
        return self

    def change(self):
        """Mark table for modification"""
        self.table.change()
        return self

    def drop_unique(self, index):
        """Drop unique constraint"""
        if isinstance(index, list):
            index = "_".join(index)
        self.table.drop_unique(index)
        return self

    def drop_primary(self, index):
        """Drop primary key"""
        if isinstance(index, list):
            index = "_".join(index)
        self.table.drop_primary(index)
        return self

    def drop_foreign(self, index):
        """Drop foreign key"""
        if isinstance(index, list):
            index = "_".join(index)
        self.table.drop_foreign(index)
        return self

    # === SQL Generation ===

    def to_sql(self):
        """Generate SQL for table"""
        if self.action == "create":
            return self.platform.compile_create_sql(self.table)
        elif self.action == "alter":
            return self.platform.compile_alter_sql(self.table)
        elif self.action == "drop":
            return self.platform.compile_drop_table(self.table.name)
        elif self.action == "drop_if_exists":
            return self.platform.compile_drop_table_if_exists(self.table.name)
        elif self.action == "rename":
            return self.platform.compile_rename_table(
                self.table.name, self.table.new_name
            )

        return self.platform.compile_create_sql(self.table)

    # === Context Manager ===

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Store SQL when exiting context manager"""
        if exc_type is None:  # Only store if no exception occurred
            self._generated_sql = self.to_sql()
        # Don't execute - let the migration system handle it

    def get_sql(self):
        """Get generated SQL"""
        return getattr(self, "_generated_sql", self.to_sql())

    def execute(self):
        """This method is deprecated - Blueprint should not execute SQL directly"""
        from cara.support.Logger import Log

        Log.warning("Blueprint.execute() is deprecated - use get_sql() instead")
        return self.to_sql()

    def _execute_sql(self, sql):
        """Legacy method - no longer used"""
        pass
