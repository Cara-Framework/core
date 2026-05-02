import contextlib
from decimal import Decimal

from cara.facades import DB

from .Blueprint import Blueprint
from .SchemaConnectionManager import SchemaConnectionManager
from .SchemaQueryExecutor import SchemaQueryExecutor
from .Table import Table
from .TableDiff import TableDiff


def _release_connection(connection) -> None:
    """Return a borrowed connection to the pool. Best-effort."""
    if connection is None:
        return
    try:
        close = getattr(connection, "close_connection", None)
        if callable(close):
            close()
    except Exception:
        # Cleanup must never mask the real result.
        pass


@contextlib.contextmanager
def _borrow_connection(connection_manager, schema=None):
    """Borrow a pool connection and guarantee its release on exit.

    Without this, every ad-hoc ``connection_manager.create_connection_instance``
    bleeds a pool slot — within ~50 borrows the pool is exhausted and
    every subsequent migration / schema query times out.
    """
    conn = connection_manager.create_connection_instance(schema)
    try:
        yield conn
    finally:
        _release_connection(conn)


class Schema:
    """
    Single Responsibility: Schema definition and management
    Open/Closed: Can be extended with new column types
    Dependency Inversion: Depends on abstractions (DatabaseManager)
    """

    _default_string_length = "255"
    _type_hints_map = {
        "string": str,
        "char": str,
        "big_increments": int,
        "integer": int,
        "tiny_integer": int,
        "small_integer": int,
        "medium_integer": int,
        "big_integer": int,
        "unsigned_integer": int,
        "unsigned_big_integer": int,
        "integer_unsigned": int,
        "big_integer_unsigned": int,
        "tiny_integer_unsigned": int,
        "small_integer_unsigned": int,
        "medium_integer_unsigned": int,
        "increments": int,
        "uuid": str,
        "binary": bytes,
        "boolean": bool,
        "decimal": Decimal,
        "double": float,
        "enum": str,
        "text": str,
        "float": float,
        "geometry": str,
        "json": dict,
        "jsonb": bytes,
        "inet": str,
        "cidr": str,
        "macaddr": str,
        "long_text": str,
        "point": str,
        "time": str,
        "timestamp": str,
        "date": str,
        "year": str,
        "datetime": str,
        "tiny_increments": int,
        "unsigned": int,
    }

    def __init__(
        self,
        dry=False,
        connection=None,
        connection_class=None,
        platform=None,
        grammar=None,
        connection_details=None,
        schema=None,
    ):
        """Initialize Schema with dependency injection - follows Dependency Inversion Principle"""
        self._dry = dry
        self.grammar = grammar
        self.connection_details = connection_details or {}
        self._blueprint = None
        self.schema = schema

        # Initialize components - Composition over inheritance
        self.connection_manager = SchemaConnectionManager(DB)
        self.query_executor = SchemaQueryExecutor(self.connection_manager, dry)

        # Set up connection if provided
        if connection or connection_class or platform:
            # Legacy support - use provided values
            self.connection_manager.connection = connection
            self.connection_manager.connection_class = connection_class
            self.connection_manager.platform = platform or DB.get_platform(connection)
        else:
            # Use default connection
            self.on(connection)

    def on(self, connection_key):
        """
        Change the connection - delegates to connection manager

        Arguments:
            connection {string} -- A connection string like 'mysql' or 'postgres'.

        Returns:
            self
        """
        self.connection_manager.resolve_connection(connection_key)
        return self

    def dry(self):
        """Enable dry run mode - Single responsibility"""
        self._dry = True
        self.query_executor.dry = True
        return self

    # === Blueprint Factory Methods - Factory Pattern ===

    def create(self, table):
        """Create table blueprint - Factory pattern"""
        blueprint = self._create_blueprint(table, Table(table), "create")
        return BlueprintExecutor(blueprint, self)

    def create_table_if_not_exists(self, table):
        """Create table if not exists blueprint - Factory pattern"""
        blueprint = self._create_blueprint(
            table, Table(table), "create_table_if_not_exists"
        )
        return BlueprintExecutor(blueprint, self)

    def table(self, table):
        """Alter table blueprint - Factory pattern"""
        blueprint = self._create_blueprint(table, TableDiff(table), "alter")
        return BlueprintExecutor(blueprint, self)

    def _create_blueprint(self, table_name, table_obj, action):
        """DRY - Common blueprint creation logic.

        We deliberately do NOT pass a real connection to Blueprint here.
        Blueprint only compiles SQL (it stores the connection but never
        executes through it — the BlueprintExecutor's __exit__ runs the
        compiled statements via ``query_executor.execute_query``, which
        borrows + releases its own connection per statement). Passing a
        connection here used to silently leak one pool slot per
        ``with self.schema.create(...)`` block, exhausting the pool
        within ~50 migrations. Pass ``None``.
        """
        self._table = table_name

        # Get grammar from DatabaseManager if not set
        grammar = self.grammar or DB.get_grammar(self.connection_manager.connection)

        self._blueprint = Blueprint(
            grammar,
            connection=None,
            table=table_obj,
            action=action,
            platform=self.connection_manager.platform,
            schema=self.schema,
            default_string_length=self._default_string_length,
            dry=self._dry,
        )

        return self._blueprint

    # === Query Methods - Delegation to Query Executor ===

    def has_column(self, table, column, query_only=False):
        """Check if table has column - delegates to query executor"""
        sql = self.connection_manager.platform.compile_column_exists(table, column)
        return self.query_executor.execute_query(sql)

    def drop_table(self, table, query_only=False):
        """Drop table - delegates to query executor"""
        sql = self.connection_manager.platform.compile_drop_table(table)
        return self.query_executor.execute_query(sql)

    def drop(self, *args, **kwargs):
        """Alias for drop_table - Interface segregation"""
        return self.drop_table(*args, **kwargs)

    def drop_table_if_exists(self, table, exists=False, query_only=False):
        """Drop table if exists - delegates to query executor"""
        sql = self.connection_manager.platform.compile_drop_table_if_exists(table)
        return self.query_executor.execute_query(sql)

    def rename(self, table, new_name):
        """Rename table - delegates to query executor"""
        sql = self.connection_manager.platform.compile_rename_table(table, new_name)
        return self.query_executor.execute_query(sql)

    def truncate(self, table, foreign_keys=False):
        """Truncate table - delegates to query executor"""
        sql = self.connection_manager.platform.compile_truncate(
            table, foreign_keys=foreign_keys
        )
        return self.query_executor.execute_query(sql)

    def has_table(self, table, query_only=False):
        """Check if table exists - delegates to query executor"""
        connection_info = self.connection_manager.get_connection_info()
        sql = self.connection_manager.platform.compile_table_exists(
            table,
            database=connection_info.get("database"),
            schema=self.get_schema(),
        )
        return self.query_executor.execute_query(sql)

    def enable_foreign_key_constraints(self):
        """Enable foreign key constraints - delegates to query executor"""
        sql = self.connection_manager.platform.enable_foreign_key_constraints()
        return self.query_executor.execute_query(sql)

    def disable_foreign_key_constraints(self):
        """Disable foreign key constraints - delegates to query executor"""
        sql = self.connection_manager.platform.disable_foreign_key_constraints()
        return self.query_executor.execute_query(sql)

    def raw(self, sql, bindings=()):
        """Execute raw SQL - escape hatch used by migrations for DDL the
        Blueprint DSL does not cover (e.g. ``CREATE EXTENSION``, trigram/GIN
        indexes, custom ``ALTER TABLE`` constraints).
        """
        return self.query_executor.execute_query(sql, bindings)

    def statement(self, sql, bindings=()):
        """Alias for raw — mirrors the Laravel ``DB::statement`` naming so
        migrations reading Laravel docs feel natural."""
        return self.query_executor.execute_query(sql, bindings)

    # === Postgres-specific index helpers ==========================================

    def gin_index(self, table, column, opclass=None, name=None, if_not_exists=True):
        """Create a Postgres GIN index.

        Arguments:
            table    {str}        -- Table to index.
            column   {str|list}   -- Column name, "col opclass" expression string,
                                     or list of column expressions for composite GIN.
            opclass  {str|None}   -- Operator class (e.g. "gin_trgm_ops", "jsonb_path_ops").
                                     Applied to all columns when given.
            name     {str|None}   -- Index name. Defaults to ``idx_{table}_{col}_gin``.
            if_not_exists {bool}  -- Emit IF NOT EXISTS for idempotent migrations.

        Examples:
            schema.gin_index("brand", "name", opclass="gin_trgm_ops")
            schema.gin_index("product", "search_vector")
            schema.gin_index("brand", "aliases", opclass="jsonb_path_ops")
        """
        return self._create_using_index("GIN", table, column, opclass, name, if_not_exists)

    def gist_index(self, table, column, opclass=None, name=None, if_not_exists=True):
        """Create a Postgres GiST index. Same signature as gin_index."""
        return self._create_using_index("GIST", table, column, opclass, name, if_not_exists)

    def _create_using_index(self, method, table, column, opclass, name, if_not_exists):
        columns = column if isinstance(column, (list, tuple)) else [column]

        def _col_expr(c):
            # If caller already embedded an opclass (e.g. "name gin_trgm_ops"), trust it.
            if " " in c.strip():
                return c
            return f"{c} {opclass}" if opclass else c

        col_sql = ", ".join(_col_expr(c) for c in columns)

        first_col = columns[0].strip().split()[0]
        default_name = f"idx_{table}_{first_col}_{method.lower()}"
        index_name = name or default_name

        ine = "IF NOT EXISTS " if if_not_exists else ""
        sql = f"CREATE INDEX {ine}{index_name} ON {table} USING {method} ({col_sql})"
        return self.query_executor.execute_query(sql)

    # === Information Methods - Single Responsibility ===

    def get_connection_information(self):
        """Get connection info - delegates to connection manager"""
        return self.connection_manager.get_connection_info()

    def new_connection(self):
        """Create new connection - delegates to connection manager"""
        if self._dry:
            return None
        return self.connection_manager.create_connection_instance(self.schema)

    def get_schema(self):
        """Get schema name - Single responsibility"""
        return self.schema or self.get_connection_information().get(
            "full_details", {}
        ).get("schema")

    def get_columns(self, table, dict=True):
        """Get table columns - delegates to platform.

        Borrows a connection via ``_borrow_connection`` so the platform
        introspection call doesn't strand a pool slot. The platform
        helper reads from the connection synchronously inside the with
        block, so it's safe to release immediately on exit.
        """
        with _borrow_connection(self.connection_manager, self.schema) as conn:
            table_schema = self.connection_manager.platform.get_current_schema(
                conn,
                table,
                schema=self.get_schema(),
            )

        if dict:
            result = {}
            for column in table_schema.get_added_columns().items():
                result.update({column[0]: column[1]})
            return result
        else:
            return table_schema.get_added_columns().items()

    def get_all_tables(self):
        """Get all tables in database - delegates to query executor"""
        connection_info = self.connection_manager.get_connection_info()
        sql = self.connection_manager.platform.compile_get_all_tables(
            database=connection_info.get("database"),
            schema=self.get_schema(),
        )

        result = self.query_executor.get_query_result(sql)
        return list(map(lambda t: list(t.values())[0], result)) if result else []  # noqa: safe — platform SQL always returns single-column rows

    # === Class Methods - Configuration ===

    @classmethod
    def set_default_string_length(cls, length):
        """Set default string length - Configuration"""
        cls._default_string_length = length
        return cls

    @staticmethod
    def build(field_builder_func):
        """
        Build schema using lambda function syntax.
        Example: Schema.build(lambda field: (
            field.string("name"),
            field.text("description").nullable()
        ))
        """
        # Create a field builder instance
        field_builder = FieldBuilder()

        # Call the lambda function to get field definitions
        field_definitions = field_builder_func(field_builder)

        # Convert to the expected format for ModelDiscoverer
        return field_definitions


class BlueprintExecutor:
    """Wrapper that executes Blueprint SQL after context manager exits"""

    def __init__(self, blueprint, schema):
        self.blueprint = blueprint
        self.schema = schema

    def __enter__(self):
        return self.blueprint.__enter__()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # First let blueprint store its SQL
        result = self.blueprint.__exit__(exc_type, exc_value, exc_traceback)

        # If no exception, execute the SQL using schema's query executor
        if exc_type is None:
            sql_statements = self.blueprint.get_sql()
            if isinstance(sql_statements, list):
                for sql in sql_statements:
                    if sql and sql.strip():
                        self.schema.query_executor.execute_query(sql.strip())
            elif sql_statements and sql_statements.strip():
                self.schema.query_executor.execute_query(sql_statements.strip())

        return result


class FieldBuilder:
    """Field builder for new Schema.build syntax."""

    def string(self, name, length=255):
        return FieldDefinition("string", name, length=length)

    def text(self, name):
        return FieldDefinition("text", name)

    def integer(self, name):
        return FieldDefinition("integer", name)

    def tiny_integer(self, name):
        return FieldDefinition("tiny_integer", name)

    def small_integer(self, name):
        return FieldDefinition("small_integer", name)

    def medium_integer(self, name):
        return FieldDefinition("medium_integer", name)

    def big_integer(self, name):
        return FieldDefinition("big_integer", name)

    def unsigned_integer(self, name):
        return FieldDefinition("unsigned_integer", name)

    def unsigned_big_integer(self, name):
        return FieldDefinition("unsigned_big_integer", name)

    def decimal(self, name, precision=10, scale=2):
        return FieldDefinition("decimal", name, precision=precision, scale=scale)

    def boolean(self, name):
        return FieldDefinition("boolean", name)

    def enum(self, name, options):
        return FieldDefinition("enum", name, options=options)

    def uuid(self, name):
        return FieldDefinition("uuid", name)

    def json(self, name):
        return FieldDefinition("json", name)

    def jsonb(self, name):
        # Postgres-native binary JSON. Several models call
        # ``field.jsonb("metadata")``; without this method the call
        # raised ``AttributeError`` inside ``Schema.build``,
        # ``MakeMigrationCommand`` swallowed it as a generic ValueError,
        # and the column quietly disappeared from every generated
        # migration (every ``metadata`` JSONB field across all tables).
        return FieldDefinition("jsonb", name)

    def timestamp(self, name):
        return FieldDefinition("timestamp", name)

    def datetime(self, name):
        return FieldDefinition("datetime", name)

    def date(self, name):
        return FieldDefinition("date", name)

    def time(self, name):
        return FieldDefinition("time", name)

    def float(self, name):
        return FieldDefinition("float", name)

    def binary(self, name):
        return FieldDefinition("binary", name)

    def char(self, name, length=255):
        return FieldDefinition("char", name, length=length)

    def increments(self, name):
        return FieldDefinition("increments", name)

    def big_increments(self, name):
        return FieldDefinition("big_increments", name)

    def timestamps(self):
        """Create timestamps fields (created_at, updated_at)."""
        return FieldDefinition("timestamps", None)

    def soft_deletes(self):
        """Create soft delete field (deleted_at)."""
        return FieldDefinition("soft_deletes", None)

    def foreign(self, field_name):
        """Create a standalone foreign key definition."""
        fk_definition = FieldDefinition("foreign_key", None)
        fk_definition._is_foreign = True
        fk_definition._foreign_key_config = {
            "field": field_name,
            "references": None,
            "on": None,
            "on_delete": None,
            "on_update": None,
        }
        return fk_definition


class FieldDefinition:
    """Represents a field definition in the new syntax."""

    def __init__(self, field_type, name, **kwargs):
        self.field_type = field_type
        self.name = name
        self.params = kwargs
        self._nullable = False
        self._default = None
        self._unique = False
        # Foreign key properties
        self._is_foreign = False
        self._foreign_key_config = {}

    def nullable(self):
        self._nullable = True
        return self

    def default(self, value):
        self._default = value
        return self

    def unique(self):
        """Mark this field as unique."""
        self._unique = True
        return self

    def foreign(self):
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

    def references(self, column):
        """Set the referenced column for foreign key."""
        if self._is_foreign:
            self._foreign_key_config["references"] = column
        return self

    def on(self, table):
        """Set the referenced table for foreign key."""
        if self._is_foreign:
            self._foreign_key_config["on"] = table
        return self

    def on_delete(self, action):
        """Set the ON DELETE action for foreign key."""
        if self._is_foreign:
            self._foreign_key_config["on_delete"] = action
        return self

    def on_update(self, action):
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
        if self._unique:
            params["unique"] = True

        result = {"type": self.field_type, "params": params}

        # Add foreign key information if this is a foreign key
        if self._is_foreign:
            result["foreign_key"] = self._foreign_key_config

        return result
