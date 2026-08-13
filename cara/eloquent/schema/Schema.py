from __future__ import annotations

import contextlib
import inspect
from decimal import Decimal
from typing import Self

from cara.facades import DB

from .Blueprint import Blueprint
from .BlueprintExecutor import BlueprintExecutor
from .FieldBuilder import FieldBuilder
from .SchemaConnectionManager import SchemaConnectionManager
from .SchemaQueryExecutor import SchemaQueryExecutor
from .Table import Table
from .TableDiff import TableDiff


def _release_connection(connection) -> None:
    """Return an owned connection, never the active transaction handle."""
    if connection is None:
        return
    transaction_level = getattr(connection, "transaction_level", 0)
    if isinstance(transaction_level, (int, float)) and transaction_level > 0:
        return
    try:
        close = getattr(connection, "close_connection", None)
        if callable(close):
            close()
    except OSError, RuntimeError, AttributeError:
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
        grammar=None,
        schema=None,
    ):
        """Initialize Schema with dependency injection - follows Dependency Inversion Principle"""
        self._dry = dry
        self.grammar = grammar
        self._blueprint = None
        self.schema = schema

        # Initialize components - Composition over inheritance
        self.connection_manager = SchemaConnectionManager(DB)
        self.query_executor = SchemaQueryExecutor(self.connection_manager, dry)

        self.on(connection)

    def on(self, connection_key) -> Self:
        """
        Change the connection - delegates to connection manager

        Arguments:
            connection {string} -- A configured connection name.

        Returns:
            self
        """
        self.connection_manager.resolve_connection(connection_key)
        return self

    def dry(self) -> Self:
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

    def drop(self, table, query_only=False):
        """Drop a table through the query executor."""
        sql = self.connection_manager.platform.compile_drop_table(table)
        return self.query_executor.execute_query(sql)

    def drop_if_exists(self, table, exists=False, query_only=False):
        """Drop a table when it exists through the query executor."""
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
            schema.gin_index("author", "name", opclass="gin_trgm_ops")
            schema.gin_index("article", "search_vector")
            schema.gin_index("author", "aliases", opclass="jsonb_path_ops")
        """
        return self._create_using_index(
            "GIN", table, column, opclass, name, if_not_exists
        )

    def gist_index(self, table, column, opclass=None, name=None, if_not_exists=True):
        """Create a Postgres GiST index. Same signature as gin_index."""
        return self._create_using_index(
            "GIST", table, column, opclass, name, if_not_exists
        )

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
        return list(map(lambda t: list(t.values())[0], result)) if result else []

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

        # Downstream model schemas are keyed by column name, so a duplicate
        # would silently overwrite the earlier declaration. Expanded helpers
        # pay the same invariant as explicit columns.
        expanded_names: list[str] = []
        for definition in field_definitions:
            if definition.field_type == "timestamps":
                expanded_names.extend(("created_at", "updated_at"))
            elif definition.field_type == "soft_deletes":
                expanded_names.append("deleted_at")
            elif definition.name is not None:
                expanded_names.append(str(definition.name))
        duplicates = sorted(
            {name for name in expanded_names if expanded_names.count(name) > 1}
        )
        if duplicates:
            raise ValueError(
                "Schema declares duplicate column(s): " + ", ".join(duplicates)
            )

        # Convert to the expected format for ModelDiscoverer
        return field_definitions


# --- The model field-type vocabulary, derived from the builder above --------
#
# ``FieldBuilder`` IS the vocabulary: a model can declare exactly what
# ``Schema.build(lambda field: ...)`` offers, no more. Every consumer that
# needs to know the legal types — most importantly the migration AST parser —
# reads these sets instead of restating the list.
#
# Restating it is not theoretical debt. The hand-copied list in
# ``ModelDiscoverer`` was missing ``jsonb`` and erased the ``metadata`` column
# from ~10 tables; it was still missing ``char`` and ``binary``, so those
# columns never reached a generated migration and ``schema:check`` then
# accused the model of failing to declare a column it plainly declares. A copy
# has no way to learn that the builder grew a method. Deriving the sets here
# means adding a builder method is the only step there is.

#: Constraint declarations, not column types. ``field.unique([...])`` /
#: ``field.index([...])`` are collected by the parser's separate composite
#: path, so they must not be mistaken for a field type.
CONSTRAINT_BUILDERS = frozenset({"unique", "index"})

#: Builders whose first positional argument is not a column name.
_UNNAMED_FIELD_BUILDERS = frozenset({"timestamps", "soft_deletes", "foreign"})

#: ``field.foreign(...)`` yields a definition typed ``"foreign_key"``. There is
#: no builder method by that name, so the alias is declared rather than derived.
_INTERNAL_TYPE_ALIASES = frozenset({"foreign_key"})

_FIELD_BUILDERS = (
    frozenset(name for name in vars(FieldBuilder) if not name.startswith("_"))
    - CONSTRAINT_BUILDERS
)

#: Field types declared as ``field.<type>("column_name", ...)``.
FIELD_TYPES_WITH_NAMES = _FIELD_BUILDERS - _UNNAMED_FIELD_BUILDERS

#: Field types that carry no column name of their own.
FIELD_TYPES_WITHOUT_NAMES = _UNNAMED_FIELD_BUILDERS | _INTERNAL_TYPE_ALIASES


def _derive_builder_parameters() -> tuple[
    dict[str, tuple[str, ...]], dict[str, dict[str, object]]
]:
    """Read each builder's extra parameters and defaults off its signature.

    The same restatement problem as the type list, one level down. The
    migration AST parser hand-coded which POSITIONAL index meant what per
    type (``decimal`` args 1/2 are precision/scale, ``string`` arg 1 is
    length) and never looked at keywords at all, so
    ``field.decimal("price", precision=12, scale=4)`` parsed to an EMPTY
    param dict; the emitter then filled in its own restated ``10, 2`` and
    wrote a money column four digits short of what the model declares. The
    builder's signature already states both the order and the defaults, so
    nobody has to say it a second time.
    """
    parameters: dict[str, tuple[str, ...]] = {}
    defaults: dict[str, dict[str, object]] = {}
    for field_type in _FIELD_BUILDERS:
        signature = inspect.signature(getattr(FieldBuilder, field_type))
        names = [name for name in signature.parameters if name != "self"]
        if field_type in FIELD_TYPES_WITH_NAMES:
            # The leading column name is not a field parameter — it names the
            # column the parser keys the definition under.
            names = names[1:]
        parameters[field_type] = tuple(names)
        defaults[field_type] = {
            name: signature.parameters[name].default
            for name in names
            if signature.parameters[name].default is not inspect.Parameter.empty
        }
    return parameters, defaults


#: ``<field type>`` -> its extra parameter names in positional order.
#: ``<field type>`` -> ``{parameter: default}`` for the ones that have one.
FIELD_TYPE_PARAMETERS, FIELD_TYPE_DEFAULTS = _derive_builder_parameters()
