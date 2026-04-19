import inspect
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from cara.eloquent.expressions import (
    AggregateExpression,
    BetweenExpression,
    FromTable,
    GroupByExpression,
    HavingExpression,
    JoinClause,
    OrderByExpression,
    QueryExpression,
    SelectExpression,
    SubGroupExpression,
    SubSelectExpression,
    UpdateQueryExpression,
)
from cara.exceptions import (
    HTTP404Exception,
    InvalidArgumentException,
    ModelNotFoundException,
    MultipleRecordsFoundException,
)
from cara.support.Collection import Collection

from ..observers import ObservesEvents
from ..pagination import LengthAwarePaginator, SimplePaginator
from ..schema import Schema
from ..scopes import BaseScope
from .EagerRelation import EagerRelations



class TransactionContext:
    """Context manager for database transactions."""
    
    def __init__(self, builder):
        self.builder = builder
    
    def __enter__(self):
        self.builder.begin()
        return self.builder
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.builder.rollback()
            return False
        else:
            self.builder.commit()
            return True


class QueryBuilder(ObservesEvents):
    """
    Single Responsibility: Builds and executes database queries
    Open/Closed: Can be extended with new query types and methods
    Dependency Inversion: Depends on abstractions (DatabaseManager, Grammar)
    """

    def __init__(
        self,
        grammar=None,
        connection=None,
        connection_class=None,
        table=None,
        connection_details=None,
        connection_driver=None,
        model=None,
        scopes=None,
        schema=None,
        dry=False,
        config_path=None,
    ):
        """
        QueryBuilder initializer.

        Arguments:
            grammar {eloquent.grammar.Grammar} -- A grammar class.

        Keyword Arguments:
            connection {eloquent.connection.Connection} -- A connection class (default: {None})
            table {str} -- the name of the table (default: {""})
        """
        self.config_path = config_path
        self.grammar = grammar
        self.table(table)
        self.dry = dry
        self._creates_related = {}
        self.connection = connection
        self.connection_class = connection_class
        self._connection = None
        self._connection_details = connection_details or {}
        self._connection_driver = connection_driver
        self._scopes = scopes or {}
        self.lock = False
        self._schema = schema
        self._eager_relation = EagerRelations()
        if model:
            self._global_scopes = model._global_scopes
            if model.__with__:
                self.with_(model.__with__)
        else:
            self._global_scopes = {}

        self.builder = self

        self._columns = ()
        self._creates = {}

        self._sql = ""
        self._bindings = ()

        self._updates = ()

        self._wheres = ()
        self._order_by = ()
        self._group_by = ()
        self._joins = ()
        self._having = ()
        self._macros = {}

        self._aggregates = ()

        self._limit = False
        self._offset = False
        self._distinct = False
        self._model = model
        self.set_action("select")

        # Get DatabaseManager instance for all config/logic needs
        from ..DatabaseManager import get_database_manager

        self._db_manager = get_database_manager()

        if not self._connection_details:
            self._connection_details = self._db_manager.get_connection_details()

        self.on(connection)

        if grammar:
            self.grammar = grammar

        if connection_class:
            self.connection_class = connection_class

    def _set_creates_related(self, fields: dict):
        self._creates_related = fields
        return self

    def set_schema(self, schema):
        self._schema = schema
        return self

    def shared_lock(self):
        return self.make_lock("share")

    def lock_for_update(self):
        return self.make_lock("update")

    def make_lock(self, lock):
        self.lock = lock
        return self

    def reset(self):
        """Resets the query builder instance so you can make multiple calls with the same builder
        instance."""

        self.set_action("select")

        self._updates = ()

        self._wheres = ()
        self._order_by = ()
        self._group_by = ()
        self._joins = ()
        self._having = ()
        self._aggregates = ()

        return self

    def get_connection_information(self):
        """Get connection info from DatabaseManager"""
        return self._db_manager.get_connection_info(self.connection)

    def table(self, table, raw=False):
        """
        Sets a table on the query builder.

        Arguments:
            table {string} -- The name of the table

        Returns:
            self
        """
        if table:
            self._table = FromTable(table, raw=raw)
        else:
            self._table = table
        return self

    def from_(self, table):
        """
        Alias for the table method.

        Arguments:
            table {string} -- The name of the table

        Returns:
            self
        """
        return self.table(table)

    def from_raw(self, table):
        """
        Alias for the table method.

        Arguments:
            table {string} -- The name of the table

        Returns:
            self
        """
        return self.table(table, raw=True)

    def table_raw(self, query):
        """
        Sets a query on the query builder.

        Arguments:
            query {string} -- The query to use for the table

        Returns:
            self
        """
        return self.from_raw(query)

    def get_table_name(self):
        """Get the name of the table for this query."""
        return self._table.name

    # NOTE: ``get_connection`` is defined later in this class and returns
    # ``self._connection`` (the resolved connection instance). The earlier
    # definition that returned ``self.connection_class`` was dead code and
    # has been removed to avoid confusion.

    def begin(self):
        """Begin a new database transaction."""
        self._connection = self.new_connection()
        self._connection.begin()
        return self._connection

    def begin_transaction(self, *args, **kwargs):
        """Alias for begin()."""
        return self.begin(*args, **kwargs)

    def get_schema_builder(self):
        """Get a schema builder instance for the current connection."""
        return Schema(
            connection=self.connection_class,
            grammar=self.grammar,
        )

    def commit(self):
        """Commit the active database transaction."""
        if not hasattr(self, '_connection') or self._connection is None:
            raise RuntimeError("No active transaction to commit.")
        return self._connection.commit()

    def rollback(self):
        """Roll back the active database transaction."""
        if not hasattr(self, '_connection') or self._connection is None:
            raise RuntimeError("No active transaction to roll back.")
        self._connection.rollback()
        return self

    def transaction(self, callback=None):
        """Execute code within a database transaction.

        Can be used as a context manager or with a callback.

        Example (context manager):
            with Product.query().transaction() as trx:
                product = Product.create({...})
                ProductImage.create({...})

        Example (callback):
            Product.query().transaction(lambda: [
                Product.create({...}),
                ProductImage.create({...}),
            ])
        """
        if callback is None:
            return TransactionContext(self)

        self.begin()
        try:
            result = callback()
            self.commit()
            return result
        except Exception:
            self.rollback()
            raise

    # NOTE: ``get_relation`` is defined later in this class with a more
    # general signature (accepting an optional builder argument). Python
    # silently shadows methods, so the version formerly here was dead code.

    def set_scope(self, name, callable):
        """
        Sets a scope based on a class and maps it to a name.

        Arguments:
            cls {eloquent.Model} -- An ORM model class.
            name {string} -- The name of the scope to use.

        Returns:
            self
        """
        # setattr(self, name, callable)
        self._scopes.update({name: callable})

        return self

    def set_global_scope(self, name="", callable=None, action="select"):
        """
        Sets the global scopes that should be used before creating the SQL.

        Arguments:
            cls {eloquent.Model} -- An ORM model class.
            name {string} -- The name of the global scope.

        Returns:
            self
        """
        if isinstance(name, BaseScope):
            name.on_boot(self)
            return self

        if action not in self._global_scopes:
            self._global_scopes[action] = {}

        self._global_scopes[action].update({name: callable})

        return self

    def without_global_scopes(self):
        self._global_scopes = {}
        return self

    def remove_global_scope(self, scope, action=None):
        """
        Sets the global scopes that should be used before creating the SQL.

        Arguments:
            cls {eloquent.Model} -- An ORM model class.
            name {string} -- The name of the global scope.

        Returns:
            self
        """
        if isinstance(scope, BaseScope):
            scope.on_remove(self)
            return self

        scopes = self._global_scopes.get(action)
        if scopes and scope in scopes:
            del scopes[scope]

        return self

    def __getattr__(self, attribute):
        """
        Magic method for fetching query scopes.

        This method is only used when a method or attribute does not already exist.

        Arguments:
            attribute {string} -- The attribute to fetch.

        Raises:
            AttributeError: Raised when there is no attribute or scope on the builder class.

        Returns:
            self
        """
        if attribute == "__setstate__":
            raise AttributeError(
                "'QueryBuilder' object has no attribute '{}'".format(attribute)
            )

        if attribute in self._scopes:

            def method(*args, **kwargs):
                return self._scopes[attribute](self._model, self, *args, **kwargs)

            return method

        if attribute in self._macros:

            def method(*args, **kwargs):
                return self._macros[attribute](self._model, self, *args, **kwargs)

            return method

        raise AttributeError(
            "'QueryBuilder' object has no attribute '{}'".format(attribute)
        )

    def on(self, connection):
        """Use DatabaseManager for connection resolution"""
        # If connection is an object, use default connection name instead of object's name
        if hasattr(connection, "name") and hasattr(connection, "make_connection"):
            # This is a connection instance, use default connection
            connection_name = self._db_manager.get_default_connection()
        elif connection == "default":
            connection_name = self._db_manager.get_default_connection()
        else:
            connection_name = connection or self._db_manager.get_default_connection()

        self.connection = connection_name

        # Validate connection exists
        if self.connection:
            self._db_manager.validate_connection(self.connection)

            # Get connection class and grammar from DatabaseManager
            self.connection_class = self._db_manager.get_connection_class(
                self.connection
            )
            self.grammar = self._db_manager.get_grammar(self.connection)

        return self

    def select(self, *args):
        """
        Specifies columns that should be selected.

        Returns:
            self
        """
        for arg in args:
            if isinstance(arg, list):
                for column in arg:
                    self._columns += (SelectExpression(column),)
            else:
                for column in arg.split(","):
                    self._columns += (SelectExpression(column),)

        return self

    def distinct(self, boolean=True):
        """
        Specifies that all columns should be distinct.

        Returns:
            self
        """
        self._distinct = boolean
        return self

    def add_select(self, alias, callable):
        """
        Specifies columns that should be selected.

        Returns:
            self
        """
        builder = callable(self.new())
        self._columns += (SubGroupExpression(builder, alias=alias),)

        return self

    def statement(self, query, bindings=None):
        if bindings is None:
            bindings = []
        result = self.new_connection().query(query, bindings)
        return self.prepare_result(result)

    def select_raw(self, query):
        """
        Specifies raw SQL that should be injected into the select expression.

        Returns:
            self
        """
        self._columns += (SelectExpression(query, raw=True),)
        return self

    def get_processor(self):
        return self.connection_class.get_default_post_processor()()

    def bulk_create(
        self,
        creates: List[Dict[str, Any]],
        query: bool = False,
        cast: bool = True,
    ):
        self.set_action("bulk_create")
        model = None

        if self._model:
            model = self._model

        self._creates = []
        for unsorted_create in creates:
            if model:
                unsorted_create = model.filter_mass_assignment(unsorted_create)
            if cast and model:
                unsorted_create = model.cast_values(unsorted_create)
            self._creates.append(dict(sorted(unsorted_create.items())))

        if query:
            return self

        if model:
            model = model.hydrate(self._creates)
        if not self.dry:
            connection = self.new_connection()
            query_result = connection.query(self.to_qmark(), self._bindings, results=1)

            processed_results = query_result or self._creates
        else:
            processed_results = self._creates

        if model:
            return model

        return processed_results

    def create(
        self,
        creates: Optional[Dict[str, Any]] = None,
        query: bool = False,
        id_key: str = "id",
        cast: bool = True,
        ignore_mass_assignment: bool = False,
        **kwargs,
    ):
        """
        Specifies a dictionary that should be used to create new values.

        Arguments:
            creates {dict} -- A dictionary of columns and values.

        Returns:
            self
        """
        self.set_action("insert")
        model = None
        self._creates = creates if creates else kwargs

        if self._model:
            model = self._model
            # Update values with related record's
            self._creates.update(self._creates_related)
            # Filter __fillable/__guarded__ fields
            if not ignore_mass_assignment:
                self._creates = model.filter_mass_assignment(self._creates)
            # Cast values if necessary
            if cast:
                self._creates = model.cast_values(self._creates)

        if query:
            return self

        if model:
            model = model.hydrate(self._creates)
            self.observe_events(model, "creating")

            # if attributes were modified during model observer then we need to update the creates here
            self._creates.update(model.get_dirty_attributes())

        if not self.dry:
            connection = self.new_connection()

            query_result = connection.query(self.to_qmark(), self._bindings, results=1)

            if model:
                id_key = model.get_primary_key()

            processed_results = self.get_processor().process_insert_get_id(
                self,
                query_result or self._creates,
                id_key,
            )
        else:
            processed_results = self._creates

        if model:
            model = model.fill(processed_results)
            self.observe_events(model, "created")
            return model

        return processed_results

    def hydrate(self, result, relations=None):
        return self._model.hydrate(result, relations)

    def delete(self, column=None, value=None, query=False):
        """
        Specify the column and value to delete or deletes everything based on a previously used
        where expression.

        Keyword Arguments:
            column {string} -- The name of the column (default: {None})
            value {string|int} -- The value of the column (default: {None})

        Returns:
            self
        """
        model = None
        self.set_action("delete")

        if self._model:
            model = self._model

        if column and value:
            if isinstance(value, (list, tuple)):
                self.where_in(column, value)
            else:
                self.where(column, value)

        if query:
            return self

        if model and model.is_loaded():
            self.where(
                model.get_primary_key(),
                model.get_primary_key_value(),
            )
            self.observe_events(model, "deleting")

        result = self.new_connection().query(self.to_qmark(), self._bindings)

        if model:
            self.observe_events(model, "deleted")

        return result

    def where(self, column, *args):
        """
        Specifies a where expression.

        Arguments:
            column {string} -- The name of the column to search

        Keyword Arguments:
            args {List} -- The operator and the value of the column to search. (default: {None})

        Returns:
            self
        """
        operator, value = self._extract_operator_value(*args)

        if inspect.isfunction(column):
            builder = column(self.new())
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        operator,
                        SubGroupExpression(builder),
                    )
                ),
            )
        elif isinstance(column, dict):
            for key, value in column.items():
                self._wheres += ((QueryExpression(key, "=", value, "value")),)
        elif isinstance(value, QueryBuilder):
            self._wheres += (
                (
                    QueryExpression(
                        column,
                        operator,
                        SubSelectExpression(value),
                    )
                ),
            )
        else:
            self._wheres += ((QueryExpression(column, operator, value, "value")),)
        return self

    def where_from_builder(self, builder):
        """
        Specifies a where expression.

        Arguments:
            column {string} -- The name of the column to search

        Keyword Arguments:
            args {List} -- The operator and the value of the column to search. (default: {None})

        Returns:
            self
        """

        self._wheres += ((QueryExpression(None, "=", SubGroupExpression(builder))),)

        return self

    def where_like(self, column, value):
        """
        Specifies a where expression.

        Arguments:
            column {string} -- The name of the column to search

        Keyword Arguments:
            args {List} -- The operator and the value of the column to search. (default: {None})

        Returns:
            self
        """
        return self.where(column, "like", value)

    def where_not_like(self, column, value):
        """
        Specifies a where expression.

        Arguments:
            column {string} -- The name of the column to search

        Keyword Arguments:
            args {List} -- The operator and the value of the column to search. (default: {None})

        Returns:
            self
        """
        return self.where(column, "not like", value)

    def where_raw(self, query: str, bindings=()):
        """
        Specifies raw SQL that should be injected into the where expression.

        Arguments:
            query {string} -- The raw query string.

        Keyword Arguments:
            bindings {tuple} -- query bindings that should be added to the connection. (default: {()})

        Returns:
            self
        """
        self._wheres += (
            (
                QueryExpression(
                    query,
                    "=",
                    None,
                    "value",
                    raw=True,
                    bindings=bindings,
                )
            ),
        )
        return self

    def or_where_raw(self, query: str, bindings=()):
        """
        Specifies raw SQL that should be injected into the where expression, OR-joined.

        Arguments:
            query {string} -- The raw query string.

        Keyword Arguments:
            bindings {tuple} -- query bindings that should be added to the connection. (default: {()})

        Returns:
            self
        """
        self._wheres += (
            (
                QueryExpression(
                    query,
                    "=",
                    None,
                    "value",
                    raw=True,
                    bindings=bindings,
                    keyword="or",
                )
            ),
        )
        return self

    # ===== JSON / JSONB helpers (Laravel-style) ======================================
    #
    # These are thin wrappers around where_raw that build Postgres jsonb operator SQL
    # for the caller. Column names pass through unquoted (mirrors where_raw convention,
    # since callers often use qualified names like "b.aliases" or "product.metadata").
    # Path segments are single-quote escaped to prevent injection.

    @staticmethod
    def _escape_json_path_segment(segment: str) -> str:
        """Escape a JSON path segment for safe embedding inside single quotes."""
        if not isinstance(segment, str):
            segment = str(segment)
        return segment.replace("'", "''")

    @staticmethod
    def _json_path_sql(column: str, path) -> str:
        """Build a Postgres jsonb path expression ending with ->> (text extract).

        ``path`` may be a dotted string ("a.b.c"), a list, or None/empty for a direct
        column reference. All segments except the last use -> (keep jsonb), last uses
        ->> (cast to text) so the result can be compared against a scalar.
        """
        if path is None or path == "" or path == []:
            return column
        if isinstance(path, str):
            parts = [p for p in path.split(".") if p]
        else:
            parts = [str(p) for p in path if p or p == 0]
        if not parts:
            return column
        esc = [QueryBuilder._escape_json_path_segment(p) for p in parts]
        middle = "".join(f"->'{p}'" for p in esc[:-1])
        return f"{column}{middle}->>'{esc[-1]}'"

    def where_json_contains(self, column: str, value):
        """
        Filter rows whose jsonb column contains the given value (Postgres @>).

        Example:
            Brand.where_json_contains("aliases", ["Olay"])
            # -> aliases @> '["Olay"]'::jsonb

            Product.where_json_contains("metadata", {"active_deal": True})
            # -> metadata @> '{"active_deal": true}'::jsonb
        """
        import json

        return self.where_raw(f"{column} @> %s::jsonb", [json.dumps(value)])

    def or_where_json_contains(self, column: str, value):
        """OR-joined variant of where_json_contains."""
        import json

        return self.or_where_raw(f"{column} @> %s::jsonb", [json.dumps(value)])

    def where_json_doesnt_contain(self, column: str, value):
        """Inverse of where_json_contains."""
        import json

        return self.where_raw(f"NOT ({column} @> %s::jsonb)", [json.dumps(value)])

    def where_json_path(self, column: str, path, operator: str = "=", value=None):
        """
        Filter by a nested JSON path extract.

        Arguments:
            column {string} -- The JSON column, e.g. "metadata".
            path {string|list} -- Dotted string ("active_deal.savings_pct") or a list
                of segments. Each segment is treated as a key name, not an array index.
            operator {string} -- SQL comparison operator ("=", ">=", "!=", "LIKE", ...).
            value -- The value to compare against (bound safely).

        Example:
            q.where_json_path("metadata", "external_order_id", "=", "ord_123")
            # -> metadata->>'external_order_id' = %s
        """
        # Two-arg form: where_json_path(column, path, value) with operator defaulted to "="
        if value is None and operator not in (
            "=", "!=", "<>", ">", ">=", "<", "<=", "LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE",
        ):
            value = operator
            operator = "="
        sql_col = self._json_path_sql(column, path)
        return self.where_raw(f"{sql_col} {operator} %s", [value])

    def or_where_json_path(self, column: str, path, operator: str = "=", value=None):
        """OR-joined variant of where_json_path."""
        if value is None and operator not in (
            "=", "!=", "<>", ">", ">=", "<", "<=", "LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE",
        ):
            value = operator
            operator = "="
        sql_col = self._json_path_sql(column, path)
        return self.or_where_raw(f"{sql_col} {operator} %s", [value])

    def where_json_length(self, column: str, operator_or_value, value=None):
        """
        Filter by length of a jsonb array column.

        Example:
            q.where_json_length("aliases", ">", 0)       # jsonb_array_length(aliases) > 0
            q.where_json_length("aliases", 3)            # jsonb_array_length(aliases) = 3
        """
        if value is None:
            operator, val = "=", operator_or_value
        else:
            operator, val = operator_or_value, value
        return self.where_raw(
            f"jsonb_array_length({column}) {operator} %s", [val]
        )

    def where_json_key_exists(self, column: str, key: str):
        """
        Filter rows whose jsonb object contains the given top-level key (Postgres ?).

        Example:
            q.where_json_key_exists("metadata", "active_deal")
            # -> metadata ? 'active_deal'
        """
        esc = self._escape_json_path_segment(key)
        return self.where_raw(f"{column} ? '{esc}'")

    def or_where(self, column, *args):
        """
        Specifies an or where query expression.

        Arguments:
            column {[type]} -- [description]
            value {[type]} -- [description]

        Returns:
            [type] -- [description]
        """
        operator, value = self._extract_operator_value(*args)
        if inspect.isfunction(column):
            builder = column(self.new())
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        operator,
                        SubGroupExpression(builder),
                        keyword="or",
                    )
                ),
            )
        elif isinstance(value, QueryBuilder):
            self._wheres += (
                (
                    QueryExpression(
                        column,
                        operator,
                        SubSelectExpression(value),
                    )
                ),
            )
        else:
            self._wheres += (
                (
                    QueryExpression(
                        column,
                        operator,
                        value,
                        "value",
                        keyword="or",
                    )
                ),
            )
        return self

    def where_exists(self, value: "str|int|QueryBuilder"):
        """
        Specifies a where exists expression.

        Arguments:
            value {string|int|QueryBuilder} -- A value to check for the existence of a query expression.

        Returns:
            self
        """
        if inspect.isfunction(value):
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        "EXISTS",
                        SubSelectExpression(value(self.new())),
                    )
                ),
            )
        elif isinstance(value, QueryBuilder):
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        "EXISTS",
                        SubSelectExpression(value),
                    )
                ),
            )
        else:
            self._wheres += ((QueryExpression(None, "EXISTS", value, "value")),)

        return self

    def or_where_exists(self, value: "str|int|QueryBuilder"):
        """
        Specifies a where exists expression.

        Arguments:
            value {string|int|QueryBuilder} -- A value to check for the existence of a query expression.

        Returns:
            self
        """
        if inspect.isfunction(value):
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        "EXISTS",
                        SubSelectExpression(value(self.new())),
                        keyword="or",
                    )
                ),
            )
        elif isinstance(value, QueryBuilder):
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        "EXISTS",
                        SubSelectExpression(value),
                        keyword="or",
                    )
                ),
            )
        else:
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        "EXISTS",
                        value,
                        "value",
                        keyword="or",
                    )
                ),
            )

        return self

    def where_not_exists(self, value: "str|int|QueryBuilder"):
        """
        Specifies a where exists expression.

        Arguments:
            value {string|int|QueryBuilder} -- A value to check for the existence of a query expression.

        Returns:
            self
        """

        if inspect.isfunction(value):
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        "NOT EXISTS",
                        SubSelectExpression(value(self.new())),
                    )
                ),
            )
        elif isinstance(value, QueryBuilder):
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        "NOT EXISTS",
                        SubSelectExpression(value),
                    )
                ),
            )
        else:
            self._wheres += ((QueryExpression(None, "NOT EXISTS", value, "value")),)

        return self

    def or_where_not_exists(self, value: "str|int|QueryBuilder"):
        """
        Specifies a where exists expression.

        Arguments:
            value {string|int|QueryBuilder} -- A value to check for the existence of a query expression.

        Returns:
            self
        """

        if inspect.isfunction(value):
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        "NOT EXISTS",
                        SubSelectExpression(value(self.new())),
                        keyword="or",
                    )
                ),
            )
        elif isinstance(value, QueryBuilder):
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        "NOT EXISTS",
                        SubSelectExpression(value),
                        keyword="or",
                    )
                ),
            )
        else:
            self._wheres += (
                (
                    QueryExpression(
                        None,
                        "NOT EXISTS",
                        value,
                        "value",
                        keyword="or",
                    )
                ),
            )

        return self

    def having(self, column, equality="", value=""):
        """
        Specifying a having expression.

        Arguments:
            column {string} -- The name of the column.

        Keyword Arguments:
            equality {string} -- An equality operator (default: {"="})
            value {string} -- The value of the having expression (default: {""})

        Returns:
            self
        """
        self._having += ((HavingExpression(column, equality, value)),)
        return self

    def having_raw(self, string):
        """
        Specifies raw SQL that should be injected into the having expression.

        Arguments:
            string {string} -- The raw query string.

        Returns:
            self
        """
        self._having += ((HavingExpression(string, raw=True)),)
        return self

    def where_null(self, column):
        """
        Specifies a where expression where the column is NULL.

        Arguments:
            column {string} -- The name of the column.

        Returns:
            self
        """
        self._wheres += ((QueryExpression(column, "=", None, "NULL")),)
        return self

    def or_where_null(self, column):
        """
        Specifies a where expression where the column is NULL.

        Arguments:
            column {string} -- The name of the column.

        Returns:
            self
        """
        self._wheres += ((QueryExpression(column, "=", None, "NULL", keyword="or")),)
        return self

    # NOTE: ``chunk`` is defined later in this class with a Laravel-style
    # ``(chunk_size, callback)`` signature. The dead generator-style version
    # previously here has been removed — it could not be reached because
    # Python class bodies use last-definition-wins semantics.

    def where_not_null(self, column: str):
        """
        Specifies a where expression where the column is not NULL.

        Arguments:
            column {string} -- The name of the column.

        Returns:
            self
        """
        self._wheres += ((QueryExpression(column, "=", True, "NOT NULL")),)
        return self

    def _get_date_string(self, date):
        if isinstance(date, str):
            return date
        elif hasattr(date, "to_date_string"):
            return date.to_date_string()
        elif hasattr(date, "strftime"):
            return date.strftime("%m-%d-%Y")

    def where_date(self, column: str, date: "str|datetime"):
        """
        Specifies a where DATE expression.

        Arguments:
            column {string} -- The name of the column.

        Returns:
            self
        """
        self._wheres += (
            (
                QueryExpression(
                    column,
                    "=",
                    self._get_date_string(date),
                    "DATE",
                )
            ),
        )
        return self

    def or_where_date(self, column: str, date: "str|datetime"):
        """
        Specifies a where DATE expression.

        Arguments:
            column {string} -- The name of the column.
            date {string|datetime|pendulum} -- The name of the column.

        Returns:
            self
        """
        self._wheres += (
            (
                QueryExpression(
                    column,
                    "=",
                    self._get_date_string(date),
                    "DATE",
                    keyword="or",
                )
            ),
        )
        return self

    def between(self, column: str, low: int, high: int):
        """
        Specifies a where between expression.

        Arguments:
            column {string} -- The name of the column.
            low {string} -- The value on the low end.
            high {string} -- The value on the high end.

        Returns:
            self
        """
        self._wheres += (BetweenExpression(column, low, high),)
        return self

    def where_between(self, *args, **kwargs):
        return self.between(*args, **kwargs)

    def where_not_between(self, *args, **kwargs):
        return self.not_between(*args, **kwargs)

    def not_between(self, column: str, low: str, high: str):
        """
        Specifies a where not between expression.

        Arguments:
            column {string} -- The name of the column.
            low {string} -- The value on the low end.
            high {string} -- The value on the high end.

        Returns:
            self
        """
        self._wheres += (BetweenExpression(column, low, high, not_between=True),)
        return self

    def where_in(self, column, wheres=None):
        """
        Specifies where a column contains a list of a values.

        Arguments:
            column {string} -- The name of the column.

        Keyword Arguments:
            wheres {list} -- A list of values (default: {[]})

        Returns:
            self
        """

        wheres = wheres or []

        if not wheres:
            self._wheres += ((QueryExpression(0, "=", 1, "value_equals")),)

        elif isinstance(wheres, QueryBuilder):
            self._wheres += (
                (
                    QueryExpression(
                        column,
                        "IN",
                        SubSelectExpression(wheres),
                    )
                ),
            )
        elif callable(wheres):
            self._wheres += (
                (
                    QueryExpression(
                        column,
                        "IN",
                        SubSelectExpression(wheres(self.new())),
                    )
                ),
            )
        else:
            self._wheres += ((QueryExpression(column, "IN", list(wheres))),)
        return self

    def get_relation(self, relationship, builder=None):
        if not builder:
            builder = self

        if not builder._model:
            raise AttributeError(
                "You must specify a model in order to use relationship methods"
            )

        return getattr(builder._model, relationship)

    def has(self, *relationships):
        if not self._model:
            raise AttributeError(
                "You must specify a model in order to use 'has' relationship methods"
            )

        for relationship in relationships:
            if "." in relationship:
                last_builder = self._model.builder
                for split_relationship in relationship.split("."):
                    related = last_builder.get_relation(split_relationship)
                    last_builder = related.query_has(last_builder)
            else:
                related = getattr(self._model, relationship)
                related.query_has(self)
        return self

    def or_has(self, *relationships):
        if not self._model:
            raise AttributeError(
                "You must specify a model in order to use 'has' relationship methods"
            )

        for relationship in relationships:
            if "." in relationship:
                last_builder = self._model.builder
                split_count = len(relationship.split("."))
                for index, split_relationship in enumerate(relationship.split(".")):
                    related = last_builder.get_relation(split_relationship)

                    if index + 1 == split_count:
                        last_builder = related.query_has(
                            last_builder,
                            method="where_exists",
                        )
                        continue

                    last_builder = related.query_has(
                        last_builder,
                        method="or_where_exists",
                    )
            else:
                related = getattr(self._model, relationship)
                related.query_has(self, method="or_where_exists")
        return self

    def doesnt_have(self, *relationships):
        if not self._model:
            raise AttributeError(
                "You must specify a model in order to use the 'doesnt_have' relationship methods"
            )

        for relationship in relationships:
            if "." in relationship:
                last_builder = self._model.builder
                split_count = len(relationship.split("."))
                for index, split_relationship in enumerate(relationship.split(".")):
                    related = last_builder.get_relation(split_relationship)
                    if index + 1 == split_count:
                        last_builder = related.query_has(
                            last_builder,
                            method="where_exists",
                        )
                        continue

                    last_builder = related.query_has(
                        last_builder,
                        method="where_not_exists",
                    )
            else:
                related = getattr(self._model, relationship)
                related.query_has(self, method="where_not_exists")
        return self

    def or_doesnt_have(self, *relationships):
        if not self._model:
            raise AttributeError(
                "You must specify a model in order to use the 'doesnt_have' relationship methods"
            )

        for relationship in relationships:
            if "." in relationship:
                last_builder = self._model.builder
                split_count = len(relationship.split("."))
                for index, split_relationship in enumerate(relationship.split(".")):
                    related = last_builder.get_relation(split_relationship)
                    if index + 1 == split_count:
                        last_builder = related.query_has(
                            last_builder,
                            method="where_exists",
                        )
                        continue

                    last_builder = related.query_has(
                        last_builder,
                        method="or_where_not_exists",
                    )
            else:
                related = getattr(self._model, relationship)
                related.query_has(self, method="or_where_not_exists")
        return self

    def where_has(self, relationship, callback):
        if not self._model:
            raise AttributeError(
                "You must specify a model in order to use 'has' relationship methods"
            )

        if "." in relationship:
            last_builder = self._model.builder
            splits = relationship.split(".")
            split_count = len(splits)
            for index, split_relationship in enumerate(splits):
                related = last_builder.get_relation(split_relationship)

                if index + 1 == split_count:
                    last_builder = related.query_where_exists(
                        last_builder,
                        callback,
                        method="where_exists",
                    )
                    continue
                last_builder = related.query_has(last_builder, method="where_exists")
        else:
            related = getattr(self._model, relationship)
            related.query_where_exists(self, callback, method="where_exists")
        return self

    def or_where_has(self, relationship, callback):
        if not self._model:
            raise AttributeError(
                "You must specify a model in order to use 'has' relationship methods"
            )

        if "." in relationship:
            last_builder = self._model.builder
            splits = relationship.split(".")
            split_count = len(splits)
            for index, split_relationship in enumerate(splits):
                related = last_builder.get_relation(split_relationship)
                if index + 1 == split_count:
                    last_builder = related.query_where_exists(
                        last_builder,
                        callback,
                        method="where_exists",
                    )
                    continue

                last_builder = related.query_has(last_builder, method="or_where_exists")
        else:
            related = getattr(self._model, relationship)
            related.query_where_exists(self, callback, method="or_where_exists")
        return self

    def where_doesnt_have(self, relationship, callback):
        if not self._model:
            raise AttributeError(
                "You must specify a model in order to use the 'doesnt_have' relationship methods"
            )

        if "." in relationship:
            last_builder = self._model.builder
            split_count = len(relationship.split("."))
            for index, split_relationship in enumerate(relationship.split(".")):
                related = last_builder.get_relation(split_relationship)
                if index + 1 == split_count:
                    last_builder = getattr(
                        last_builder._model,
                        split_relationship,
                    ).query_where_exists(
                        last_builder,
                        callback,
                        method="where_not_exists",
                    )
                    continue

                last_builder = related.query_has(
                    last_builder, method="where_not_exists"
                )
        else:
            related = getattr(self._model, relationship)
            related.query_where_exists(self, callback, method="where_not_exists")
        return self

    def or_where_doesnt_have(self, relationship, callback):
        if not self._model:
            raise AttributeError(
                "You must specify a model in order to use the 'doesnt_have' relationship methods"
            )

        if "." in relationship:
            last_builder = self._model.builder
            split_count = len(relationship.split("."))
            for index, split_relationship in enumerate(relationship.split(".")):
                related = last_builder.get_relation(split_relationship)
                if index + 1 == split_count:
                    last_builder = getattr(
                        last_builder._model,
                        split_relationship,
                    ).query_where_exists(
                        last_builder,
                        callback,
                        method="or_where_not_exists",
                    )
                    continue

                last_builder = related.query_has(
                    last_builder,
                    method="or_where_not_exists",
                )
        else:
            related = getattr(self._model, relationship)
            related.query_where_exists(self, callback, method="or_where_not_exists")
        return self

    def with_count(self, relationship, callback=None):
        self.select(*self._model.get_selects())
        return getattr(self._model, relationship).get_with_count_query(
            self, callback=callback
        )


    def with_sum(self, relationship, column, callback=None):
        """Eager load a relationship's SUM aggregate.

        Adds {relationship}_{column}_sum attribute to each model.

        Example:
            Product.with_sum("prices", "price_min").get()
            # product.prices_price_min_sum = 150.00
        """
        self.select(*self._model.get_selects())
        return getattr(self._model, relationship).get_with_sum_query(
            self, column, callback=callback
        )

    def with_avg(self, relationship, column, callback=None):
        """Eager load a relationship's AVG aggregate.

        Adds {relationship}_{column}_avg attribute to each model.

        Example:
            Product.with_avg("prices", "price").get()
            # product.prices_price_avg = 75.50
        """
        self.select(*self._model.get_selects())
        return getattr(self._model, relationship).get_with_avg_query(
            self, column, callback=callback
        )

    def with_min(self, relationship, column, callback=None):
        """Eager load a relationship's MIN aggregate.

        Adds {relationship}_{column}_min attribute to each model.

        Example:
            Product.with_min("prices", "price").get()
            # product.prices_price_min = 10.00
        """
        self.select(*self._model.get_selects())
        return getattr(self._model, relationship).get_with_min_query(
            self, column, callback=callback
        )

    def with_max(self, relationship, column, callback=None):
        """Eager load a relationship's MAX aggregate.

        Adds {relationship}_{column}_max attribute to each model.

        Example:
            Product.with_max("prices", "price").get()
            # product.prices_price_max = 200.00
        """
        self.select(*self._model.get_selects())
        return getattr(self._model, relationship).get_with_max_query(
            self, column, callback=callback
        )

    def tap(self, callback):
        """Execute callback with the builder and return the builder for chaining.

        Useful for debugging or side effects without breaking the chain.

        Example:
            Product.active().tap(lambda q: print(q.to_sql())).get()
        """
        callback(self)
        return self

    def pipe(self, callback):
        """Pass the builder to a callback and return the callback's result.

        Unlike tap(), pipe() returns what the callback returns.

        Example:
            result = Product.active().pipe(lambda q: q.count() > 0)
        """
        return callback(self)

    def where_not_in(self, column, wheres=None):
        """
        Specifies where a column does not contain a list of a values.

        Arguments:
            column {string} -- The name of the column.

        Keyword Arguments:
            wheres {list} -- A list of values (default: {[]})

        Returns:
            self
        """

        wheres = wheres or []

        if isinstance(wheres, QueryBuilder):
            self._wheres += (
                (
                    QueryExpression(
                        column,
                        "NOT IN",
                        SubSelectExpression(wheres),
                    )
                ),
            )
        elif not wheres:
            return self
        else:
            self._wheres += ((QueryExpression(column, "NOT IN", list(wheres))),)
        return self

    def join(
        self,
        table: str,
        column1=None,
        equality=None,
        column2=None,
        clause="inner",
    ):
        """
        Specifies a join expression.

        Arguments:
            table {string} -- The name of the table or an instance of JoinClause.
            column1 {string} -- The name of the foreign table.
            equality {string} -- The equality to join on.
            column2 {string} -- The name of the local column.

        Keyword Arguments:
            clause {string} -- The action clause. (default: {"inner"})

        Returns:
            self
        """
        if inspect.isfunction(column1):
            self._joins += (column1(JoinClause(table, clause=clause)),)
        elif isinstance(table, str):
            self._joins += (
                JoinClause(table, clause=clause).on(column1, equality, column2),
            )
        else:
            self._joins += (table,)
        return self

    def left_join(
        self,
        table,
        column1=None,
        equality=None,
        column2=None,
    ):
        """
        A helper method to add a left join expression.

        Arguments:
            table {string} -- The name of the table to join on.
            column1 {string} -- The name of the foreign table.
            equality {string} -- The equality to join on.
            column2 {string} -- The name of the local column.

        Returns:
            self
        """
        return self.join(
            table=table,
            column1=column1,
            equality=equality,
            column2=column2,
            clause="left",
        )

    def right_join(
        self,
        table,
        column1=None,
        equality=None,
        column2=None,
    ):
        """
        A helper method to add a right join expression.

        Arguments:
            table {string} -- The name of the table to join on.
            column1 {string} -- The name of the foreign table.
            equality {string} -- The equality to join on.
            column2 {string} -- The name of the local column.

        Returns:
            self
        """
        return self.join(
            table=table,
            column1=column1,
            equality=equality,
            column2=column2,
            clause="right",
        )

    def joins(self, *relationships, clause="inner"):
        for relationship in relationships:
            getattr(self._model, relationship).joins(self, clause=clause)

        return self

    def join_on(self, relationship, callback=None, clause="inner"):
        relation = getattr(self._model, relationship)
        relation.joins(self, clause=clause)

        if callback:
            new_from_builder = self.new_from_builder()
            new_from_builder.table(relation.get_builder().get_table_name())
            self.where_from_builder(callback(new_from_builder))

        return self

    def where_column(self, column1, column2):
        """
        Specifies where two columns equal eachother.

        Arguments:
            column1 {string} -- The name of the column.
            column2 {string} -- The name of the column.

        Returns:
            self
        """
        self._wheres += ((QueryExpression(column1, "=", column2, "column")),)
        return self

    def take(self, *args, **kwargs):
        """Alias for limit method."""
        return self.limit(*args, **kwargs)

    def limit(self, amount):
        """
        Specifies a limit expression.

        Arguments:
            amount {int} -- The number of rows to limit.

        Returns:
            self
        """
        self._limit = amount
        return self

    def offset(self, amount):
        """
        Specifies an offset expression.

        Arguments:
            amount {int} -- The number of rows to limit.

        Returns:
            self
        """
        self._offset = amount
        return self

    def skip(self, *args, **kwargs):
        """Alias for limit method."""
        return self.offset(*args, **kwargs)

    def update(
        self,
        updates: Dict[str, Any],
        dry: bool = False,
        force: bool = False,
        cast: bool = True,
        ignore_mass_assignment: bool = False,
    ):
        """
        Specifies columns and values to be updated.

        Arguments:
            updates {dictionary} -- A dictionary of columns and values to update.
            dry {bool, optional}: Do everything except execute the query against the DB
            force {bool, optional}: Force an update statement to be executed even if nothing was changed
            cast {bool, optional}: Run all values through model's casters
            ignore_mass_assignment {bool, optional}: Whether the update should ignore mass assignment on the model

        Returns:
            self
        """
        model = None

        additional = {}

        if self._model:
            model = self._model
            # Filter __fillable/__guarded__ fields
            if not ignore_mass_assignment:
                updates = model.filter_mass_assignment(updates)

        if model and model.is_loaded():
            self.where(
                model.get_primary_key(),
                model.get_primary_key_value(),
            )
            additional.update({model.get_primary_key(): model.get_primary_key_value()})

            self.observe_events(model, "updating")

        if model:
            if not model.__force_update__ and not force:
                # Filter updates to only those with changes
                updates = {
                    attr: value
                    for attr, value in updates.items()
                    if (
                        value is None
                        or model.__original_attributes__.get(attr, None) != value
                    )
                }

            # Do not execute query if no changes
            if not updates:
                return self if dry or self.dry else model

            # Cast date fields
            date_fields = model.get_dates()
            for key, value in updates.items():
                if key in date_fields:
                    if value is not None:
                        updates[key] = model.get_new_datetime_string(value)
                    else:
                        updates[key] = value
                # Cast value if necessary
                # NOTE: Must use `is not None` — NOT `if value` — because
                # falsy values like {}, [], 0, False are valid data that
                # still need casting (e.g. json.dumps({}) → "{}").
                # Using truthiness would skip the cast for these values,
                # causing psycopg2 "can't adapt type 'dict'" errors.
                if cast:
                    if value is not None:
                        updates[key] = model.cast_value(key, value)
                    else:
                        updates[key] = value
        elif not updates:
            # Do not perform query if there are no updates
            return self

        self._updates = (UpdateQueryExpression(updates),)
        self.set_action("update")
        if dry or self.dry:
            return self

        additional.update(updates)

        self.new_connection().query(self.to_qmark(), self._bindings)
        if model:
            model.fill(updates)
            self.observe_events(model, "updated")
            model.fill_original(updates)
            return model
        return additional

    def force_update(self, updates: dict, dry=False):
        return self.update(updates, dry=dry, force=True)

    def set_updates(self, updates: dict, dry=False):
        """
        Specifies columns and values to be updated.

        Arguments:
            updates {dictionary} -- A dictionary of columns and values to update.

        Keyword Arguments:
            dry {bool} -- Whether the query should be executed. (default: {False})

        Returns:
            self
        """
        self._updates += (UpdateQueryExpression(updates),)
        return self

    def increment(self, column, value=1, dry=False):
        """
        Increments a column's value.

        Arguments:
            column {string} -- The name of the column.

        Keyword Arguments:
            value {int} -- The value to increment by. (default: {1})

        Returns:
            self
        """
        model = None
        id_key = "id"
        id_value = None

        additional = {}

        if self._model:
            model = self._model
            id_value = self._model.get_primary_key_value()

        if model and model.is_loaded():
            self.where(
                model.get_primary_key(),
                model.get_primary_key_value(),
            )
            additional.update({model.get_primary_key(): model.get_primary_key_value()})

            self.observe_events(model, "updating")

        self._updates += (
            UpdateQueryExpression(column, value, update_type="increment"),
        )

        if dry or self.dry:
            return self.get_grammar().compile("update").to_sql()

        self.set_action("update")
        results = self.new_connection().query(self.to_qmark(), self._bindings)
        processed_results = self.get_processor().get_column_value(
            self, column, results, id_key, id_value
        )
        return processed_results

    def decrement(self, column, value=1, dry=False):
        """
        Decrements a column's value.

        Arguments:
            column {string} -- The name of the column.

        Keyword Arguments:
            value {int} -- The value to decrement by. (default: {1})

        Returns:
            self
        """
        model = None
        id_key = "id"
        id_value = None

        additional = {}

        if self._model:
            model = self._model
            id_value = self._model.get_primary_key_value()

        if model and model.is_loaded():
            self.where(
                model.get_primary_key(),
                model.get_primary_key_value(),
            )
            additional.update({model.get_primary_key(): model.get_primary_key_value()})

            self.observe_events(model, "updating")

        self._updates += (
            UpdateQueryExpression(column, value, update_type="decrement"),
        )

        if dry or self.dry:
            return self.get_grammar().compile("update").to_sql()

        self.set_action("update")
        result = self.new_connection().query(self.to_qmark(), self._bindings)
        processed_results = self.get_processor().get_column_value(
            self, column, result, id_key, id_value
        )
        return processed_results

    def sum(self, column, dry=False):
        """Get the sum of a column's values.

        Returns:
            The sum value, or 0 if no results.
        """
        return self._run_aggregate("SUM", column, dry)

    def count(self, column=None, dry=False):
        """Get the number of records matching the query.

        Args:
            column: Optional column to count (defaults to *).
            dry: If True, return the builder instead of executing.

        Returns:
            int -- The count of matching records.
        """
        col = column or "*"
        return self._run_aggregate("COUNT", col, dry)

    def max(self, column, dry=False):
        """Get the maximum value of a column.

        Returns:
            The max value, or None if no results.
        """
        return self._run_aggregate("MAX", column, dry)

    def order_by(self, column, direction="ASC"):
        """
        Specifies a column to order by.

        Arguments:
            column {string} -- The name of the column.

        Keyword Arguments:
            direction {string} -- Specify either ASC or DESC order. (default: {"ASC"})

        Returns:
            self
        """
        for col in column.split(","):
            self._order_by += (OrderByExpression(col, direction=direction),)
        return self

    def order_by_raw(self, query, bindings=None):
        """
        Specifies a column to order by.

        Arguments:
            column {string} -- The name of the column.

        Keyword Arguments:
            direction {string} -- Specify either ASC or DESC order. (default: {"ASC"})

        Returns:
            self
        """
        if bindings is None:
            bindings = []
        self._order_by += (OrderByExpression(query, raw=True, bindings=bindings),)
        return self

    def group_by(self, column):
        """
        Specifies a column to group by.

        Arguments:
            column {string} -- The name of the column to group by.

        Returns:
            self
        """
        for col in column.split(","):
            self._group_by += (GroupByExpression(column=col),)

        return self

    def group_by_raw(self, query, bindings=None):
        """
        Specifies a column to group by.

        Arguments:
            query {string} -- A raw query

        Returns:
            self
        """
        if bindings is None:
            bindings = []
        self._group_by += (
            GroupByExpression(column=query, raw=True, bindings=bindings),
        )

        return self

    def aggregate(self, aggregate, column, alias=None):
        """Register an aggregate expression on the builder.

        Arguments:
            aggregate {string} -- The aggregate function (COUNT, SUM, etc.).
            column {string} -- The column expression.
            alias {string} -- Optional alias.
        """
        self._aggregates += (
            AggregateExpression(
                aggregate=aggregate,
                column=column,
                alias=alias,
            ),
        )

    def _run_aggregate(self, function, column, dry=False):
        """Execute an aggregate function and return the scalar result.

        Handles stripping ORDER BY (invalid in aggregate-only queries)
        and cleaning up builder state afterward.

        Returns:
            The aggregate result, or None/0 if no results.
        """
        alias = f"m_{function.lower()}_result"
        self.aggregate(function, f"{column} as {alias}")

        if dry or self.dry:
            return self

        saved_order_by = self._order_by
        self._order_by = ()
        try:
            result = self.new_connection().query(
                self.to_qmark(), self._bindings, results=1
            )
        finally:
            self._order_by = saved_order_by

        if result is None:
            return 0 if function == "COUNT" else None

        if isinstance(result, dict):
            val = result.get(alias)
            if val is not None:
                return float(val) if function in ("AVG", "SUM") else val
            return 0 if function == "COUNT" else None

        prepared = list(result.values())
        if not prepared:
            return 0 if function == "COUNT" else None
        return prepared[0]

    def first(self, fields=None, query=False):
        """
        Gets the first record.

        Returns:
            dictionary -- Returns a dictionary of results.
        """

        if not fields:
            fields = []

        self.select(fields).limit(1)

        if query:
            return self

        result = self.new_connection().query(self.to_qmark(), self._bindings, results=1)

        return self.prepare_result(result)

    def first_or_create(self, wheres, creates: Optional[dict] = None):
        """
        Get the first record matching the attributes or create it.

        Returns:
            Model
        """
        if creates is None:
            creates = {}

        record = self.where(wheres).first()
        total = {}
        if record:
            if hasattr(record, "serialize"):
                total.update(record.serialize())
            else:
                total.update(record)

        total.update(creates)
        total.update(wheres)

        total.update(self._creates_related)

        if not record:
            return self.create(total, id_key=self.get_primary_key())
        return record

    def sole(self, query=False):
        """Gets the only record matching a given criteria."""

        result = self.take(2).get()

        if result.is_empty():
            raise ModelNotFoundException()

        if result.count() > 1:
            raise MultipleRecordsFoundException()

        return result.first()

    def sole_value(self, column: str, query=False):
        return self.sole()[column]

    # NOTE: ``first_or_fail`` and ``find_or_fail`` are both defined later
    # in this class. The earlier duplicates here were shadowed and could
    # never be reached; they have been removed.

    def first_where(self, column, *args):
        """Gets the first record with the given key / value pair."""
        if not args:
            return self.where_not_null(column).first()
        return self.where(column, *args).first()

    # NOTE: ``tap`` is already defined earlier in this class — the duplicate
    # definition that used to live here was identical and has been removed
    # to avoid confusion.

    def last(self, column=None, query=False):
        """
        Gets the last record, ordered by column in descendant order or primary key if no column is
        given.

        Returns:
            dictionary -- Returns a dictionary of results.
        """
        _column = column if column else self._model.get_primary_key()
        self.limit(1).order_by(_column, direction="DESC")

        if query:
            return self

        result = self.new_connection().query(
            self.to_qmark(),
            self._bindings,
            results=1,
        )

        return self.prepare_result(result)

    def _get_eager_load_result(self, related, collection):
        return related.eager_load_from_collection(collection)

    def find(self, record_id, column=None, query=False):
        """
        Finds a row by the primary key ID. Requires a model.

        Arguments:
            record_id {int} -- The ID of the primary key to fetch.

        Returns:
            Model|None
        """
        if not column:
            if not self._model:
                raise InvalidArgumentException("A colum to search is required")

            column = self._model.get_primary_key()

        if isinstance(record_id, (list, tuple)):
            self.where_in(column, record_id)
        else:
            self.where(column, record_id)

        if query:
            return self

        return self.first()

    def find_or(
        self,
        record_id: int,
        callback: Callable,
        args=None,
        column=None,
    ):
        """
        Finds a row by the primary key ID (Requires a model) or raise a ModelNotFound exception.

        Arguments:
            record_id {int} -- The ID of the primary key to fetch.
            callback {Callable} -- The function to call if no record is found.

        Returns:
            Model|Callable
        """

        if not callable(callback):
            raise InvalidArgumentException("A callback must be callable.")

        result = self.find(record_id=record_id, column=column)

        if not result:
            if not args:
                return callback()
            else:
                return callback(*args)

        return result

    def find_or_fail(self, record_id, column=None):
        """
        Finds a row by the primary key ID (Requires a model) or raise a ModelNotFound exception.

        Arguments:
            record_id {int} -- The ID of the primary key to fetch.

        Returns:
            Model|ModelNotFound
        """

        result = self.find(record_id=record_id, column=column)

        if not result:
            raise ModelNotFoundException()

        return result

    def find_or_404(self, record_id, column=None):
        """
        Finds a row by the primary key ID (Requires a model) or raise an 404 exception.

        Arguments:
            record_id {int} -- The ID of the primary key to fetch.

        Returns:
            Model|HTTP404
        """

        try:
            return self.find_or_fail(record_id=record_id, column=column)
        except ModelNotFoundException:
            raise HTTP404Exception()

    def first_or_fail(self, query=False):
        """
        Returns the first row from database. If no result found a ModelNotFound exception.

        Returns:
            dictionary|ModelNotFound
        """

        if query:
            return self.first(query=True)

        result = self.first()

        if not result:
            raise ModelNotFoundException()

        return result

    def get_primary_key(self):
        return self._model.get_primary_key()

    def prepare_result(self, result, collection=False):
        if self._model and result:
            # eager load here
            hydrated_model = self._model.hydrate(result)

            if (
                self._eager_relation.relations
                or self._eager_relation.eagers
                or self._eager_relation.nested_eagers
                or self._eager_relation.callback_eagers
            ) and hydrated_model:
                # Process all registered relations
                all_relations = (
                    self._eager_relation.get_relations()
                    + self._eager_relation.get_eagers()
                )
                for eager_load in set(all_relations):  # Remove duplicates
                    if isinstance(eager_load, dict):
                        # Nested
                        for (
                            relation,
                            eagers,
                        ) in eager_load.items():
                            callback = None
                            if inspect.isclass(self._model):
                                related = getattr(self._model, relation)
                            elif callable(eagers):
                                related = getattr(self._model, relation)
                                callback = eagers
                            else:
                                related = self._model.get_related(relation)

                            result_set = related.get_related(
                                self,
                                hydrated_model,
                                eagers=eagers,
                                callback=callback,
                            )

                            self._register_relationships_to_model(
                                related,
                                result_set,
                                hydrated_model,
                                relation_key=relation,
                            )
                    elif isinstance(eager_load, str):
                        # Single string relation
                        try:
                            if inspect.isclass(self._model):
                                # Get relationship instance (now works as property)
                                related = getattr(self._model, eager_load)
                                # If it's a function (old style), call it to get relationship
                                if callable(related) and not hasattr(
                                    related, "get_related"
                                ):
                                    related = related()
                            else:
                                related = self._model.get_related(eager_load)

                            result_set = related.get_related(self, hydrated_model)

                            self._register_relationships_to_model(
                                related,
                                result_set,
                                hydrated_model,
                                relation_key=eager_load,
                            )
                        except Exception as e:
                            from cara.facades import Log

                            Log.error(f"Error processing eager {eager_load}: {str(e)}")
                            raise
                    else:
                        # List/tuple of relations
                        for eager in eager_load:
                            try:
                                if inspect.isclass(self._model):
                                    # Get relationship instance (now works as property)
                                    related = getattr(self._model, eager)
                                    # If it's a function (old style), call it to get relationship
                                    if callable(related) and not hasattr(
                                        related, "get_related"
                                    ):
                                        related = related()
                                else:
                                    related = self._model.get_related(eager)

                                result_set = related.get_related(self, hydrated_model)

                                self._register_relationships_to_model(
                                    related,
                                    result_set,
                                    hydrated_model,
                                    relation_key=eager,
                                )
                            except Exception as e:
                                from cara.facades import Log

                                Log.error(f"Error processing eager {eager}: {str(e)}")
                                raise

            if collection:
                return hydrated_model if result else Collection([])
            else:
                return hydrated_model if result else None

        if collection:
            return Collection(result) if result else Collection([])
        else:
            return result or None

    def _register_relationships_to_model(
        self,
        related,
        related_result,
        hydrated_model,
        relation_key,
    ):
        """
        Takes a related result and a hydrated model and registers them to eachother using the
        relation key.

        Args:
            related_result (Model|Collection): Will be the related result based on the type of relationship.
            hydrated_model (Model|Collection): If a collection we will need to loop through the collection of models
                                                and register each one individually. Else we can just load the
                                                related_result into the hydrated_models
            relation_key (string): A key to bind the relationship with. Defaults to None.

        Returns:
            self
        """
        if related_result and isinstance(hydrated_model, Collection):
            map_related = self._map_related(related_result, related)
            for model in hydrated_model:
                if isinstance(related_result, Collection):
                    related.register_related(relation_key, model, map_related)
                else:
                    model.add_relation({relation_key: map_related or None})
        else:
            hydrated_model.add_relation({relation_key: related_result or None})
        return self

    def _map_related(self, related_result, related):
        return related.map_related(related_result)

    def all(self, selects=None, query=False):
        """
        Returns all records from the table.

        Returns:
            dictionary -- Returns a dictionary of results.
        """
        selects = selects or []
        self.select(*selects)

        if query:
            return self

        result = self.new_connection().query(self.to_qmark(), self._bindings) or []

        return self.prepare_result(result, collection=True)

    def get(self, selects=None):
        """
        Runs the select query built from the query builder.

        Returns:
            self
        """
        selects = selects or []
        self.select(*selects)
        result = self.new_connection().query(self.to_qmark(), self._bindings)

        return self.prepare_result(result, collection=True)

    def new_connection(self):
        if self._connection:
            return self._connection

        # Use DatabaseManager to create connection instance
        self._connection = self._db_manager.create_connection_instance(
            self.connection, self._schema
        )
        return self._connection

    def get_connection(self):
        return self._connection

    def without_eager(self):
        self._should_eager = False
        return self

    def with_(self, *eagers):
        try:
            self._eager_relation.register(*eagers)
        except Exception as e:
            from cara.facades import Log

            Log.error(f"Eager relation register failed: {str(e)}")
            raise
        return self

    def paginate(self, per_page, page=1):
        if page == 1:
            offset = 0
        else:
            offset = (int(page) * per_page) - per_page

        new_from_builder = self.new_from_builder()
        new_from_builder._order_by = ()
        new_from_builder._columns = ()

        result = self.limit(per_page).offset(offset).get()
        total = new_from_builder.count()

        paginator = LengthAwarePaginator(result, per_page, page, total)
        return paginator

    def simple_paginate(self, per_page, page=1):
        if page == 1:
            offset = 0
        else:
            offset = (int(page) * per_page) - per_page

        result = self.limit(per_page).offset(offset).get()

        paginator = SimplePaginator(result, per_page, page)
        return paginator

    def set_action(self, action):
        """
        Sets the action that the query builder should take when the query is built.

        Arguments:
            action {string} -- The action that the query builder should take.

        Returns:
            self
        """
        self._action = action
        return self

    def get_grammar(self):
        """
        Initializes and returns the grammar class.

        Returns:
            cara.eloquent.grammar.Grammar -- An ORM grammar class.
        """

        # Either _creates when creating, otherwise use columns
        columns = self._creates or self._columns
        if not columns and not self._aggregates and self._model:
            self.select(*self._model.get_selects())
            columns = self._columns

        grammar_instance = self.grammar(
            columns=columns,
            table=self._table,
            wheres=self._wheres,
            limit=self._limit,
            offset=self._offset,
            updates=self._updates,
            aggregates=self._aggregates,
            order_by=self._order_by,
            group_by=self._group_by,
            distinct=self._distinct,
            lock=self.lock,
            joins=self._joins,
            having=self._having,
        )

        # Pass upsert data to grammar if it's an upsert action
        if hasattr(self, "_upsert_values"):
            grammar_instance._upsert_values = getattr(self, "_upsert_values", [])
            grammar_instance._upsert_unique_by = getattr(self, "_upsert_unique_by", [])
            grammar_instance._upsert_update = getattr(self, "_upsert_update", [])

        return grammar_instance

    def to_sql(self):
        """
        Compiles the QueryBuilder class into a SQL statement.

        Returns:
            self
        """

        self.run_scopes()
        grammar = self.get_grammar()
        sql = grammar.compile(self._action, qmark=False).to_sql()
        return sql

    def explain(self):
        """
        Explains the Query execution plan.

        Returns:
            Collection
        """
        sql = self.to_sql()
        explanation = self.statement(f"EXPLAIN {sql}")
        return explanation

    def dump_sql(self, pretty: bool = True):
        """Compile the query without executing and return (sql, bindings).

        Equivalent to Laravel's ``$query->toSql() + $query->getBindings()`` in one
        call. Uses the qmark path so bindings are isolated from the SQL string.

        Example:
            sql, params = Product.active().where("id", 5).dump_sql()
        """
        # to_qmark() has a side effect of resetting the builder; take a copy first
        # so subsequent calls on the original builder still work.
        cloned = deepcopy(self)
        grammar = cloned.get_grammar()
        cloned.run_scopes()
        sql = grammar.compile(cloned._action, qmark=True).to_sql()
        bindings = list(grammar._bindings)
        if pretty:
            # Swap '?' placeholders for %s for psycopg-style display
            sql = sql.replace("'?'", "%s")
        return sql, bindings

    def debug_sql(self):
        """Print compiled SQL + bindings to stderr (dev-aid). Returns self for chaining.

        Example:
            rows = Product.active().where("brand", "Olay").debug_sql().get()
            # stderr: [SQL] SELECT ... FROM "product" WHERE "brand" = %s
            # stderr: [BIND] ['Olay']
        """
        import sys

        sql, bindings = self.dump_sql()
        print(f"[SQL] {sql}", file=sys.stderr)
        print(f"[BIND] {bindings}", file=sys.stderr)
        return self

    def run_scopes(self):
        for name, scope in self._global_scopes.get(self._action, {}).items():
            scope(self)

        return self

    def to_qmark(self):
        """
        Compiles the QueryBuilder class into a Qmark SQL statement.

        Returns:
            self
        """

        self.run_scopes()
        grammar = self.get_grammar()
        sql = grammar.compile(self._action, qmark=True).to_sql()

        self._bindings = grammar._bindings

        self.reset()

        return sql

    def new(self):
        """
        Creates a new QueryBuilder class.

        Returns:
            QueryBuilder -- The ORM QueryBuilder class.
        """
        builder = QueryBuilder(
            grammar=self.grammar,
            connection_class=self.connection_class,
            connection=self.connection,
            connection_driver=self._connection_driver,
            model=self._model,
        )

        if self._table:
            builder.table(self._table.name)

        return builder

    def avg(self, column, dry=False):
        """Get the average value of a column.

        Returns:
            The average value, or None if no results.
        """
        return self._run_aggregate("AVG", column, dry)

    def min(self, column, dry=False):
        """Get the minimum value of a column.

        Returns:
            The min value, or None if no results.
        """
        return self._run_aggregate("MIN", column, dry)

    def _extract_operator_value(self, *args):
        operators = [
            "=",
            ">",
            ">=",
            "<",
            "<=",
            "!=",
            "<>",
            "like",
            "not like",
            "regexp",
            "not regexp",
        ]

        operator = operators[0]

        value = None

        if (len(args)) >= 2:
            operator = args[0]
            value = args[1]
        elif len(args) == 1:
            value = args[0]

        if operator not in operators:
            raise ValueError(
                "Invalid comparison operator. The operator can be %s"
                % ", ".join(operators)
            )

        return operator, value

    def __call__(self):
        """
        Magic method to standardize what happens when the query builder object is called.

        Returns:
            self
        """
        return self

    def macro(self, name, callable):
        self._macros.update({name: callable})
        return self

    def when(self, conditional, callback, otherwise=None):
        """Apply the callback if the condition is truthy (Laravel-style).

        Args:
            conditional: The condition to evaluate.
            callback: Called with the builder when condition is truthy.
            otherwise: Called with the builder when condition is falsy.

        Returns:
            self
        """
        if conditional:
            callback(self)
        elif otherwise is not None:
            otherwise(self)
        return self

    def unless(self, conditional, callback, otherwise=None):
        """Apply the callback if the condition is falsy (opposite of when).

        Args:
            conditional: The condition to evaluate.
            callback: Called with the builder when condition is falsy.
            otherwise: Called with the builder when condition is truthy.

        Returns:
            self
        """
        return self.when(not conditional, callback, otherwise)

    def truncate(self, foreign_keys=False, dry=False):
        sql = self.get_grammar().truncate_table(self.get_table_name(), foreign_keys)

        if dry or self.dry:
            return sql

        return self.new_connection().query(sql, ())

    def exists(self):
        """Determine if any rows exist for the current query.

        Uses SELECT 1 ... LIMIT 1 for efficiency instead of fetching a full row.

        Returns:
            bool
        """
        saved_columns = self._columns
        saved_limit = self._limit
        self._columns = (SelectExpression("1", raw=True),)
        self._limit = 1
        try:
            result = self.new_connection().query(
                self.to_qmark(), self._bindings, results=1
            )
        finally:
            self._columns = saved_columns
            self._limit = saved_limit
        return result is not None and result != {}

    def doesnt_exist(self):
        """Determine if no rows exist for the current query.

        Returns:
            bool
        """
        return not self.exists()

    def in_random_order(self):
        """Puts Query results in random order."""
        return self.order_by_raw(self.grammar().compile_random())

    def new_from_builder(self, from_builder=None):
        """Create a new QueryBuilder copying all state from an existing builder.

        Returns:
            QueryBuilder
        """
        if from_builder is None:
            from_builder = self

        builder = QueryBuilder(
            grammar=self.grammar,
            connection_class=self.connection_class,
            connection=self.connection,
            connection_driver=self._connection_driver,
            model=from_builder._model,
        )

        if self._table:
            builder.table(self._table.name)

        builder._columns = tuple(deepcopy(from_builder._columns))
        builder._creates = deepcopy(from_builder._creates)
        builder._sql = ""
        builder._bindings = tuple(deepcopy(from_builder._bindings))
        builder._updates = tuple(deepcopy(from_builder._updates))
        builder._wheres = tuple(deepcopy(from_builder._wheres))
        builder._order_by = tuple(deepcopy(from_builder._order_by))
        builder._group_by = tuple(deepcopy(from_builder._group_by))
        builder._joins = tuple(deepcopy(from_builder._joins))
        builder._having = tuple(deepcopy(from_builder._having))
        builder._macros = deepcopy(from_builder._macros)
        builder._aggregates = tuple(deepcopy(from_builder._aggregates))
        builder._global_scopes = deepcopy(from_builder._global_scopes)
        builder._limit = from_builder._limit
        builder._offset = from_builder._offset
        builder._distinct = from_builder._distinct
        builder._eager_relation = deepcopy(from_builder._eager_relation)

        return builder

    def clone(self):
        """Create an independent copy of this builder (Laravel-style).

        Useful when you need to run both count() and get() from the same
        base query without one operation corrupting the other.

        Returns:
            QueryBuilder
        """
        return self.new_from_builder(self)

    def get_table_columns(self):
        return self.get_schema().get_columns(self._table.name)

    def get_schema(self):
        return Schema(
            connection=self.connection,
            connection_details=self._connection_details,
        )

    def latest(self, *fields):
        """
        Gets the latest record.

        Returns:
            querybuilder
        """

        if not fields:
            fields = ("created_at",)

        return self.order_by(column=",".join(fields), direction="DESC")

    def oldest(self, *fields):
        """
        Gets the oldest record.

        Returns:
            querybuilder
        """

        if not fields:
            fields = ("created_at",)

        return self.order_by(column=",".join(fields), direction="ASC")

    def value(self, column: str):
        """Get a single column's value from the first result.

        Returns:
            The column value, or None if no results.
        """
        result = self.select(column).first()
        if result is None:
            return None
        if isinstance(result, dict):
            return result.get(column)
        return getattr(result, column, None)

    def pluck(self, column: str, key_by: Optional[str] = None):
        """Get a Collection containing the values of a given column.

        Like Laravel's pluck(), returns a flat list of column values,
        or a dict keyed by another column.

        Args:
            column: The column to pluck values from.
            key_by: Optional column to use as dictionary keys.

        Returns:
            Collection -- A collection of values (or keyed dict).

        Example:
            names = User.where('active', True).pluck('name')
            # Collection(['Alice', 'Bob', 'Charlie'])

            users = User.pluck('name', 'id')
            # Collection({1: 'Alice', 2: 'Bob'})
        """
        if key_by:
            results = self.select(column, key_by).get()
        else:
            results = self.select(column).get()

        if not results:
            return Collection()

        if key_by:
            plucked = {}
            for item in results:
                if isinstance(item, dict):
                    plucked[item.get(key_by)] = item.get(column)
                else:
                    plucked[getattr(item, key_by, None)] = getattr(item, column, None)
            return Collection(plucked)

        values = []
        for item in results:
            if isinstance(item, dict):
                values.append(item.get(column))
            else:
                values.append(getattr(item, column, None))
        return Collection(values)

    def chunk(self, chunk_size: int, callback: Callable):
        """Process the results in chunks (Laravel-style).

        The callback receives each chunk as a Collection. Return False
        from the callback to stop processing further chunks.

        Args:
            chunk_size: Number of records per chunk.
            callback: Function that receives each chunk Collection.

        Returns:
            bool -- True if all chunks were processed.

        Example:
            def process(chunk):
                for product in chunk:
                    product.update({'processed': True})

            Product.active().chunk(200, process)
        """
        page = 1
        while True:
            offset = (page - 1) * chunk_size
            builder = self.clone()
            results = builder.limit(chunk_size).offset(offset).get()

            if not results or (hasattr(results, 'is_empty') and results.is_empty()):
                break

            result = callback(results)

            if result is False:
                return False

            count = len(results) if hasattr(results, '__len__') else results.count()
            if count < chunk_size:
                break

            page += 1

        return True

    def upsert(
        self,
        values: List[Dict[str, Any]],
        unique_by: List[str],
        update: Optional[List[str]] = None,
        cast: bool = True,
    ):
        """
        Insert new records or update existing ones (Laravel-style upsert).

        Args:
            values: List of dictionaries with data to insert/update
            unique_by: List of column names that determine uniqueness
            update: List of column names to update on conflict (if None, updates all except unique_by)
            cast: Whether to apply model casts

        Returns:
            Number of affected rows

        Example:
            Receipt.upsert([
                {"receipt_id": "123", "status": "processed", "amount": 100},
                {"receipt_id": "124", "status": "pending", "amount": 200}
            ], unique_by=["receipt_id"], update=["status", "amount", "updated_at"])
        """
        self.set_action("upsert")
        model = None

        if self._model:
            model = self._model

        # Process and validate input data
        self._upsert_values = []
        for record in values:
            if model:
                # Apply mass assignment protection
                record = model.filter_mass_assignment(record)
                # Apply casts if requested
                if cast:
                    record = model.cast_values(record)

            # Sort the dict by key for consistent column order
            self._upsert_values.append(dict(sorted(record.items())))

        # Store upsert configuration
        self._upsert_unique_by = unique_by

        # If update columns not specified, update all columns except unique_by and timestamps
        if update is None:
            if self._upsert_values:
                all_columns = set(self._upsert_values[0].keys())
                exclude_columns = set(unique_by)

                # Don't auto-update created_at, but do update updated_at
                if model and hasattr(model, "date_created_at"):
                    exclude_columns.add(model.date_created_at)

                self._upsert_update = list(all_columns - exclude_columns)
            else:
                self._upsert_update = []
        else:
            self._upsert_update = update

        # Add timestamps if model supports them
        if model and hasattr(model, "__timestamps__") and model.__timestamps__:
            timestamp_value = model.get_new_date().to_datetime_string()

            # Add created_at and updated_at to all records
            for record in self._upsert_values:
                if model.date_created_at not in record:
                    record[model.date_created_at] = timestamp_value
                if model.date_updated_at not in record:
                    record[model.date_updated_at] = timestamp_value

            # Ensure updated_at is in the update list
            if model.date_updated_at not in self._upsert_update:
                self._upsert_update.append(model.date_updated_at)

        if not self.dry:
            connection = self.new_connection()
            query_result = connection.query(self.to_qmark(), self._bindings, results=1)

            # PostgreSQL returns number of affected rows
            return (
                query_result
                if isinstance(query_result, int)
                else len(self._upsert_values)
            )

        return len(self._upsert_values)

    def bulk_update(
        self,
        records: List[Dict[str, Any]],
        key: str = "id",
        update_columns: Optional[List[str]] = None,
    ):
        """Bulk update multiple records in a single query using PostgreSQL VALUES + UPDATE FROM.

        Args:
            records: List of dicts, each must contain the key column
            key: Column to match records on (default: "id")
            update_columns: Columns to update (if None, updates all except key)

        Returns:
            Number of affected rows

        Example:
            Product.bulk_update([
                {"id": 1, "price": 9.99, "status": "active"},
                {"id": 2, "price": 19.99, "status": "inactive"},
            ], key="id", update_columns=["price", "status"])
        """
        if not records:
            return 0

        # Determine columns to update
        if update_columns is None:
            update_columns = [k for k in records[0].keys() if k != key]

        if not update_columns:
            return 0

        # Build VALUES clause
        all_columns = [key] + update_columns
        placeholders = []
        bindings = []
        for record in records:
            row_placeholders = []
            for col in all_columns:
                bindings.append(record.get(col))
                row_placeholders.append("%s")
            placeholders.append(f"({', '.join(row_placeholders)})")

        values_clause = ', '.join(placeholders)
        col_defs = ', '.join(f'"{c}"' for c in all_columns)
        set_clause = ', '.join(f'"{c}" = _bulk."{c}"' for c in update_columns)
        table = self._table.name if hasattr(self._table, 'name') else str(self._table)

        sql = f'''
            UPDATE "{table}" SET {set_clause}
            FROM (VALUES {values_clause}) AS _bulk({col_defs})
            WHERE "{table}"."{key}" = _bulk."{key}"
        '''

        connection = self.new_connection()
        return connection.query(sql, tuple(bindings))

    def cursor(self, chunk_size: int = 1000):
        """
        Stream results from the database using a cursor for memory-efficient iteration.

        This method processes large datasets without loading everything into memory.
        It yields individual model instances one by one.

        Args:
            chunk_size: Number of records to fetch in each batch (default: 1000)

        Yields:
            Model: Individual model instances

        Example:
            # Memory-efficient processing of large datasets
            for user in User.where('active', True).cursor():
                process_user(user)

            # Process with custom chunk size
            for receipt in Receipt.cursor(chunk_size=500):
                process_receipt(receipt)
        """
        # Use offset-based pagination for cursor
        offset = 0

        while True:
            # Create a CLEAN copy of current builder with all constraints
            chunk_builder = QueryBuilder(
                grammar=self.grammar,
                connection_class=self.connection_class,
                connection=self.connection,
                connection_driver=self._connection_driver,
                model=self._model,
            )

            # Copy table
            chunk_builder._table = self._table

            # Copy ALL query constraints (this is the key fix!)
            chunk_builder._wheres = tuple(self._wheres) if self._wheres else ()
            chunk_builder._columns = tuple(self._columns) if self._columns else ()
            chunk_builder._order_by = tuple(self._order_by) if self._order_by else ()
            chunk_builder._group_by = tuple(self._group_by) if self._group_by else ()
            chunk_builder._having = tuple(self._having) if self._having else ()
            chunk_builder._joins = tuple(self._joins) if self._joins else ()
            chunk_builder._distinct = self._distinct
            chunk_builder._aggregates = (
                tuple(self._aggregates) if self._aggregates else ()
            )

            # Copy eager loading settings
            chunk_builder._eager_relation = self._eager_relation

            # Apply chunk-specific limit and offset
            chunk_builder._limit = chunk_size
            chunk_builder._offset = offset

            # Generate query and execute (chunk_builder is independent)
            query = chunk_builder.to_qmark()
            bindings = chunk_builder._bindings.copy()

            chunk_result = chunk_builder.new_connection().query(query, bindings) or []

            # If no more results, break the loop
            if not chunk_result:
                break

            # Process each record in the chunk
            for record in chunk_result:
                # Use chunk_builder for model hydration to maintain eager loading
                model_instance = chunk_builder.prepare_result(record, collection=False)
                yield model_instance

            # Move to next chunk
            offset += chunk_size

            # If we got less than chunk_size, we've reached the end
            if len(chunk_result) < chunk_size:
                break
