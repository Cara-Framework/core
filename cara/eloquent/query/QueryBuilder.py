from __future__ import annotations

import inspect
import json
import logging
import re
from collections.abc import Callable
from copy import deepcopy
from datetime import datetime
from typing import Any

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401

# ``order_by`` SQL injection guard: accept ``column`` or
# ``table.column`` identifiers only. Anything fancier (functions,
# expressions, NULLS FIRST, ...) must use ``order_by_raw`` where the
# caller takes responsibility.
_ORDER_BY_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")

from cara.eloquent.expressions import (
    AggregateExpression,
    BetweenExpression,
    F,
    FromTable,
    Greatest,
    GroupByExpression,
    HavingExpression,
    JoinClause,
    Least,
    Operation,
    OrderByExpression,
    QueryExpression,
    SelectExpression,
    SubGroupExpression,
    SubSelectExpression,
    UpdateQueryExpression,
)


def _is_column_expression(value) -> bool:
    """True if ``value`` is a column-reference expression node
    (``F`` / ``Operation`` / ``Greatest`` / ``Least``).

    These render to quoted SQL identifiers — never bound values — so the
    update/where compilers branch on them to skip casting, change-detection,
    and parameter binding. Mirrors ``BaseGrammar.is_column_expression``; kept
    here too so QueryBuilder need not reach into the grammar for the check.
    """
    return isinstance(value, (F, Operation, Greatest, Least))
from cara.exceptions import (
    Http404Exception,
    InvalidArgumentException,
    ModelNotFoundException,
    MultipleRecordsFoundException,
    QueryException,
)
from cara.support import Collection

from ..observers import ObservesEvents
from ..pagination import LengthAwarePaginator, SimplePaginator
from ..schema import Schema
from ..scopes import BaseScope
from .EagerRelation import EagerRelations

_logger = logging.getLogger("cara.eloquent.query")


class TransactionContext:
    """Context manager for database transactions."""

    def __init__(self, builder):
        self.builder = builder

    def __enter__(self):
        self.builder.begin()
        return self.builder

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            try:
                self.builder.rollback()
            except Exception as rollback_exc:
                # Chain so the original exception isn't masked.
                raise rollback_exc from exc_val
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
        grammar: Any = None,
        connection: Any = None,
        connection_class: type | None = None,
        table: str | None = None,
        connection_details: dict[str, Any] | None = None,
        connection_driver: str | None = None,
        model: Any = None,
        scopes: dict[str, Callable] | None = None,
        schema: str | None = None,
        dry: bool = False,
        config_path: str | None = None,
    ) -> None:
        """QueryBuilder initializer.

        Arguments:
            grammar -- A grammar class.
            connection -- A connection class.
            table -- the name of the table.
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
        self._lock_modifier = {"skip_locked": False, "nowait": False, "of": []}
        self._schema = schema
        self._eager_relation = EagerRelations()
        if model:
            # ROOT CAUSE (2026-04-24): previously this was
            # ``self._global_scopes = model._global_scopes`` — a shared
            # reference to the class-level dict. Any callback that ran
            # ``remove_global_scope()`` (notably SoftDeleteScope's
            # ``_soft_delete_query``) mutated the class dict forever,
            # so a single ``.delete()`` would strip the soft-delete
            # scope class-wide and every subsequent delete hard-
            # deleted rows + their FK-cascade dependents. Snapshot to
            # a per-builder copy so scope mutations are scoped to this
            # query only. Shallow copy of both layers is enough; the
            # inner values are callables we never rewrite.
            self._global_scopes = {
                action: dict(scopes) for action, scopes in model._global_scopes.items()
            }
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
        # Seed macros from the model so every REBUILT builder carries the
        # convenience methods (with_trashed / only_trashed / restore /
        # force_delete, …) registered during the model's boot — the macro
        # counterpart of the ``_global_scopes`` copy above. The boot-time
        # builder (model snapshots its macros only AFTER boot wiring finishes)
        # and model-less builders fall back to empty.
        self._macros = dict(getattr(model, "_macros", None) or {}) if model else {}

        self._aggregates = ()
        # Unions registered via union()/union_all() — list of (builder, all)
        self._unions = []

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

    def set_schema(self, schema) -> Self:
        self._schema = schema
        return self

    def shared_lock(self):
        return self.make_lock("share")

    def lock_for_update(
        self,
        skip_locked: bool = False,
        nowait: bool = False,
        of=None,
    ) -> Self:
        """Acquire a ``FOR UPDATE`` row lock, optionally with modifiers.

        Keyword Arguments:
            skip_locked -- emit ``FOR UPDATE SKIP LOCKED`` (rows currently
                locked by another transaction are skipped instead of waited
                on). Supported by PostgreSQL.
            nowait -- emit ``FOR UPDATE NOWAIT`` (fail immediately instead of
                blocking if a row is already locked). Mutually exclusive with
                ``skip_locked``.
            of -- a table name or list of table names for ``FOR UPDATE OF
                <table>`` — restricts the lock to rows from those tables in a
                joined query (Postgres). Names are quoted as identifiers.

        Example::

            Job.where("status", "queued").lock_for_update(skip_locked=True).first()
            # ... FOR UPDATE SKIP LOCKED
        """
        if skip_locked and nowait:
            raise InvalidArgumentException(
                "lock_for_update: skip_locked and nowait are mutually exclusive."
            )
        return self.make_lock(
            "update", skip_locked=skip_locked, nowait=nowait, of=of
        )

    def make_lock(
        self,
        lock,
        skip_locked: bool = False,
        nowait: bool = False,
        of=None,
    ) -> Self:
        self.lock = lock
        # Modifiers ride alongside the base lock key so the existing
        # share/update map stays untouched; the grammar appends them.
        if of is None:
            of_tables = []
        elif isinstance(of, str):
            of_tables = [of]
        else:
            of_tables = list(of)
        self._lock_modifier = {
            "skip_locked": skip_locked,
            "nowait": nowait,
            "of": of_tables,
        }
        return self

    def reset(self) -> Self:
        """Resets the query builder instance so you can make multiple calls with the same builder
        instance.

        ROOT-CAUSE (2026-06 extractor audit): pre-fix this method
        omitted ``_limit``, ``_offset``, ``_distinct``, and
        ``_columns``, so a ``first()`` (which sets ``limit(1)``)
        followed by any reuse of the same builder silently carried
        ``LIMIT 1`` into the next query even though all WHERE /
        ORDER BY clauses were wiped.  The most visible symptom was
        a two-step "primary → fallback" lookup that reused one
        builder: the first ``.first()`` wiped ``_wheres`` but left
        ``_limit = 1``, so the second query — which rebuilt its
        WHEREs via a fresh ``_base()`` call — accidentally kept the
        stale limit (harmless in that case but semantically wrong
        for any caller that expected ``reset()`` to truly reset).
        Adding these fields completes the contract: after
        ``reset()`` the builder is indistinguishable from a freshly
        constructed one (minus table/model/connection bindings)."""

        self.set_action("select")

        self._updates = ()

        self._wheres = ()
        self._order_by = ()
        self._group_by = ()
        self._joins = ()
        self._having = ()
        self._aggregates = ()

        self._limit = False
        self._offset = False
        self._distinct = False
        self._columns = ()

        # Same contract, second pass (2026-07 framework audit): these
        # four also leaked across reuses — a lock_for_update().first()
        # left every later query compiling FOR UPDATE, a union() query
        # re-appended its stale UNION, and _creates poisoned the next
        # SELECT's column list.
        self._creates = {}
        self._unions = []
        self.lock = False
        self._lock_modifier = {"skip_locked": False, "nowait": False, "of": []}

        return self

    def get_connection_information(self):
        """Get connection info from DatabaseManager"""
        return self._db_manager.get_connection_info(self.connection)

    def table(self, table, raw=False) -> Self:
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
        if not hasattr(self, "_connection") or self._connection is None:
            raise QueryException("No active transaction to commit.")
        return self._connection.commit()

    def rollback(self) -> Self:
        """Roll back the active database transaction."""
        if not hasattr(self, "_connection") or self._connection is None:
            raise QueryException("No active transaction to roll back.")
        self._connection.rollback()
        return self

    def transaction(self, callback=None):
        """Execute code within a database transaction.

        Can be used as a context manager or with a callback.

        Example (context manager):
            with Model.query().transaction() as trx:
                record = Model.create({...})
                RelatedModel.create({...})

        Example (callback):
            Model.query().transaction(lambda: [
                Model.create({...}),
                RelatedModel.create({...}),
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

    def set_scope(self, name, callable) -> Self:
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

    def set_global_scope(self, name="", callable=None, action="select") -> Self:
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

    def without_global_scopes(self) -> Self:
        self._global_scopes = {}
        return self

    def remove_global_scope(self, scope, action=None) -> Self:
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
            raise AttributeError(f"'QueryBuilder' object has no attribute '{attribute}'")

        if attribute in self._scopes:

            def method(*args, **kwargs):
                return self._scopes[attribute](self._model, self, *args, **kwargs)

            return method

        if attribute in self._macros:

            def method(*args, **kwargs):
                return self._macros[attribute](self._model, self, *args, **kwargs)

            return method

        raise AttributeError(f"'QueryBuilder' object has no attribute '{attribute}'")

    def on(self, connection) -> Self:
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
            self.connection_class = self._db_manager.get_connection_class(self.connection)
            self.grammar = self._db_manager.get_grammar(self.connection)

        return self

    def select(self, *args) -> Self:
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

    def distinct(self, boolean=True) -> Self:
        """
        Specifies that all columns should be distinct.

        Returns:
            self
        """
        self._distinct = boolean
        return self

    def add_select(self, alias, callable) -> Self:
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
        # Non-result statements (UPDATE/DELETE/INSERT without RETURNING)
        # come back as the affected row count — hand it through as-is.
        # prepare_result would try to hydrate the int into a model on
        # model-bound builders.
        if isinstance(result, int):
            return result
        return self.prepare_result(result)

    def select_raw(self, query) -> Self:
        """
        Specifies raw SQL that should be injected into the select expression.

        Returns:
            self
        """
        self._columns += (SelectExpression(query, raw=True),)
        return self

    # ── identifier-quoting helper for builder-time SQL fragments ─────────
    # ``select_window`` / ``select_greatest`` / ``select_least`` assemble
    # their SQL at call time (not via the column pipeline), so they need the
    # grammar's identifier quoting. ``self.grammar`` is the grammar CLASS;
    # a bare instance is enough to reach ``_table_column_string`` /
    # ``compile_expression`` without touching connection state.

    def _rendering_grammar(self):
        """Return a transient grammar instance for quoting identifiers.

        Seeded with this builder's table + connection details so qualified
        column references resolve to the RIGHT table (not the grammar's
        ``"users"`` default) and the dialect's quote chars / prefix apply.
        """
        return self.grammar(
            table=self._table,
            connection_details=self._connection_details,
        )

    def _quote_window_identifier(self, column: str) -> str:
        """Quote a window PARTITION BY / ORDER BY identifier safely.

        Reuses the SAME ``order_by`` injection guard as the rest of the
        builder: only ``name`` / ``table.column`` identifiers are allowed.
        Anything fancier (functions, expressions) is rejected — callers
        wanting raw SQL there should build it with ``select_raw``.
        """
        col = column.strip()
        if not _ORDER_BY_COLUMN_RE.match(col):
            raise InvalidArgumentException(
                f"Invalid window identifier {column!r}. "
                f"Expected ``name`` or ``table.column``; use ``select_raw`` "
                f"for arbitrary expressions."
            )
        return self._rendering_grammar()._table_column_string(col, separator="")

    def select_window(
        self,
        expression: str,
        *,
        partition_by=None,
        order_by=None,
        alias: str = "rn",
    ) -> Self:
        """Add a window-function column:
        ``expression OVER (PARTITION BY ... ORDER BY ...) AS alias``.

        Arguments:
            expression -- the window function call, e.g. ``"ROW_NUMBER()"``,
                ``"RANK()"``, ``"LAG(price)"``. Passed through verbatim
                (caller owns its correctness — it is a function call, not a
                request-supplied value).

        Keyword Arguments:
            partition_by -- a column name or list of column names for the
                ``PARTITION BY`` clause. Each is quoted as an identifier.
            order_by -- a column name, a list of column names, or a list of
                ``(column, direction)`` pairs for the ``ORDER BY`` clause.
                Direction must be ASC/DESC; columns are quoted as identifiers.
            alias -- the output column alias (default ``"rn"``).

        Example::

            Listing.select("*").select_window(
                "ROW_NUMBER()",
                partition_by=["product_id"],
                order_by=[("price_low", "asc")],
                alias="rn",
            )
            # SELECT *, ROW_NUMBER() OVER (
            #     PARTITION BY "product_id" ORDER BY "price_low" ASC
            # ) AS "rn" FROM ...
        """
        clauses = []

        if partition_by:
            cols = [partition_by] if isinstance(partition_by, str) else list(partition_by)
            quoted = ", ".join(self._quote_window_identifier(c) for c in cols)
            clauses.append(f"PARTITION BY {quoted}")

        if order_by:
            order_specs = [order_by] if isinstance(order_by, str) else list(order_by)
            rendered = []
            for spec in order_specs:
                if isinstance(spec, (list, tuple)):
                    col, direction = spec[0], (spec[1] if len(spec) > 1 else "ASC")
                else:
                    col, direction = spec, "ASC"
                dir_str = (direction or "ASC").upper()
                if dir_str not in ("ASC", "DESC"):
                    raise InvalidArgumentException(
                        f"Invalid window order direction {direction!r}; expected ASC or DESC"
                    )
                rendered.append(f"{self._quote_window_identifier(col)} {dir_str}")
            clauses.append("ORDER BY " + ", ".join(rendered))

        over = f" {' '.join(clauses)} " if clauses else ""
        quoted_alias = self._rendering_grammar().column_string().format(
            column=alias, separator=""
        )
        self._columns += (
            SelectExpression(
                f"{expression} OVER ({over.strip()}) AS {quoted_alias}", raw=True
            ),
        )
        return self

    def select_greatest(self, *columns, alias: str | None = None) -> Self:
        """Add a ``GREATEST(...)`` SELECT column (mirrors ``select_if_null``).

        Each argument may be a column name (string — quoted as an
        identifier), an ``F`` reference, or a literal expression node.

        Example::

            q.select_greatest("price_low", "floor_price", alias="effective_low")
            # SELECT GREATEST("price_low", "floor_price") AS "effective_low"
        """
        return self._select_function_expression(Greatest, columns, alias)

    def select_least(self, *columns, alias: str | None = None) -> Self:
        """Add a ``LEAST(...)`` SELECT column (mirrors ``select_if_null``).

        Example::

            q.select_least("price_high", "ceiling_price", alias="effective_high")
            # SELECT LEAST("price_high", "ceiling_price") AS "effective_high"
        """
        return self._select_function_expression(Least, columns, alias)

    def _select_function_expression(self, func_cls, columns, alias) -> Self:
        """Shared body for ``select_greatest`` / ``select_least``.

        Coerces bare string column names to ``F`` references (so they are
        quoted as identifiers, NOT escaped as string literals) and renders
        the function via the grammar's expression compiler.
        """
        args = [c if _is_column_expression(c) else F(c) for c in columns]
        sql = self._rendering_grammar().compile_expression(func_cls(*args))
        if alias:
            quoted_alias = self._rendering_grammar().column_string().format(
                column=alias, separator=""
            )
            sql += f" AS {quoted_alias}"
        self._columns += (SelectExpression(sql, raw=True),)
        return self

    def get_processor(self):
        return self.connection_class.get_default_post_processor()()

    def bulk_create(
        self,
        creates: list[dict[str, Any]],
        query: bool = False,
        cast: bool = True,
    ):
        self.set_action("bulk_create")
        model = None

        if self._model:
            model = self._model

        # First pass: filter / cast each row, but DO NOT sort yet — we
        # need a single canonical column order across all rows so the
        # generated INSERT (column1, column2, …) VALUES (...), (...)
        # actually aligns. The previous implementation sorted each row
        # independently, then BaseGrammar took columns from row[0]
        # only — so heterogeneous rows ({a,b} mixed with {a,c}) silently
        # corrupted: row 2's value for column "c" landed in column "b".
        prepared: list[dict[str, Any]] = []
        column_set: set = set()
        for unsorted_create in creates:
            if model:
                unsorted_create = model.filter_mass_assignment(unsorted_create)
            if cast and model:
                unsorted_create = model.cast_values(unsorted_create)
            prepared.append(unsorted_create)
            column_set.update(unsorted_create.keys())

        # Canonical sorted column list. Missing keys in a row are
        # filled with ``None`` so every row has the same shape under
        # the generated INSERT.
        all_columns = sorted(column_set)
        self._creates = [{col: row.get(col) for col in all_columns} for row in prepared]

        if query:
            return self

        if model:
            model = model.hydrate(self._creates)
        if not self.dry:
            connection = self.new_connection()
            # to_qmark() resets the builder (including _creates); keep the
            # payload for the no-RETURNING fallback.
            creates = self._creates
            query_result = connection.query(self.to_qmark(), self._bindings, results=1)

            processed_results = query_result or creates
        else:
            processed_results = self._creates

        if model:
            return model

        return processed_results

    def create(
        self,
        creates: dict[str, Any] | None = None,
        query: bool = False,
        id_key: str = "id",
        cast: bool = True,
        ignore_mass_assignment: bool = False,
        **kwargs: Any,
    ) -> Any:
        """
        Create a new record from the given dictionary of values.

        Arguments:
            creates {dict} -- A dictionary of columns and values.

        Returns:
            Model instance (when bound to a model) or raw insert result.
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

            # to_qmark() resets the builder (including _creates) once the
            # grammar has been compiled — snapshot the payload first so the
            # no-RETURNING fallback below still has the inserted values.
            creates = self._creates
            query_result = connection.query(self.to_qmark(), self._bindings, results=1)

            if model:
                id_key = model.get_primary_key()

            processed_results = self.get_processor().process_insert_get_id(
                self,
                query_result or creates,
                id_key,
            )
        else:
            processed_results = self._creates

        if model:
            model = model.fill(processed_results)
            self.observe_events(model, "created")
            return model

        return processed_results

    def hydrate(self, result: Any, relations: list[str] | None = None) -> Any:
        return self._model.hydrate(result, relations)

    def delete(self, column: str | None = None, value: Any = None, query: bool = False) -> Self | int:
        """
        Delete rows matching a WHERE clause, or by column/value.

        Keyword Arguments:
            column -- The name of the column (default: {None})
            value -- The value of the column (default: {None})
            query -- If True, return the builder instead of executing.

        Returns:
            Row count affected, or self if query=True.
        """
        model = None
        self.set_action("delete")

        if self._model:
            model = self._model

        # ``value is not None`` — a falsy filter value (0, False, "") is a
        # legitimate predicate; truthiness silently dropped it and the
        # no-WHERE safety net below turned the call into a QueryException.
        if column and value is not None:
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

        # Safety: refuse to execute DELETE without a WHERE clause to
        # prevent accidental mass-deletion.  Use truncate() instead.
        if not self._wheres:
            from cara.exceptions import QueryException

            raise QueryException(
                "delete() without a WHERE clause would remove all rows. "
                "Use truncate() for intentional mass-deletion."
            )

        result = self.new_connection().query(self.to_qmark(), self._bindings)

        if model:
            self.observe_events(model, "deleted")

        return result

    def where(self, column, *args) -> Self:
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

        if _is_column_expression(column) or _is_column_expression(value):
            # ``where(F("a"), ">", F("b"))`` (or a literal on either side):
            # both sides are rendered by the grammar's expression compiler so
            # column references stay quoted identifiers and literals are
            # escaped as values — never a bound %s for the expression side.
            self._wheres += (
                (
                    QueryExpression(
                        column,
                        operator,
                        value,
                        "expression",
                    )
                ),
            )
        elif inspect.isfunction(column):
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

    def where_from_builder(self, builder) -> Self:
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

    def where_raw(self, query: str, bindings=()) -> Self:
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

    def or_where_raw(self, query: str, bindings=()) -> Self:
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
    # since callers often use qualified names like "b.aliases" or "t.metadata").
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
            Model.where_json_contains("aliases", ["foo"])
            # -> aliases @> '["foo"]'::jsonb

            Model.where_json_contains("metadata", {"featured": True})
            # -> metadata @> '{"featured": true}'::jsonb
        """
        return self.where_raw(f"{column} @> %s::jsonb", [json.dumps(value)])

    def or_where_json_contains(self, column: str, value):
        """OR-joined variant of where_json_contains."""
        return self.or_where_raw(f"{column} @> %s::jsonb", [json.dumps(value)])

    def where_json_doesnt_contain(self, column: str, value):
        """Inverse of where_json_contains."""
        return self.where_raw(f"NOT ({column} @> %s::jsonb)", [json.dumps(value)])

    def where_json_path(self, column: str, path, operator: str = "=", value=None):
        """
        Filter by a nested JSON path extract.

        Arguments:
            column {string} -- The JSON column, e.g. "metadata".
            path {string|list} -- Dotted string ("details.amount") or a list
                of segments. Each segment is treated as a key name, not an array index.
            operator {string} -- SQL comparison operator ("=", ">=", "!=", "LIKE", ...).
            value -- The value to compare against (bound safely).

        Example:
            q.where_json_path("metadata", "external_order_id", "=", "ord_123")
            # -> metadata->>'external_order_id' = %s
        """
        # Two-arg form: where_json_path(column, path, value) with operator defaulted to "="
        if value is None and operator not in (
            "=",
            "!=",
            "<>",
            ">",
            ">=",
            "<",
            "<=",
            "LIKE",
            "ILIKE",
            "NOT LIKE",
            "NOT ILIKE",
        ):
            value = operator
            operator = "="
        sql_col = self._json_path_sql(column, path)
        return self.where_raw(f"{sql_col} {operator} %s", [value])

    def or_where_json_path(self, column: str, path, operator: str = "=", value=None):
        """OR-joined variant of where_json_path."""
        if value is None and operator not in (
            "=",
            "!=",
            "<>",
            ">",
            ">=",
            "<",
            "<=",
            "LIKE",
            "ILIKE",
            "NOT LIKE",
            "NOT ILIKE",
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
        return self.where_raw(f"jsonb_array_length({column}) {operator} %s", [val])

    def where_json_key_exists(self, column: str, key: str):
        """
        Filter rows whose jsonb object contains the given top-level key (Postgres ?).

        Example:
            q.where_json_key_exists("metadata", "featured")
            # -> metadata ? %s    (bound: 'featured')

        ``key`` is bound as a parameter — never interpolated — so user-supplied
        keys cannot escape out of the SQL string literal.
        """
        return self.where_raw(f"{column} ? %s", [key])

    def or_where(self, column, *args) -> Self:
        """
        Specifies an or where query expression.

        Arguments:
            column {[type]} -- [description]
            value {[type]} -- [description]

        Returns:
            [type] -- [description]
        """
        operator, value = self._extract_operator_value(*args)
        if _is_column_expression(column) or _is_column_expression(value):
            # OR-joined column-reference comparison — see ``where`` for the
            # expression-rendering rationale.
            self._wheres += (
                (
                    QueryExpression(
                        column,
                        operator,
                        value,
                        "expression",
                        keyword="or",
                    )
                ),
            )
        elif inspect.isfunction(column):
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
                        keyword="or",
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

    def where_exists(self, value: str | int | QueryBuilder) -> Self:
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

    def or_where_exists(self, value: str | int | QueryBuilder) -> Self:
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

    def where_not_exists(self, value: str | int | QueryBuilder) -> Self:
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

    def or_where_not_exists(self, value: str | int | QueryBuilder) -> Self:
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

    def having(self, column, equality="", value="") -> Self:
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

    def having_raw(self, string) -> Self:
        """
        Specifies raw SQL that should be injected into the having expression.

        Arguments:
            string {string} -- The raw query string.

        Returns:
            self
        """
        self._having += ((HavingExpression(string, raw=True)),)
        return self

    def where_null(self, column) -> Self:
        """
        Specifies a where expression where the column is NULL.

        Arguments:
            column {string} -- The name of the column.

        Returns:
            self
        """
        self._wheres += ((QueryExpression(column, "=", None, "NULL")),)
        return self

    def or_where_null(self, column) -> Self:
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

    def where_not_null(self, column: str) -> Self:
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
            return date.strftime("%Y-%m-%d")

    def where_date(self, column: str, date: str | datetime) -> Self:
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

    def or_where_date(self, column: str, date: str | datetime) -> Self:
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

    def between(self, column: str, low: int, high: int) -> Self:
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

    def not_between(self, column: str, low: str, high: str) -> Self:
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

    def where_in(self, column, wheres=None) -> Self:
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
            # Drop None values. ``IN (NULL, …)`` is never true in standard
            # SQL (NULL ≠ NULL), and the grammar would otherwise splice
            # the Python literal ``'None'`` as a string — a silent type
            # mismatch that always returns zero rows. If every value was
            # None, collapse to the same "match nothing" sentinel as the
            # empty-list branch instead of emitting bogus SQL.
            cleaned = [v for v in wheres if v is not None]
            if not cleaned:
                self._wheres += ((QueryExpression(0, "=", 1, "value_equals")),)
            else:
                self._wheres += ((QueryExpression(column, "IN", cleaned)),)
        return self

    def get_relation(self, relationship, builder=None):
        if not builder:
            builder = self

        if not builder._model:
            raise AttributeError(
                "You must specify a model in order to use relationship methods"
            )

        # ``builder._model`` may be an unhydrated instance — in that case
        # ``getattr(instance, rel)`` triggers the descriptor's instance-path
        # (lazy-load from ``__attributes__``) and KeyErrors on the local key.
        # Resolve via the descriptor on the class (walking the MRO).
        import inspect as _inspect

        owner = (
            builder._model if _inspect.isclass(builder._model) else type(builder._model)
        )
        rel = owner.__dict__.get(relationship)
        if rel is None:
            for base in owner.__mro__:
                if relationship in base.__dict__:
                    rel = base.__dict__[relationship]
                    break
        if rel is None:
            raise AttributeError(
                f"Relation '{relationship}' is not defined on {owner.__name__}"
            )
        return rel

    def has(self, *relationships) -> Self:
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
                related = self._resolve_relation_descriptor(relationship)
                related.query_has(self)
        return self

    def or_has(self, *relationships) -> Self:
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
                related = self._resolve_relation_descriptor(relationship)
                related.query_has(self, method="or_where_exists")
        return self

    def doesnt_have(self, *relationships) -> Self:
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
                related = self._resolve_relation_descriptor(relationship)
                related.query_has(self, method="where_not_exists")
        return self

    def or_doesnt_have(self, *relationships) -> Self:
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
                related = self._resolve_relation_descriptor(relationship)
                related.query_has(self, method="or_where_not_exists")
        return self

    def where_has(self, relationship, callback) -> Self:
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
            related = self._resolve_relation_descriptor(relationship)
            related.query_where_exists(self, callback, method="where_exists")
        return self

    def or_where_has(self, relationship, callback) -> Self:
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
            related = self._resolve_relation_descriptor(relationship)
            related.query_where_exists(self, callback, method="or_where_exists")
        return self

    def where_doesnt_have(self, relationship, callback) -> Self:
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
                    last_builder = last_builder.get_relation(
                        split_relationship
                    ).query_where_exists(
                        last_builder,
                        callback,
                        method="where_not_exists",
                    )
                    continue

                last_builder = related.query_has(last_builder, method="where_not_exists")
        else:
            related = self._resolve_relation_descriptor(relationship)
            related.query_where_exists(self, callback, method="where_not_exists")
        return self

    def or_where_doesnt_have(self, relationship, callback) -> Self:
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
                    last_builder = last_builder.get_relation(
                        split_relationship
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
            related = self._resolve_relation_descriptor(relationship)
            related.query_where_exists(self, callback, method="or_where_not_exists")
        return self

    def with_count(self, *relationships, callback=None):
        """
        Add ``{relationship}_count`` to the selected columns for each
        relation. Laravel parity: accepts a single string, multiple
        positional strings, a list/tuple, or a dict of
        ``{relation: callback}`` for constrained counts.

        Examples::

            Post.with_count("comments")
            Post.with_count("comments", "likes")
            Post.with_count(["comments", "likes"])
            Post.with_count({"comments": lambda q: q.where("approved", True)})
        """
        if not relationships:
            return self

        # Flatten heterogeneous inputs to a list of ``(name, callback)``
        # pairs so callers can mix-and-match shapes.
        pairs = []

        def _push(spec, inherited_cb):
            if spec is None:
                return
            if isinstance(spec, str):
                pairs.append((spec, inherited_cb))
            elif isinstance(spec, (list, tuple, set)):
                for item in spec:
                    _push(item, inherited_cb)
            elif isinstance(spec, dict):
                for name, cb in spec.items():
                    if isinstance(name, str):
                        pairs.append((name, cb if callable(cb) else inherited_cb))

        for r in relationships:
            _push(r, callback)

        self.select(*self._model.get_selects())
        builder = self
        for name, cb in pairs:
            rel = self._resolve_relation_descriptor(name)
            builder = rel.get_with_count_query(builder, callback=cb, relation_name=name)
        return builder

    def _resolve_relation_descriptor(self, name):
        """Fetch the relationship *descriptor* (not an instance-level proxy).

        ``getattr(instance, rel_name)`` triggers HasMany/HasOne.__get__ on
        instance paths which lazy-loads; we need the raw descriptor for
        subquery building. Walk the MRO to find it.
        """
        import inspect as _inspect

        owner = self._model if _inspect.isclass(self._model) else type(self._model)
        rel = owner.__dict__.get(name)
        if rel is None:
            for base in owner.__mro__:
                if name in base.__dict__:
                    rel = base.__dict__[name]
                    break
        if rel is None:
            raise AttributeError(f"Relation '{name}' is not defined on {owner.__name__}")
        return rel

    def with_sum(self, relationship, column, callback=None):
        """Eager load a relationship's SUM aggregate.

        Adds {relationship}_{column}_sum attribute to each model.

        Example:
            Model.with_sum("items", "amount").get()
            # model.items_amount_sum = 150.00
        """
        self.select(*self._model.get_selects())
        return self._resolve_relation_descriptor(relationship).get_with_sum_query(
            self, column, callback=callback, relation_name=relationship
        )

    def with_avg(self, relationship, column, callback=None):
        """Eager load a relationship's AVG aggregate.

        Adds {relationship}_{column}_avg attribute to each model.

        Example:
            Model.with_avg("items", "amount").get()
            # model.items_amount_avg = 75.50
        """
        self.select(*self._model.get_selects())
        return self._resolve_relation_descriptor(relationship).get_with_avg_query(
            self, column, callback=callback, relation_name=relationship
        )

    def with_min(self, relationship, column, callback=None):
        """Eager load a relationship's MIN aggregate.

        Adds {relationship}_{column}_min attribute to each model.

        Example:
            Model.with_min("items", "amount").get()
            # model.items_amount_min = 10.00
        """
        self.select(*self._model.get_selects())
        return self._resolve_relation_descriptor(relationship).get_with_min_query(
            self, column, callback=callback, relation_name=relationship
        )

    def with_max(self, relationship, column, callback=None):
        """Eager load a relationship's MAX aggregate.

        Adds {relationship}_{column}_max attribute to each model.

        Example:
            Model.with_max("items", "amount").get()
            # model.items_amount_max = 200.00
        """
        self.select(*self._model.get_selects())
        return self._resolve_relation_descriptor(relationship).get_with_max_query(
            self, column, callback=callback, relation_name=relationship
        )

    def tap(self, callback) -> Self:
        """Execute callback with the builder and return the builder for chaining.

        Useful for debugging or side effects without breaking the chain.

        Example:
            Model.active().tap(lambda q: print(q.to_sql())).get()
        """
        callback(self)
        return self

    def pipe(self, callback):
        """Pass the builder to a callback and return the callback's result.

        Unlike tap(), pipe() returns what the callback returns.

        Example:
            result = Model.active().pipe(lambda q: q.count() > 0)
        """
        return callback(self)

    def where_not_in(self, column, wheres=None) -> Self:
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
            # Empty exclusion list is almost always a caller bug — e.g.
            # ``Model.where_not_in('id', external_ids).update({...})`` where
            # ``external_ids`` came back empty. Silently dropping the
            # clause would turn that into "update everything". Emit an
            # explicit always-true predicate so the SQL still reflects
            # intent ("nothing to exclude") and remains well-formed.
            self._wheres += (
                (
                    QueryExpression(
                        column="1 = 1",
                        equality="",
                        value=None,
                        value_type="RAW",
                        keyword="AND",
                        raw=True,
                    )
                ),
            )
        else:
            # Same defensive cleanup as ``where_in`` — drop None values
            # so we never emit literal ``NOT IN ('None', …)`` (which
            # would match every row in the table, the opposite of what
            # ``NOT IN (NULL, …)`` would actually evaluate to). If every
            # value was None, treat it as "nothing to exclude".
            cleaned = [v for v in wheres if v is not None]
            if not cleaned:
                self._wheres += (
                    (
                        QueryExpression(
                            column="1 = 1",
                            equality="",
                            value=None,
                            value_type="RAW",
                            keyword="AND",
                            raw=True,
                        )
                    ),
                )
            else:
                self._wheres += ((QueryExpression(column, "NOT IN", cleaned)),)
        return self

    def join(
        self,
        table: str,
        column1=None,
        equality=None,
        column2=None,
        clause="inner",
    ) -> Self:
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

    def joins(self, *relationships, clause="inner") -> Self:
        for relationship in relationships:
            getattr(self._model, relationship).joins(self, clause=clause)

        return self

    def join_on(self, relationship, callback=None, clause="inner") -> Self:
        relation = getattr(self._model, relationship)
        relation.joins(self, clause=clause)

        if callback:
            new_from_builder = self.new_from_builder()
            new_from_builder.table(relation.get_builder().get_table_name())
            self.where_from_builder(callback(new_from_builder))

        return self

    def where_column(self, column1, column2) -> Self:
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

    def limit(self, amount) -> Self:
        """
        Specifies a limit expression.

        Arguments:
            amount {int} -- The number of rows to limit. ``None`` clears
                the limit (same as never calling ``limit``). ``0`` means
                "return zero rows" and is honored; negative values are
                rejected.

        Returns:
            self
        """
        if amount is None:
            self._limit = False
        elif isinstance(amount, bool):
            # ``True``/``False`` would coerce to 1/0 silently — almost
            # always a caller mistake. ``False`` matches the sentinel
            # for "no limit", but pinning it here makes intent explicit.
            raise InvalidArgumentException(f"limit() expects an int or None, got {amount!r}")
        elif isinstance(amount, int):
            if amount < 0:
                raise InvalidArgumentException(f"limit() must be >= 0, got {amount!r}")
            self._limit = amount
        else:
            raise InvalidArgumentException(f"limit() expects an int or None, got {amount!r}")
        return self

    def offset(self, amount) -> Self:
        """
        Specifies an offset expression.

        Arguments:
            amount {int} -- The number of rows to skip. ``None`` clears
                the offset. Negative values are rejected by supported drivers.

        Returns:
            self
        """
        if amount is None:
            self._offset = False
        elif isinstance(amount, bool):
            raise InvalidArgumentException(f"offset() expects an int or None, got {amount!r}")
        elif isinstance(amount, int):
            if amount < 0:
                raise InvalidArgumentException(f"offset() must be >= 0, got {amount!r}")
            self._offset = amount
        else:
            raise InvalidArgumentException(f"offset() expects an int or None, got {amount!r}")
        return self

    def skip(self, *args, **kwargs):
        """Alias for limit method."""
        return self.offset(*args, **kwargs)

    def update(
        self,
        updates: dict[str, Any],
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
                # Filter updates to only those with changes. JSON / array /
                # collection casts are EXEMPT from change-detection: their cast
                # value is a mutable dict/list and ``getattr`` hands back the
                # very object stored in ``__original_attributes__`` (the two
                # alias after hydrate), so an in-place mutation makes
                # ``original != value`` compare an object against itself
                # (always False) and the write is SILENTLY dropped — real data
                # loss with no error (CODING_RULES §8). We cannot detect the
                # change reliably, so always persist these columns when they
                # are present in the update: re-writing an unchanged JSON value
                # is cheap; dropping a mutated one is a bug.
                _mutable_casts = ("json", "array", "collection")
                updates = {
                    attr: value
                    for attr, value in updates.items()
                    if (
                        value is None
                        # Column-reference expressions (F / arithmetic /
                        # GREATEST / LEAST) are computed server-side from the
                        # CURRENT row, so there is no Python value to compare
                        # against ``__original_attributes__`` — never drop
                        # them via change-detection.
                        or _is_column_expression(value)
                        or model.__casts__.get(attr) in _mutable_casts
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
                #
                # Column-reference expressions are NOT data: the grammar
                # renders them as quoted SQL, so casting (json.dumps etc.)
                # would corrupt them. Leave them untouched.
                if cast and not _is_column_expression(value):
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

        # Safety: refuse to execute UPDATE without a WHERE clause to
        # prevent accidental mass-mutation.  ``delete()`` has the same
        # guard. If a caller's where-list reduced to empty (e.g. an
        # external id list came back empty and ``where_in`` collapsed
        # to a no-op), this catches the foot-gun before the query runs.
        if not self._wheres:
            from cara.exceptions import QueryException

            raise QueryException(
                "update() without a WHERE clause would modify all rows. "
                "Use ``where_raw('1 = 1')`` to opt in explicitly, "
                "or build the update on a loaded model instance."
            )

        # Column-reference expressions are computed by the database from the
        # CURRENT row — there is no concrete Python value to write back onto
        # the model. Strip them from the dict used to refresh in-memory state
        # so the model never carries a stale ``F`` object as an attribute.
        # (The SQL itself still updates the column server-side.)
        materialized = {
            key: value
            for key, value in updates.items()
            if not _is_column_expression(value)
        }

        additional.update(materialized)

        result = self.new_connection().query(self.to_qmark(), self._bindings)
        if model:
            model.fill(materialized)
            self.observe_events(model, "updated")
            model.fill_original(materialized)
            return model
        # Laravel parity: a non-model (table-level) update returns the
        # affected row count. Queue/outbox CAS transitions depend on it —
        # the old ``additional`` dict return was always truthy, so a losing
        # CAS (0 rows matched) still claimed the job.
        return result

    def force_update(self, updates: dict, dry=False):
        return self.update(updates, dry=dry, force=True)

    def set_updates(self, updates: dict, dry=False) -> Self:
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

        self._updates += (UpdateQueryExpression(column, value, update_type="increment"),)

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

        self._updates += (UpdateQueryExpression(column, value, update_type="decrement"),)

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
            The sum value, or ``None`` when the filter matches zero
            rows (Postgres' ``SUM`` over an empty set returns ``NULL``,
            which psycopg surfaces as Python ``None``). Callers that
            want a numeric zero MUST coerce explicitly — the canonical
            pattern in this codebase is ``float(qb.sum("amount") or 0)``.

        Pre-fix the docstring read "or 0 if no results" — incorrect,
        since every call site that didn't ``or 0`` would have hit a
        ``TypeError`` on the empty-table path (``None * 1`` raises).
        The aggregate ``COUNT`` does coerce to ``0`` on empty, but
        ``SUM`` / ``AVG`` / ``MIN`` / ``MAX`` all surface ``None``
        because the SQL semantics differ.
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

    def order_by(self, column, direction="ASC") -> Self:
        """
        Specifies a column to order by.

        SECURITY — both ``column`` and ``direction`` are validated.
        SQL grammars splice them in unparameterised, so
        any caller passing a request-supplied value here (sort=...)
        used to be a clean SQL injection sink. Names must look like
        ``foo`` or ``table.column``; direction must be ASC or DESC.
        Anything else raises ``ValueError``.
        """
        for col in column.split(","):
            col = col.strip()
            if not _ORDER_BY_COLUMN_RE.match(col):
                raise InvalidArgumentException(
                    f"Invalid order_by column {col!r}. "
                    f"Expected ``name`` or ``table.column`` identifier; use "
                    f"``order_by_raw`` for expressions."
                )
            dir_str = (direction or "ASC").upper()
            if dir_str not in ("ASC", "DESC"):
                raise InvalidArgumentException(
                    f"Invalid order_by direction {direction!r}; expected ASC or DESC"
                )
            self._order_by += (OrderByExpression(col, direction=dir_str),)
        return self

    def order_by_raw(self, query, bindings=None) -> Self:
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

    def group_by(self, column) -> Self:
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

    def group_by_raw(self, query, bindings=None) -> Self:
        """
        Specifies a column to group by.

        Arguments:
            query {string} -- A raw query

        Returns:
            self
        """
        if bindings is None:
            bindings = []
        self._group_by += (GroupByExpression(column=query, raw=True, bindings=bindings),)

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

    def first(self, fields: list[str] | None = None, query: bool = False) -> Any:
        """
        Gets the first record.

        Returns:
            Model instance, dict, or None. Self if query=True.
        """

        if not fields:
            fields = []

        self.select(fields).limit(1)

        if query:
            return self

        result = self.new_connection().query(self.to_qmark(), self._bindings, results=1)

        return self.prepare_result(result)

    def first_or_create(self, wheres, creates: dict | None = None):
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

    def find(self, record_id: Any, column: str | None = None, query: bool = False) -> Any:
        """
        Finds a row by the primary key ID. Requires a model.

        Arguments:
            record_id {int} -- The ID of the primary key to fetch.

        Returns:
            Model instance or None. Self if query=True.
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
            raise Http404Exception()

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
                # Normalize every registered eager spec — raw strings
                # ("author"), dotted nested strings ("author.profile",
                # "author.parent.owner"), lists/tuples, and dicts
                # (``{"author": callback_fn}``) — into an ordered map of
                # ``{top_level_relation: [nested_path_strings...]}``. The
                # nested paths are passed to each relationship's
                # ``get_related(..., eagers=[...])`` so the chain continues
                # recursively: BelongsTo/HasMany/HasOne all
                # call ``builder.with_(eagers)`` internally, which rebuilds
                # the same EagerRelations → QueryBuilder pipeline for the
                # next level. Laravel parity: eager-load `author.profile`
                # loads `author`, then eager-loads `profile` on that
                # related model in a second query.
                normalized, callbacks = self._normalize_eager_specs(
                    self._eager_relation.get_relations()
                    + self._eager_relation.get_eagers()
                )
                # Merge any pre-registered callback_eagers (from
                # register(dict) path) into callbacks map.
                for rel_name, cb in getattr(
                    self._eager_relation, "callback_eagers", {}
                ).items():
                    head = rel_name.split(".")[0] if rel_name else rel_name
                    if head and callable(cb):
                        callbacks.setdefault(head, cb)

                for relation, nested in normalized.items():
                    try:
                        if inspect.isclass(self._model):
                            related = getattr(self._model, relation)
                            if callable(related) and not hasattr(related, "get_related"):
                                related = related()
                        else:
                            related = self._model.get_related(relation)

                        result_set = related.get_related(
                            self,
                            hydrated_model,
                            eagers=nested,
                            callback=callbacks.get(relation),
                        )

                        self._register_relationships_to_model(
                            related,
                            result_set,
                            hydrated_model,
                            relation_key=relation,
                        )
                    except Exception as e:
                        from cara.facades import Log

                        Log.error("Error processing eager %s: %s", relation, str(e))
                        raise

            if collection:
                # Tag every row as collection-hydrated so the strict
                # lazy-load guard (opt-in, off by default) only fires for
                # multi-row fetches where N+1 actually bites — never for
                # single find()/first() loads. No-op unless the guard is on.
                if hydrated_model:
                    for _row in hydrated_model:
                        _mark = getattr(_row, "_mark_from_collection", None)
                        if callable(_mark):
                            _mark()
                return hydrated_model if result else Collection([])
            else:
                return hydrated_model if result else None

        if collection:
            return Collection(result) if result else Collection([])
        else:
            return result or None

    @staticmethod
    def _normalize_eager_specs(raw_list):
        """
        Flatten a mixed list of eager specs into a two-tuple:

        - ``relations``: ordered ``{top_level: [nested_path_strings...]}``
        - ``callbacks``: ``{top_level: callable}`` extracted from dict specs

        Accepted spec shapes::

            "author"  # simple

            "author.profile"  # dotted
            ["author", "author.posts"]  # list/tuple
            {"author": callback_fn}  # callback
            {"author": ["profile"]}  # list of nested
            {"author.profile": callback_fn}  # dotted+callback

        Duplicates are deduped, preserving insertion order. Calling
        ``with_(["author", "author.posts"])`` produces
        ``{"author": ["posts"]}`` so ``author`` is loaded once and
        the nested ``posts`` is chained via ``get_related(eagers=...)``.
        """
        relations = {}
        callbacks = {}

        def _add(spec):
            if spec is None:
                return
            if isinstance(spec, str):
                if not spec:
                    return
                head, _, tail = spec.partition(".")
                bucket = relations.setdefault(head, [])
                if tail and tail not in bucket:
                    bucket.append(tail)
            elif isinstance(spec, (list, tuple, set)):
                for item in spec:
                    _add(item)
            elif isinstance(spec, dict):
                for key, value in spec.items():
                    if not isinstance(key, str) or not key:
                        continue
                    head, _, tail = key.partition(".")
                    bucket = relations.setdefault(head, [])
                    if tail and tail not in bucket:
                        bucket.append(tail)
                    if callable(value):
                        callbacks[head] = value
                    elif isinstance(value, (list, tuple, set)):
                        for sub in value:
                            if isinstance(sub, str) and sub and sub not in bucket:
                                bucket.append(sub)
                    elif isinstance(value, str) and value and value not in bucket:
                        bucket.append(value)
            # other types are ignored (non-actionable specs)

        for entry in raw_list:
            _add(entry)

        return relations, callbacks

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
        if isinstance(hydrated_model, Collection) and isinstance(
            related_result, Collection
        ):
            # Empty results still route through register_related so each
            # relationship applies its own empty default (Collection() for
            # to-many, None for to-one). Short-circuiting to None here gave
            # parents of a zero-row eager load ``None`` where the lazy path
            # (and any non-empty eager load) yields an empty Collection.
            map_related = (
                self._map_related(related_result, related)
                if related_result
                else related_result
            )
            for model in hydrated_model:
                related.register_related(relation_key, model, map_related)
        elif related_result and isinstance(hydrated_model, Collection):
            map_related = self._map_related(related_result, related)
            for model in hydrated_model:
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

    def get(self, selects: list[str] | None = None) -> Any:
        """
        Run the SELECT query and return a collection of results.

        Returns:
            Collection of Model instances or list of dicts.
        """
        selects = selects or []
        self.select(*selects)
        result = self.new_connection().query(self.to_qmark(), self._bindings)

        return self.prepare_result(result, collection=True)

    def new_connection(self) -> Any:
        if self._connection:
            return self._connection

        self._connection = self._db_manager.create_connection_instance(
            self.connection, self._schema
        )
        return self._connection

    def get_connection(self) -> Any:
        return self._connection

    def without_eager(self) -> Self:
        self._should_eager = False
        return self

    def with_(self, *eagers) -> Self:
        try:
            self._eager_relation.register(*eagers)
        except Exception as e:
            from cara.facades import Log

            Log.error("Eager relation register failed: %s", str(e))
            raise
        return self

    # Hard upper bounds to prevent DoS via huge OFFSET / LIMIT.
    _MAX_PER_PAGE = 500
    _MAX_PAGE = 100_000

    def paginate(self, per_page, page=1):
        # Sanitise inputs — coerce to int, clamp to safe bounds.
        try:
            per_page = max(1, min(int(per_page), self._MAX_PER_PAGE))
        except (TypeError, ValueError):
            per_page = 15
        try:
            page = max(1, min(int(page), self._MAX_PAGE))
        except (TypeError, ValueError):
            page = 1

        if page == 1:
            offset = 0
        else:
            offset = (page * per_page) - per_page

        new_from_builder = self.new_from_builder()
        new_from_builder._order_by = ()
        new_from_builder._columns = ()

        # Pagination without an explicit ORDER BY returns rows in
        # plan-dependent order, so concurrent inserts can make a row
        # appear on page 2 after also appearing on page 1, or skip a
        # row entirely between two paginate() calls. Default to the
        # primary key when the caller didn't pin an order — same
        # safety net Laravel applies in `Paginator::orderBy(...)`.
        if not self._order_by:
            try:
                pk = self.get_primary_key() if hasattr(self, "get_primary_key") else None
            except Exception:
                _logger.warning(
                    "primary key detection failed for pagination", exc_info=True
                )
                pk = None
            if pk:
                self.order_by(pk, "ASC")

        result = self.limit(per_page).offset(offset).get()
        total = new_from_builder.count()

        paginator = LengthAwarePaginator(result, per_page, page, total)
        return paginator

    def simple_paginate(self, per_page, page=1):
        # Sanitise inputs — coerce to int, clamp to safe bounds.
        try:
            per_page = max(1, min(int(per_page), self._MAX_PER_PAGE))
        except (TypeError, ValueError):
            per_page = 15
        try:
            page = max(1, min(int(page), self._MAX_PAGE))
        except (TypeError, ValueError):
            page = 1

        if page == 1:
            offset = 0
        else:
            offset = (page * per_page) - per_page

        # Fetch one extra row to detect whether a next page exists.
        # SimplePaginator trims the sentinel row before exposing data.
        result = self.limit(per_page + 1).offset(offset).get()

        paginator = SimplePaginator(result, per_page, page)
        return paginator

    def set_action(self, action) -> Self:
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

        # Carry row-lock modifiers (SKIP LOCKED / NOWAIT / OF) separately so
        # the base share/update lock map stays untouched; process_locks reads
        # them off the instance.
        grammar_instance._lock_modifier = getattr(
            self, "_lock_modifier", {"skip_locked": False, "nowait": False, "of": []}
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
        if self._unions:
            sql = self._append_unions_sql(sql, qmark=False)
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
            sql, params = Model.active().where("id", 5).dump_sql()
        """
        # to_qmark() has a side effect of resetting the builder; take a copy first
        # so subsequent calls on the original builder still work.
        cloned = deepcopy(self)
        # Scopes must run BEFORE get_grammar() — the grammar snapshots
        # _wheres/_updates at construction, so the real execution paths
        # (to_sql/to_qmark) order it this way too. Reversed, the debug
        # output would omit soft-delete/tenant scope clauses.
        cloned.run_scopes()
        grammar = cloned.get_grammar()
        sql = grammar.compile(cloned._action, qmark=True).to_sql()
        bindings = list(grammar._bindings)
        if pretty:
            # Swap '?' placeholders for %s for psycopg-style display
            sql = sql.replace("'?'", "%s")
        return sql, bindings

    def debug_sql(self) -> Self:
        """Print compiled SQL + bindings to stderr (dev-aid). Returns self for chaining.

        Example:
            rows = Model.active().where("status", "active").debug_sql().get()
            # stderr: [SQL] SELECT ... FROM "model" WHERE "status" = %s
            # stderr: [BIND] ['active']
        """
        from cara.facades import Log

        sql, bindings = self.dump_sql()
        Log.debug("[SQL] %s", sql, category='db.debug')
        Log.debug("[BIND] %s", bindings, category='db.debug')
        return self

    def run_scopes(self) -> Self:
        # ROOT CAUSE (2026-04-23): ``_global_scopes`` is a class-level
        # dict shared across every QueryBuilder instance. Under the
        # threaded queue worker (and ``--concurrency=8`` sync runs) two
        # threads can race here — thread A is iterating while thread B
        # calls ``with_global_scope()`` on the same model class, which
        # mutates the same dict. Python raises ``RuntimeError:
        # dictionary changed size during iteration`` and the query
        # aborts. Snapshot to a list before iterating so the iterator
        # is frozen for the duration of ``scope(self)`` calls. Any
        # scopes registered mid-iteration will apply on the next query,
        # which matches Laravel's semantics.
        scopes = list(self._global_scopes.get(self._action, {}).items())
        for _name, scope in scopes:
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

        if self._unions:
            sql = self._append_unions_sql(sql, qmark=True)

        self.reset()

        return sql

    def _append_unions_sql(self, base_sql, qmark=False):
        """Append UNION / UNION ALL clauses by compiling each union builder
        and concatenating its SQL + bindings to the base statement.
        """
        parts = [base_sql]
        for child, all_flag in self._unions:
            cloned = deepcopy(child)
            cloned.run_scopes()
            child_grammar = cloned.get_grammar()
            child_sql = child_grammar.compile(cloned._action, qmark=qmark).to_sql()
            kw = "UNION ALL" if all_flag else "UNION"
            parts.append(f"{kw} ({child_sql})")
            if qmark:
                self._bindings = tuple(self._bindings) + tuple(child_grammar._bindings)
        return " ".join(parts)

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
            # PostgreSQL case-insensitive LIKE — passes through to the
            # grammar verbatim, same as "like".
            "ilike",
            "not ilike",
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
            raise InvalidArgumentException(
                "Invalid comparison operator. The operator can be {}".format(", ".join(operators))
            )

        return operator, value

    def __call__(self):
        """
        Magic method to standardize what happens when the query builder object is called.

        Returns:
            self
        """
        return self

    def macro(self, name, callable) -> Self:
        self._macros.update({name: callable})
        return self

    def when(self, conditional, callback, otherwise=None) -> Self:
        """Apply the callback if the condition is truthy (Laravel-style).

        Supports two calling conventions::

            # Simple boolean — callback receives (builder,)
            query.when(filters.get("status"), lambda q: q.where("status", status))

            # Value forwarding — callback receives (builder, value)
            query.when(filters.get("status"), lambda q, v: q.where("status", v))

        The value-forwarding form avoids the need to close over variables
        or compute a flag + re-read the value separately.

        Args:
            conditional: The value to evaluate. If truthy, ``callback``
                is invoked. If ``conditional`` is callable, it is called
                first and the result is used.
            callback: ``(builder)`` or ``(builder, value)`` — called
                when ``conditional`` is truthy.
            otherwise: ``(builder)`` or ``(builder, value)`` — called
                when ``conditional`` is falsy.

        Returns:
            self
        """
        import inspect

        value = conditional() if callable(conditional) else conditional
        chosen = callback if value else otherwise
        if chosen is not None:
            sig = inspect.signature(chosen)
            if len(sig.parameters) >= 2:
                chosen(self, value)
            else:
                chosen(self)
        return self

    def unless(self, conditional, callback, otherwise=None) -> Self:
        """Apply the callback if the condition is falsy (opposite of when).

        Supports the same value-forwarding convention as :meth:`when`.

        Args:
            conditional: The value to evaluate.
            callback: Called with the builder when condition is falsy.
            otherwise: Called with the builder when condition is truthy.

        Returns:
            self
        """
        import inspect

        value = conditional() if callable(conditional) else conditional
        chosen = callback if not value else otherwise
        if chosen is not None:
            sig = inspect.signature(chosen)
            if len(sig.parameters) >= 2:
                chosen(self, value)
            else:
                chosen(self)
        return self

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

    def pluck(self, column: str, key_by: str | None = None):
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
                for record in chunk:
                    record.update({'processed': True})

            Model.active().chunk(200, process)
        """
        page = 1
        while True:
            offset = (page - 1) * chunk_size
            builder = self.clone()
            results = builder.limit(chunk_size).offset(offset).get()

            if not results or (hasattr(results, "is_empty") and results.is_empty()):
                break

            result = callback(results)

            if result is False:
                return False

            count = len(results) if hasattr(results, "__len__") else results.count()
            if count < chunk_size:
                break

            page += 1

        return True

    def upsert(
        self,
        values: list[dict[str, Any]],
        unique_by: list[str],
        update: list[str] | None = None,
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
        processed: list[dict[str, Any]] = []
        for record in values:
            if model:
                # Apply mass assignment protection
                record = model.filter_mass_assignment(record)
                # Apply casts if requested
                if cast:
                    record = model.cast_values(record)
            processed.append(dict(record))

        # Stamp timestamps BEFORE the uniform-keys check so a mix of
        # explicitly-timestamped and bare rows still ends up uniform.
        # ``update=[]`` is the explicit insert-if-missing (DO NOTHING)
        # form — no update list to extend there.
        stamp_timestamps = bool(
            model and getattr(model, "__timestamps__", False)
        )
        if stamp_timestamps:
            timestamp_value = model.get_new_date().to_datetime_string()
            for record in processed:
                if record.get(model.date_created_at) is None:
                    record[model.date_created_at] = timestamp_value
                if record.get(model.date_updated_at) is None:
                    record[model.date_updated_at] = timestamp_value

        # Every row must cover the same columns. Silently taking row 0's
        # keys (the old behavior) misaligned values under the wrong
        # columns for heterogeneous rows — and filling gaps with NULL
        # would silently overwrite existing data through the
        # ``EXCLUDED.col`` update. Fail loudly instead.
        if processed:
            expected = set(processed[0])
            for i, record in enumerate(processed[1:], start=1):
                if set(record) != expected:
                    raise QueryException(
                        "upsert() rows must share the same columns: row 0 has "
                        f"{sorted(expected)}, row {i} has {sorted(record)}."
                    )

        # Sorted keys → deterministic column order across rows and runs.
        self._upsert_values = [dict(sorted(record.items())) for record in processed]

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

                self._upsert_update = sorted(all_columns - exclude_columns)
            else:
                self._upsert_update = []
        else:
            self._upsert_update = list(update)
            # Laravel parity: updated_at rides along on conflict updates
            # (but an explicit empty list means DO NOTHING — leave it).
            if (
                stamp_timestamps
                and self._upsert_update
                and model.date_updated_at not in self._upsert_update
            ):
                self._upsert_update.append(model.date_updated_at)

        if not self.dry:
            connection = self.new_connection()
            query_result = connection.query(self.to_qmark(), self._bindings)

            # Affected row count: grammars with RETURNING hand back the
            # touched rows (len == inserted + updated); grammars without
            # surface cursor.rowcount as an int.
            if isinstance(query_result, int):
                return query_result
            return len(query_result or [])

        return len(self._upsert_values)

    def bulk_update(
        self,
        records: list[dict[str, Any]],
        key: str = "id",
        update_columns: list[str] | None = None,
    ):
        """Bulk update multiple records in a single query using PostgreSQL VALUES + UPDATE FROM.

        Args:
            records: List of dicts, each must contain the key column
            key: Column to match records on (default: "id")
            update_columns: Columns to update (if None, updates all except key)

        Returns:
            Number of affected rows

        Example:
            Model.bulk_update([
                {"id": 1, "price": 9.99, "status": "active"},
                {"id": 2, "price": 19.99, "status": "inactive"},
            ], key="id", update_columns=["price", "status"])
        """
        if not records:
            return 0

        # Determine columns to update
        if update_columns is None:
            update_columns = [k for k in records[0] if k != key]

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

        values_clause = ", ".join(placeholders)
        col_defs = ", ".join(f'"{c}"' for c in all_columns)
        set_clause = ", ".join(f'"{c}" = _bulk."{c}"' for c in update_columns)
        table = self._table.name if hasattr(self._table, "name") else str(self._table)

        sql = f'''
            UPDATE "{table}" SET {set_clause}
            FROM (VALUES {values_clause}) AS _bulk({col_defs})
            WHERE "{table}"."{key}" = _bulk."{key}"
        '''

        connection = self.new_connection()
        return connection.query(sql, tuple(bindings))

    def cursor(self, chunk_size: int = 1000):
        """
        Stream results from the database in memory-bounded chunks.

        Implementation note: ``cursor()`` does NOT open a server-side
        DB cursor — it issues ``LIMIT N OFFSET M`` page queries under
        the hood and yields rows one at a time. The DB connection is
        released between chunks (not held across the whole iteration),
        which keeps the pool free but means the iteration is
        OFFSET-paginated, with two consequences worth knowing:

          * **Cost per chunk grows with offset.** PostgreSQL still
            scans-and-skips ``M`` rows before returning the next ``N``.
            For multi-million-row tables the late chunks dominate the
            total wall clock.
          * **Not stable under concurrent writes.** Inserts/deletes
            mid-iteration can shift the page boundary, causing rows
            to be skipped OR re-yielded across consecutive chunks.

        For large tables that need a stable iteration order or O(1)
        per-chunk cost regardless of position, prefer
        :py:meth:`chunk_by_id` (keyset pagination on a monotonic
        column — ``WHERE id > last_id ORDER BY id LIMIT N``).

        Args:
            chunk_size: Number of records to fetch per page (default: 1000)

        Yields:
            Model: Individual model instances

        Example:
            # Memory-efficient processing of (relatively) static datasets
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

    # ===== UNION =====
    def union(self, query, all=False) -> Self:
        """Append a UNION (or UNION ALL) clause from another QueryBuilder.

        Args:
            query: A QueryBuilder instance whose result set should be unioned.
            all: When True, emit UNION ALL (keeps duplicates).

        Returns:
            self
        """
        if hasattr(query, "get_builder"):
            query = query.get_builder()
        self._unions.append((query, bool(all)))
        return self

    def union_all(self, query):
        """Shortcut for ``union(query, all=True)``."""
        return self.union(query, all=True)

    # ===== CHUNK BY ID / LAZY ITERATION =====
    def chunk_by_id(self, chunk_size: int, callback: Callable, column: str = "id"):
        """Process results in keyset-paginated chunks ordered by ``column``.

        Safer than ``chunk`` for mutating operations because it uses a
        ``WHERE column > last_id`` cursor instead of ``OFFSET`` (which can skip
        rows when records are deleted mid-iteration).
        """
        # Upfront column validation — same ``_ORDER_BY_COLUMN_RE``
        # gate that ``order_by`` applies, but enforced HERE so a
        # caller passing a bad column name fails fast at the entry
        # point instead of after the WHERE clause has already been
        # queued onto a clone. Pre-fix the validation lived only
        # inside ``order_by`` (line ~3735), which meant ``where``
        # (line ~3733) accepted the column without checking; an
        # attacker-shaped ``"id; DROP TABLE x"`` never actually
        # reached the DB (order_by raised first, killing the query)
        # but the failure surfaced in the wrong place and the WHERE
        # quirk was a foot-gun waiting for the next refactor to
        # swap the order. ``re.fullmatch`` (not ``match``) so a
        # trailing ``;`` doesn't slip past.
        if not isinstance(column, str) or not _ORDER_BY_COLUMN_RE.fullmatch(column):
            raise InvalidArgumentException(
                f"chunk_by_id: invalid column name {column!r}. Allowed: "
                f"``[A-Za-z_][A-Za-z0-9_]*`` optionally with a single "
                f"``.<col>`` qualifier (table.column).",
            )
        last_id = None
        while True:
            builder = self.clone()
            if last_id is not None:
                builder = builder.where(column, ">", last_id)
            results = builder.order_by(column, "asc").limit(chunk_size).get()

            if not results or (hasattr(results, "is_empty") and results.is_empty()):
                break

            result = callback(results)
            if result is False:
                return False

            last_record = results[-1] if hasattr(results, "__getitem__") else None
            if last_record is None:
                break
            last_id = (
                getattr(last_record, column, None)
                if not isinstance(last_record, dict)
                else last_record.get(column)
            )
            if last_id is None:
                break

            count = len(results) if hasattr(results, "__len__") else results.count()
            if count < chunk_size:
                break

        return True

    def lazy(self, chunk_size: int = 1000):
        """Generator interface over ``chunk`` — yields individual records.

        Equivalent of Laravel's ``lazy()``. Memory-efficient streaming.
        """
        page = 1
        while True:
            offset = (page - 1) * chunk_size
            builder = self.clone()
            results = builder.limit(chunk_size).offset(offset).get()
            if not results or (hasattr(results, "is_empty") and results.is_empty()):
                break
            yield from results
            count = len(results) if hasattr(results, "__len__") else results.count()
            if count < chunk_size:
                break
            page += 1

    def lazy_by_id(self, chunk_size: int = 1000, column: str = "id"):
        """Keyset-cursor generator — yields individual records in id order.

        Same safety properties as ``chunk_by_id`` but exposed as a generator.
        """
        last_id = None
        while True:
            builder = self.clone()
            if last_id is not None:
                builder = builder.where(column, ">", last_id)
            results = builder.order_by(column, "asc").limit(chunk_size).get()
            if not results or (hasattr(results, "is_empty") and results.is_empty()):
                break
            count = 0
            last_record = None
            for record in results:
                yield record
                last_record = record
                count += 1
            if last_record is None:
                break
            last_id = (
                getattr(last_record, column, None)
                if not isinstance(last_record, dict)
                else last_record.get(column)
            )
            if last_id is None or count < chunk_size:
                break

    # ===== CURSOR PAGINATE =====
    def cursor_paginate(
        self,
        per_page: int,
        *,
        cursor=None,
        column: str = "id",
        primary_key: str = "id",
        direction: str = "asc",
        scope: str,
        filter_fingerprint: str,
    ):
        """Laravel-style cursor pagination.

        Returns a CursorPaginator carrying the rows and next/prev cursor strings.
        Use the cursor returned in the response on subsequent calls to fetch the
        next page. Avoids OFFSET — stable under inserts/deletes.

        Args:
            per_page: Page size.
            cursor: Opaque cursor string from a previous response.
            column: Column to keyset-paginate by (must be unique + indexed).
            direction: "asc" or "desc".

        Returns:
            CursorPaginator
        """
        from cara.http.Cursor import decode_cursor, encode_cursor

        from ..pagination import CursorPaginator

        if (
            isinstance(per_page, bool)
            or not isinstance(per_page, int)
            or not 1 <= per_page <= 100
        ):
            raise ValueError("per_page must be an integer between 1 and 100")
        if direction not in {"asc", "desc"}:
            raise ValueError("direction must be 'asc' or 'desc'")
        if not isinstance(column, str) or not column:
            raise ValueError("column must be a non-empty string")
        if not isinstance(primary_key, str) or not primary_key:
            raise ValueError("primary_key must be a non-empty string")
        if not isinstance(scope, str) or not scope or len(scope) > 160:
            raise ValueError("scope must be a non-empty string of at most 160 characters")
        if (
            not isinstance(filter_fingerprint, str)
            or len(filter_fingerprint) != 64
            or any(char not in "0123456789abcdef" for char in filter_fingerprint)
        ):
            raise ValueError("filter_fingerprint must be lowercase SHA-256 hex")

        builder = self.clone()
        op = ">" if direction == "asc" else "<"
        if cursor is not None:
            decoded = decode_cursor(
                cursor,
                direction=direction,
                fingerprint=filter_fingerprint,
                scope=scope,
            )
            sort_value = decoded["v"]
            row_id = decoded["id"]
            builder = builder.where(
                lambda outer: outer.where(column, op, sort_value).or_where(
                    lambda tied: tied.where(column, "=", sort_value).where(
                        primary_key, op, row_id
                    )
                )
            )

        # Fetch one extra to know whether a next page exists.
        results = (
            builder.order_by(column, direction)
            .order_by(primary_key, direction)
            .limit(per_page + 1)
            .get()
        )

        has_more = len(results) > per_page
        if has_more:
            results = results[:per_page]

        next_cursor = None
        if has_more and len(results) > 0:
            last = results[-1]
            value = (
                getattr(last, column, None)
                if not isinstance(last, dict)
                else last.get(column)
            )
            row_id = (
                getattr(last, primary_key, None)
                if not isinstance(last, dict)
                else last.get(primary_key)
            )
            next_cursor = encode_cursor(
                value,
                row_id,
                direction=direction,
                fingerprint=filter_fingerprint,
                scope=scope,
            )

        return CursorPaginator(results, per_page, next_cursor, None)
