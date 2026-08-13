"""Builder state and SELECT projection construction for ``QueryBuilder``."""

from __future__ import annotations

import logging
from typing import Self

from cara.eloquent.expressions import (
    FromTable,
    Greatest,
    Least,
    SelectExpression,
    SubGroupExpression,
)
from cara.exceptions import (
    InvalidArgumentException,
    QueryException,
)

from ..schema import Schema
from ..scopes import BaseScope
from ._QuerySafety import ORDER_BY_COLUMN_RE as _ORDER_BY_COLUMN_RE
from .TransactionContext import TransactionContext

_logger = logging.getLogger("cara.eloquent.query")
QueryBuilder: type


def _bind_query_builder(builder_type: type) -> None:
    global QueryBuilder
    QueryBuilder = builder_type


def _qb_set_creates_related(self, fields: dict):
    self._creates_related = fields
    return self


def _qb_set_schema(self, schema) -> Self:
    self._schema = schema
    return self


def _qb_shared_lock(self):
    return self.make_lock("share")


def _qb_lock_for_update(
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
    return self.make_lock("update", skip_locked=skip_locked, nowait=nowait, of=of)


def _qb_make_lock(
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


def _qb_reset(self) -> Self:
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


def _qb_get_connection_information(self):
    """Get connection info from DatabaseManager"""
    return self._db_manager.get_connection_info(self.connection)


def _qb_table(self, table, raw=False) -> Self:
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


def _qb_get_table_name(self):
    """Get the name of the table for this query."""
    return self._table.name


def _qb_begin(self):
    """Begin a new database transaction."""
    self._connection = self.new_connection()
    self._connection.begin()
    return self._connection


def _qb_get_schema_builder(self):
    """Get a schema builder instance for the current connection."""
    return Schema(
        connection=self.connection,
        grammar=self.grammar,
    )


def _qb_commit(self):
    """Commit the active database transaction."""
    if not hasattr(self, "_connection") or self._connection is None:
        raise QueryException("No active transaction to commit.")
    return self._connection.commit()


def _qb_rollback(self) -> Self:
    """Roll back the active database transaction."""
    if not hasattr(self, "_connection") or self._connection is None:
        raise QueryException("No active transaction to roll back.")
    self._connection.rollback()
    return self


def _qb_transaction(self, callback=None):
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


def _qb_set_scope(self, name, callable) -> Self:
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


def _qb_set_global_scope(self, name="", callable=None, action="select") -> Self:
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


def _qb_without_global_scopes(self) -> Self:
    self._global_scopes = {}
    return self


def _qb_remove_global_scope(self, scope, action=None) -> Self:
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


def _qb_getattr(self, attribute):
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


def _qb_on(self, connection) -> Self:
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


def _qb_select(self, *args) -> Self:
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


def _qb_distinct(self, boolean=True) -> Self:
    """
    Specifies that all columns should be distinct.

    Returns:
        self
    """
    self._distinct = boolean
    return self


def _qb_add_select(self, alias, callable) -> Self:
    """
    Specifies columns that should be selected.

    Returns:
        self
    """
    builder = callable(self.new())
    self._columns += (SubGroupExpression(builder, alias=alias),)

    return self


def _qb_statement(self, query, bindings=None):
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


def _qb_select_raw(self, query) -> Self:
    """
    Specifies raw SQL that should be injected into the select expression.

    Returns:
        self
    """
    self._columns += (SelectExpression(query, raw=True),)
    return self


def _qb_rendering_grammar(self):
    """Return a transient grammar instance for quoting identifiers.

    Seeded with this builder's table + connection details so qualified
    column references resolve to the RIGHT table (not the grammar's
    ``"users"`` default) and the dialect's quote chars / prefix apply.
    """
    return self.grammar(
        table=self._table,
        connection_details=self._connection_details,
    )


def _qb_quote_window_identifier(self, column: str) -> str:
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


def _qb_select_window(
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

        Order.select("*").select_window(
            "ROW_NUMBER()",
            partition_by=["customer_id"],
            order_by=[("placed_at", "asc")],
            alias="rn",
        )
        # SELECT *, ROW_NUMBER() OVER (
        #     PARTITION BY "customer_id" ORDER BY "placed_at" ASC
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
    quoted_alias = (
        self._rendering_grammar().column_string().format(column=alias, separator="")
    )
    self._columns += (
        SelectExpression(
            f"{expression} OVER ({over.strip()}) AS {quoted_alias}", raw=True
        ),
    )
    return self


def _qb_select_greatest(self, *columns, alias: str | None = None) -> Self:
    """Add a ``GREATEST(...)`` SELECT column (mirrors ``select_if_null``).

    Each argument may be a column name (string — quoted as an
    identifier), an ``F`` reference, or a literal expression node.

    Example::

        q.select_greatest("price_low", "floor_price", alias="effective_low")
        # SELECT GREATEST("price_low", "floor_price") AS "effective_low"
    """
    return self._select_function_expression(Greatest, columns, alias)


def _qb_select_least(self, *columns, alias: str | None = None) -> Self:
    """Add a ``LEAST(...)`` SELECT column (mirrors ``select_if_null``).

    Example::

        q.select_least("price_high", "ceiling_price", alias="effective_high")
        # SELECT LEAST("price_high", "ceiling_price") AS "effective_high"
    """
    return self._select_function_expression(Least, columns, alias)
