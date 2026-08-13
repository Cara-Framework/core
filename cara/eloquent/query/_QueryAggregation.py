"""Mutation, compilation and aggregate operations for ``QueryBuilder``."""

from __future__ import annotations

import inspect
import logging
from copy import deepcopy
from typing import Self

from cara.eloquent.expressions import (
    SelectExpression,
)
from cara.exceptions import (
    InvalidArgumentException,
)
from cara.facades import Log
from cara.support import Collection

from ..schema import Schema

_logger = logging.getLogger("cara.eloquent.query")
QueryBuilder: type


def _bind_query_builder(builder_type: type) -> None:
    global QueryBuilder
    QueryBuilder = builder_type


def _qb_set_action(self, action) -> Self:
    """
    Sets the action that the query builder should take when the query is built.

    Arguments:
        action {string} -- The action that the query builder should take.

    Returns:
        self
    """
    self._action = action
    return self


def _qb_get_grammar(self):
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


def _qb_to_sql(self):
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


def _qb_explain(self):
    """
    Explains the Query execution plan.

    Returns:
        Collection
    """
    sql = self.to_sql()
    explanation = self.statement(f"EXPLAIN {sql}")
    return explanation


def _qb_dump_sql(self, pretty: bool = True):
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


def _qb_debug_sql(self) -> Self:
    """Print compiled SQL + bindings to stderr (dev-aid). Returns self for chaining.

    Example:
        rows = Model.active().where("status", "active").debug_sql().get()
        # stderr: [SQL] SELECT ... FROM "model" WHERE "status" = %s
        # stderr: [BIND] ['active']
    """

    sql, bindings = self.dump_sql()
    Log.debug("[SQL] %s", sql, category="db.debug")
    Log.debug("[BIND] %s", bindings, category="db.debug")
    return self


def _qb_run_scopes(self) -> Self:
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


def _qb_to_qmark(self):
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


def _qb_append_unions_sql(self, base_sql, qmark=False):
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


def _qb_new(self):
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
        database_manager=self._db_manager,
    )

    if self._table:
        builder.table(self._table.name)

    return builder


def _qb_avg(self, column, dry=False):
    """Get the average value of a column.

    Returns:
        The average value, or None if no results.
    """
    return self._run_aggregate("AVG", column, dry)


def _qb_min(self, column, dry=False):
    """Get the minimum value of a column.

    Returns:
        The min value, or None if no results.
    """
    return self._run_aggregate("MIN", column, dry)


def _qb_extract_operator_value(self, *args):
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
            "Invalid comparison operator. The operator can be {}".format(
                ", ".join(operators)
            )
        )

    return operator, value


def _qb_call(self):
    """
    Magic method to standardize what happens when the query builder object is called.

    Returns:
        self
    """
    return self


def _qb_macro(self, name, callable) -> Self:
    self._macros.update({name: callable})
    return self


def _qb_when(self, conditional, callback, otherwise=None) -> Self:
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

    value = conditional() if callable(conditional) else conditional
    chosen = callback if value else otherwise
    if chosen is not None:
        sig = inspect.signature(chosen)
        if len(sig.parameters) >= 2:
            chosen(self, value)
        else:
            chosen(self)
    return self


def _qb_unless(self, conditional, callback, otherwise=None) -> Self:
    """Apply the callback if the condition is falsy (opposite of when).

    Supports the same value-forwarding convention as :meth:`when`.

    Args:
        conditional: The value to evaluate.
        callback: Called with the builder when condition is falsy.
        otherwise: Called with the builder when condition is truthy.

    Returns:
        self
    """

    value = conditional() if callable(conditional) else conditional
    chosen = callback if not value else otherwise
    if chosen is not None:
        sig = inspect.signature(chosen)
        if len(sig.parameters) >= 2:
            chosen(self, value)
        else:
            chosen(self)
    return self


def _qb_truncate(self, foreign_keys=False, dry=False):
    sql = self.get_grammar().truncate_table(self.get_table_name(), foreign_keys)

    if dry or self.dry:
        return sql

    return self.new_connection().query(sql, ())


def _qb_exists(self):
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
        result = self.new_connection().query(self.to_qmark(), self._bindings, results=1)
    finally:
        self._columns = saved_columns
        self._limit = saved_limit
    return result is not None and result != {}


def _qb_doesnt_exist(self):
    """Determine if no rows exist for the current query.

    Returns:
        bool
    """
    return not self.exists()


def _qb_in_random_order(self):
    """Puts Query results in random order."""
    return self.order_by_raw(self.grammar().compile_random())


def _qb_new_from_builder(self, from_builder=None):
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
        database_manager=self._db_manager,
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


def _qb_clone(self):
    """Create an independent copy of this builder (Laravel-style).

    Useful when you need to run both count() and get() from the same
    base query without one operation corrupting the other.

    Returns:
        QueryBuilder
    """
    return self.new_from_builder(self)


def _qb_get_table_columns(self):
    return self.get_schema().get_columns(self._table.name)


def _qb_get_schema(self):
    return Schema(connection=self.connection)


def _qb_latest(self, *fields):
    """
    Gets the latest record.

    Returns:
        querybuilder
    """

    if not fields:
        fields = ("created_at",)

    return self.order_by(column=",".join(fields), direction="DESC")


def _qb_oldest(self, *fields):
    """
    Gets the oldest record.

    Returns:
        querybuilder
    """

    if not fields:
        fields = ("created_at",)

    return self.order_by(column=",".join(fields), direction="ASC")


def _qb_value(self, column: str):
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


def _qb_pluck(self, column: str, key_by: str | None = None):
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
