"""Core SQL statement compilation operations for ``BaseGrammar``."""

from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401

from cara.eloquent.expressions import (
    JoinClause,
    OnClause,
)


def _grammar_initialize(
    self,
    columns=(),
    table="users",
    database=None,
    wheres=(),
    limit=False,
    offset=False,
    updates=None,
    aggregates=(),
    order_by=(),
    distinct=False,
    group_by=(),
    joins=(),
    lock=False,
    having=(),
    connection_details=None,
):
    self._columns = columns
    self.table = table
    self.database = database
    self._wheres = wheres
    self._limit = limit
    self._offset = offset
    self._updates = updates or {}
    self._aggregates = aggregates
    self._order_by = order_by
    self._group_by = group_by
    self._distinct = distinct
    self._joins = joins
    self._having = having
    self.lock = lock
    self._lock_modifier = {"skip_locked": False, "nowait": False, "of": []}
    self._connection_details = connection_details or {}
    self._column = None

    self._bindings = []

    self._sql = ""

    self._sql_qmark = ""
    self._action = "select"
    self.queries = []


def _grammar_compile(self, action, qmark=False):
    self._action = action
    return getattr(self, "_compile_" + action)(qmark=qmark)


def _compile_select(self, qmark=False):
    """
    Compile a select query statement.

    Keyword Arguments:
        qmark {bool} -- [description] (default: {False})

    Returns:
        [type] -- [description]
    """
    if not self.table:
        self._sql = (
            self.select_no_table()
            .format(
                columns=self.process_columns(separator=", ", qmark=qmark),
                table=self.process_table(self.table),
                joins=self.process_joins(qmark=qmark),
                wheres=self.process_wheres(qmark=qmark),
                limit=self.process_limit(),
                offset=self.process_offset(),
                aggregates=self.process_aggregates(),
                # Evaluation order MUST match SQL clause order
                # (GROUP BY → HAVING → ORDER BY). .format() is
                # keyword-matched so the rendered template is
                # unaffected by kwarg order, BUT process_group_by /
                # process_order_by append their raw `bindings` to
                # self._bindings as a side effect of being CALLED.
                # If order_by is evaluated before group_by, a query
                # carrying raw bindings on BOTH clauses binds them
                # into each other's %s slots (qmark/executed path).
                group_by=self.process_group_by(),
                having=self.process_having(qmark=qmark),
                order_by=self.process_order_by(),
                lock=self.process_locks(),
            )
            .strip()
        )
    else:
        self._sql = (
            self.select_format()
            .format(
                columns=self.process_columns(separator=", ", qmark=qmark),
                keyword="DISTINCT" if self._distinct else "",
                table=self.process_table(self.table),
                joins=self.process_joins(qmark=qmark),
                wheres=self.process_wheres(qmark=qmark),
                limit=self.process_limit(),
                offset=self.process_offset(),
                aggregates=self.process_aggregates(),
                # Evaluation order MUST match SQL clause order
                # (GROUP BY → HAVING → ORDER BY). .format() is
                # keyword-matched so the rendered template is
                # unaffected by kwarg order, BUT process_group_by /
                # process_order_by append their raw `bindings` to
                # self._bindings as a side effect of being CALLED.
                # If order_by is evaluated before group_by, a query
                # carrying raw bindings on BOTH clauses binds them
                # into each other's %s slots (qmark/executed path).
                group_by=self.process_group_by(),
                having=self.process_having(qmark=qmark),
                order_by=self.process_order_by(),
                lock=self.process_locks(),
            )
            .strip()
        )

    return self


def _compile_update(self, qmark=False):
    """
    Compiles an update query statement.

    Keyword Arguments:
        qmark {bool} -- Whether the query should use qmark. (default: {False})

    Returns:
        self
    """
    self._sql = self.update_format().format(
        key_equals=self._compile_key_value_equals(qmark=qmark),
        table=self.process_table(self.table),
        wheres=self.process_wheres(qmark=qmark),
    )

    return self


def _compile_insert(self, qmark=False):
    """
    Compiles an insert expression.

    Returns:
        self
    """
    self._sql = self.insert_format().format(
        key_equals=self._compile_key_value_equals(qmark=qmark),
        table=self.process_table(self.table),
        columns=self.process_columns(separator=", ", action="insert", qmark=qmark),
        values=self.process_values(separator=", ", qmark=qmark),
    )

    return self


def _compile_bulk_create(self, qmark=False):
    """
    Compiles an insert expression.

    Returns:
        self
    """
    all_values = [list(x.values()) for x in self._columns]

    self._sql = self.bulk_insert_format().format(
        key_equals=self._compile_key_value_equals(qmark=qmark),
        table=self.process_table(self.table),
        columns=self.columnize_bulk_columns(list(self._columns[0].keys())),
        values=self.columnize_bulk_values(all_values, qmark=qmark),
    )
    return self


def _grammar_columnize_bulk_columns(self, columns=None):
    columns = columns or []
    return ", ".join(
        self.column_string().format(column=x, separator="") for x in columns
    ).rstrip(",")


def _grammar_columnize_bulk_values(self, columns=None, qmark=False):
    columns = columns or []
    sql = ""
    for x in columns:
        inner = ""
        if isinstance(x, list):
            for y in x:
                if qmark:
                    self.add_binding(y)
                inner += (
                    "'?', "
                    if qmark
                    else self.value_string().format(value=y, separator=", ")
                )

            inner = inner.rstrip(", ")
            sql += self.process_value_string().format(value=inner, separator=", ")
        else:
            if qmark:
                self.add_binding(x)
            sql += (
                "'?', "
                if qmark
                else self.process_value_string().format(
                    value="?" if qmark else x,
                    separator=", ",
                )
            )

    return sql.rstrip(", ")


def _grammar_process_value_string(self):
    return "({value}){separator}"


def _compile_delete(self, qmark=False):
    """
    Compiles a delete expression.

    Returns:
        self
    """
    self._sql = self.delete_format().format(
        key_equals=self._compile_key_value_equals(qmark=qmark),
        table=self.process_table(self.table),
        wheres=self.process_wheres(qmark=qmark),
    )

    return self


def _get_multiple_columns(self, columns):
    """
    Compiles a string or a list of strings into the grammars column syntax.

    Arguments:
        columns {string|list} -- A column or list of columns

    Returns:
        self
    """
    if isinstance(columns, list):
        column_string = ""
        for col in columns:
            column_string += self.process_column(col) + ", "
        return column_string.rstrip(", ")

    return self.process_column(columns)


def _grammar_process_joins(self, qmark=False):
    """
    Compiles a join expression.

    Returns:
        self
    """
    sql = ""
    for join in self._joins:
        if isinstance(join, JoinClause):
            on_string = ""
            for clause_idx, clause in enumerate(join.get_on_clauses()):
                keyword = clause.operator.upper() if clause_idx else "ON"

                if isinstance(clause, OnClause):
                    on_string += f"{keyword} {self._table_column_string(clause.column1)} {clause.equality} {self._table_column_string(clause.column2)} "
                else:
                    if clause.value_type == "NULL":
                        sql_string = f"{self.where_null_string()} "
                        on_string += sql_string.format(
                            keyword=keyword,
                            column=self.process_column(clause.column),
                        )
                    elif clause.value_type == "NOT NULL":
                        sql_string = f"{self.where_not_null_string()} "
                        on_string += sql_string.format(
                            keyword=keyword,
                            column=self.process_column(clause.column),
                        )
                    else:
                        if qmark:
                            value = "'?'"
                            self.add_binding(clause.value)
                        else:
                            value = self._compile_value(clause.value)
                        on_string += f"{keyword} {self._table_column_string(clause.column)} {clause.equality} {value} "

            sql += self.join_string().format(
                foreign_table=self.process_table(join.table),
                alias=f" AS {self.process_table(join.alias)}" if join.alias else "",
                on=on_string,
                keyword=self.join_keywords[join.clause],
            )
            sql += " "

    return sql


def _compile_key_value_equals(self, qmark=False):
    """
    Compiles key value pairs.

    Keyword Arguments:
        qmark {bool} -- Whether the query should use qmark. (default: {False})

    Returns:
        self
    """
    sql = ""
    for update in self._updates:
        if update.update_type == "increment":
            sql_string = self.increment_string()
        elif update.update_type == "decrement":
            sql_string = self.decrement_string()
        else:
            sql_string = self.key_value_string()

        column = update.column
        value = update.value
        if isinstance(column, dict):
            for key, value in column.items():
                if self.is_column_expression(value):
                    # F / arithmetic / GREATEST / LEAST: the value is a
                    # column-reference expression, NOT a bound param.
                    # Render it with identifiers quoted by the grammar so
                    # e.g. ``"click_count" = "click_count" + 1`` emits no
                    # %s placeholder (qmark path adds no binding either).
                    sql += self.column_value_string().format(
                        column=self._table_column_string(key),
                        value=self.compile_expression(value),
                        separator=", ",
                    )
                elif hasattr(value, "expression"):
                    sql += self.column_value_string().format(
                        column=self._table_column_string(key),
                        value=value.expression,
                        separator=", ",
                    )
                else:
                    sql += sql_string.format(
                        column=self._table_column_string(key),
                        value=value if not qmark else "?",
                        separator=", ",
                    )

                    if qmark:
                        self._bindings += (value,)
        else:
            sql += sql_string.format(
                column=self._table_column_string(column),
                value=value if not qmark else "?",
                separator=", ",
            )
            if qmark:
                self._bindings += (value,)

    sql = sql.rstrip(", ")
    return sql
