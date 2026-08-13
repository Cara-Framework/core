"""SQL clause rendering operations for ``BaseGrammar``."""

from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401

from cara.eloquent.expressions import (
    SubGroupExpression,
    SubSelectExpression,
)
from cara.exceptions import InvalidArgumentException


def _grammar_process_aggregates(self):
    """
    Compiles aggregates to be used in a query expression.

    Returns:
        self
    """
    sql = ""
    for aggregates in self._aggregates:
        aggregate = aggregates.aggregate
        column = aggregates.column
        aggregate_function = self.aggregate_options.get(aggregate, "")
        if not aggregates.alias and column == "*":
            aggregate_string = self.aggregate_string_without_alias()
        else:
            aggregate_string = self.aggregate_string_with_alias()

        sql += (
            aggregate_string.format(
                aggregate_function=aggregate_function,
                column="*" if column == "*" else self._table_column_string(column),
                alias=self.process_alias(aggregates.alias or column),
            )
            + ", "
        )

    return sql


def _grammar_process_order_by(self):
    """Compile ORDER BY clause.

    Automatically omits ORDER BY when aggregates are present without
    GROUP BY, since PostgreSQL (and SQL standard) rejects ORDER BY in
    aggregate-only queries.

    ROOT-CAUSE NOTE (frontend_stress_log scenario 2, cycle 1):
    Pre-fix the comma separator was added only INSIDE the
    non-raw branch (``if order_crit: order_crit += ", "``).
    Two consecutive ``order_by_raw`` calls therefore concatenated
    without any separator — e.g.
    ``order_by_raw("review_count DESC NULLS LAST")`` followed by
    ``order_by_raw("rating DESC NULLS LAST")`` rendered as
    ``review_count DESC NULLS LASTrating DESC NULLS LAST``,
    triggering a Postgres ``syntax error at or near "NULLS"``
    and a 500 with the full traceback leaked to the response
    body. Surfaced live via ``?sort_by=popular`` (PopularSorter
    chains two raws). The fix lifts the separator to the top of
    the loop so it fires on every iteration after the first,
    regardless of raw vs. typed.
    """
    sql = ""
    if self._aggregates and not self._group_by:
        return sql
    if self._order_by:
        order_crit = ""
        for order_bys in self._order_by:
            # Comma separator before EVERY clause after the
            # first — applies uniformly to raw and non-raw so
            # consecutive ``order_by_raw`` calls get the comma
            # they need (the pre-fix code added it only in the
            # non-raw branch).
            if order_crit:
                order_crit += ", "
            if order_bys.raw:
                order_crit += order_bys.column
                if not isinstance(order_bys.bindings, (list, tuple)):
                    raise InvalidArgumentException(
                        f"Bindings must be tuple or list. Received {type(order_bys.bindings)}"
                    )

                if order_bys.bindings:
                    self.add_binding(*order_bys.bindings)

                continue

            column = order_bys.column
            direction = order_bys.direction
            if "." in column:
                column_string = self._table_column_string(column)
            else:
                column_string = self.column_string().format(column=column, separator="")
            order_crit += self.order_by_format().format(
                column=column_string,
                direction=direction.upper(),
            )

        sql += self.order_by_string().format(order_columns=order_crit)
    return sql


def _grammar_process_group_by(self):
    """
    Compiles a group by for a query expression.

    Returns:
        self
    """
    columns = []
    for group_by in self._group_by:
        if group_by.raw:
            if group_by.bindings:
                self.add_binding(*group_by.bindings)

            # Raw entries join the same list — returning early here
            # used to discard every other GROUP BY column.
            columns.append(group_by.column)
        else:
            columns.append(self._table_column_string(group_by.column))

    if columns:
        return " GROUP BY {column}".format(column=", ".join(columns))

    return ""


def _grammar_process_alias(self, column):
    """
    Compiles an alias for a column.

    Arguments:
        column {string} -- The name of the column.

    Returns:
        self
    """
    return column


def _grammar_process_table(self, table):
    """
    Compiles a given table name.

    Arguments:
        table {string} -- The table name to compile.

    Returns:
        self
    """
    if not table:
        return ""

    if isinstance(table, str):
        return ".".join(
            self.table_string().format(
                table=t,
                database=self._connection_details.get("database", ""),
                prefix=self._connection_details.get("prefix", ""),
            )
            for t in table.split(".")
        )

    if table.raw:
        return table.name

    return ".".join(
        self.table_string().format(
            table=t,
            database=self._connection_details.get("database", ""),
            prefix=self._connection_details.get("prefix", ""),
        )
        for t in table.name.split(".")
    )


def _grammar_process_limit(self):
    """
    Compiles the limit expression.

    ``self._limit`` uses ``False`` as the "no limit set" sentinel
    (initial state), so ``limit(0)`` — a legitimate "return zero
    rows" request — must render. A blanket ``if not self._limit``
    truthiness check treated 0 and False identically and silently
    upgraded ``LIMIT 0`` to "no limit", returning every row in the
    table when the caller asked for none.

    Returns:
        self
    """
    if self._limit is False or self._limit is None:
        return ""

    return self.limit_string(offset=self._offset).format(limit=self._limit)


def _grammar_process_offset(self):
    """
    Compiles the offset expression.

    ``OFFSET 0`` is the SQL default; emitting it is harmless but
    noisy, so keep the falsy short-circuit for the zero case.
    ``False``/``None`` are the "unset" sentinels.

    Returns:
        self
    """
    if self._offset is False or self._offset is None or self._offset == 0:
        return ""

    return self.offset_string().format(offset=self._offset, limit=self._limit or 1)


def _grammar_process_locks(self):
    base = self.locks.get(self.lock, "")
    if not base:
        return base
    return base + self._lock_modifier_sql(base)


def _lock_modifier_sql(self, base_lock: str) -> str:
    """Render SKIP LOCKED / NOWAIT / OF modifiers for a row lock.

    Only applies to ``FOR UPDATE`` / ``FOR SHARE`` style locks. SQLite's
    base lock string is empty, so modifiers degrade to no lock instead of
    emitting invalid SQL. Subclasses may override dialect syntax.
    """
    modifier = getattr(
        self, "_lock_modifier", {"skip_locked": False, "nowait": False, "of": []}
    )
    if not base_lock.upper().startswith("FOR "):
        return ""

    sql = ""
    of_tables = modifier.get("of") or []
    if of_tables:
        quoted = ", ".join(self.table_string().format(table=t) for t in of_tables)
        sql += f" OF {quoted}"
    if modifier.get("skip_locked"):
        sql += " SKIP LOCKED"
    elif modifier.get("nowait"):
        sql += " NOWAIT"
    return sql


def _grammar_process_having(self, qmark=False):
    """
    Compiles having expression.

    Keyword Arguments:
        qmark {bool} -- Whether or not to use Qmark (default: {False})

    Returns:
        self
    """
    sql = ""
    for having in self._having:
        column = having.column
        equality = having.equality
        value = having.value
        raw = having.raw

        if not equality and not value:
            sql_string = self.having_string()
            compiled_value = ""
        else:
            sql_string = self.having_equality_string()
            # Parameterize exactly like the where compiler — the
            # pre-fix path spliced the value into the SQL string
            # unescaped even on the executed (qmark) path.
            if qmark:
                compiled_value = "'?'"
                self.add_binding(value)
            else:
                compiled_value = self._compile_value(value)

        sql += sql_string.format(
            column=self._table_column_string(column) if raw is False else column,
            equality=equality,
            value=compiled_value,
        )

    return sql


def _grammar_process_wheres(self, qmark=False, strip_first_where=False):
    """
    Compiles the where expression.

    Keyword Arguments:
        qmark {bool} -- Whether or not to use Qmark. (default: {False})
        strip_first_where {bool} -- Whether or not to strip out the first where keyword.
        This is useful when using subselects (default: {False})

    Returns:
        self
    """
    sql = ""
    loop_count = 0
    for where in self._wheres:
        column = where.column
        equality = where.equality
        value = where.value
        value_type = where.value_type
        """
        Need to get a specific keyword here. This keyword either needs to be something like
        WHERE, AND, OR.

        Depending on the loop depends on the placement of the AND
        """
        if loop_count == 0:
            if strip_first_where:
                keyword = ""
            else:
                keyword = " " + self.first_where_string()
        elif (
            hasattr(where, "keyword")
            and isinstance(where.keyword, str)
            and where.keyword.lower() == "or"
        ):
            # Normalize custom where objects too: a strict ``== "or"``
            # check can silently downgrade uppercase OR to AND.
            keyword = " " + self.or_where_string()
        else:
            keyword = " " + self.additional_where_string()

        if where.raw:
            """If we have a raw query we just want to use the query supplied and don't need to
            compile anything."""
            sql += self.raw_query_string().format(keyword=keyword, query=where.column)

            if not isinstance(where.bindings, (list, tuple)):
                raise InvalidArgumentException(
                    f"Bindings must be tuple or list. Received {type(where.bindings)}"
                )

            if where.bindings:
                self.add_binding(*where.bindings)

            loop_count += 1

            continue

        if value_type == "expression":
            # ``where(F("a"), op, F("b"))`` — render BOTH sides through
            # the expression compiler so column references are quoted
            # identifiers and any literal operand is escaped as a value.
            # No %s binding is emitted for either side.
            left = (
                self.compile_expression(column)
                if self.is_column_expression(column)
                else self._table_column_string(column)
            )
            right = (
                self.compile_expression(value)
                if self.is_column_expression(value)
                else self._compile_value(value).strip()
            )
            sql += self.where_string().format(
                keyword=keyword,
                column=left,
                equality=equality.upper(),
                value=right,
            )
            loop_count += 1
            continue

        """The column is an easy compile
        """
        column = self._table_column_string(column)
        """
        Need to find which type of where string it is.

        If it is a WHERE NULL, WHERE EXISTS, WHERE `col` = 'val' etc
        """
        equality = equality.upper()

        if equality == "BETWEEN":
            low = where.low
            high = where.high
            if qmark:
                self.add_binding(low)
                self.add_binding(high)
                low = "?"
                high = "?"

            sql_string = self.between_string().format(
                low=self._compile_value(low),
                high=self._compile_value(high),
                column=self._table_column_string(where.column),
                keyword=keyword,
            )
        elif equality == "NOT BETWEEN":
            low = where.low
            high = where.high
            if qmark:
                self.add_binding(low)
                self.add_binding(high)
                low = "?"
                high = "?"

            sql_string = self.not_between_string().format(
                low=self._compile_value(low),
                high=self._compile_value(high),
                column=self._table_column_string(where.column),
                keyword=keyword,
            )
        elif value_type == "value_equals":
            sql_string = self.value_equal_string().format(
                value1=where.column,
                value2=where.value,
                keyword=keyword,
            )
        elif value_type == "NULL":
            sql_string = self.where_null_string()
        elif value_type == "DATE":
            sql_string = self.where_date_string()
        elif value_type == "NOT NULL":
            sql_string = self.where_not_null_string()
        elif equality == "EXISTS":
            sql_string = self.where_exists_string()
        elif equality == "NOT EXISTS":
            sql_string = self.where_not_exists_string()
        elif equality == "LIKE":
            sql_string = self.where_like_string()
        elif equality == "REGEXP":
            sql_string = self.where_regexp_string()
        elif equality == "NOT REGEXP":
            sql_string = self.where_not_regexp_string()
        elif equality == "NOT LIKE":
            sql_string = self.where_not_like_string()
        else:
            sql_string = self.where_string()

        """If the value should actually be a sub query then we need to wrap it in a query here
        """
        if isinstance(value, SubGroupExpression):
            grammar = value.builder.get_grammar()
            query_value = (
                self.subquery_string()
                .format(
                    query=grammar.process_wheres(
                        qmark=qmark,
                        strip_first_where=True,
                    )
                )
                .replace("(  ", "(")
            )
            if grammar._bindings:
                self.add_binding(*grammar._bindings)
            sql_string = self.where_group_string()
        elif isinstance(value, SubSelectExpression):
            if qmark:
                query_from_builder = value.builder.to_qmark()
                if value.builder._bindings:
                    self.add_binding(*value.builder._bindings)
            else:
                query_from_builder = value.builder.to_sql()
            query_value = self.subquery_string().format(query=query_from_builder)
        elif isinstance(value, list):
            query_value = "("
            for val in value:
                if qmark:
                    query_value += "'?', "
                    self.add_binding(val)
                else:
                    query_value += self.value_string().format(value=val, separator=",")
            query_value = query_value.rstrip(",").rstrip(", ") + ")"
        elif value is True and value_type != "NOT NULL":
            sql_string = self.get_true_column_string()
            query_value = 1
        elif value is False and value_type != "NOT NULL":
            sql_string = self.get_false_column_string()
            query_value = 0
        elif qmark and value_type != "column":
            query_value = "'?'"
            if (
                value is not True
                and value_type != "value_equals"
                and value_type != "NULL"
                and value_type != "BETWEEN"
            ):
                self.add_binding(value)
        elif value_type == "value":
            if qmark:
                query_value = "'?'"
            else:
                query_value = self.value_string().format(value=value, separator="")

            self.add_binding(value)
        elif value_type == "column":
            query_value = self._table_column_string(column=value, separator="")
        elif value_type == "DATE":
            query_value = self.value_string().format(value=value, separator="")
        elif value_type == "having":
            query_value = self._table_column_string(column=value, separator="")
        else:
            query_value = ""

        sql += sql_string.format(
            keyword=keyword,
            column=column,
            equality=equality,
            value=query_value,
        )

        loop_count += 1

    return sql
