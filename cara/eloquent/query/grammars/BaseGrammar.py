from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401

import re

from cara.eloquent.expressions import (
    JoinClause,
    OnClause,
    SelectExpression,
    SubGroupExpression,
    SubSelectExpression,
)
from cara.exceptions import InvalidArgumentException, QueryException

_MULTI_SPACE_RE = re.compile(r" +")


class BaseGrammar:
    """
    The keys in this dictionary is how the ORM will reference these aggregates.

    The values on the right are the matching functions for the grammar

    Returns:
        [type] -- [description]
    """

    table = "users"

    def __init__(
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

    def compile(self, action, qmark=False):
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

    def columnize_bulk_columns(self, columns=None):
        columns = columns or []
        return ", ".join(
            self.column_string().format(column=x, separator="") for x in columns
        ).rstrip(",")

    def columnize_bulk_values(self, columns=None, qmark=False):
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

    def process_value_string(self):
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

    # TODO: Columnize?
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

    def process_joins(self, qmark=False):
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

    # TODO: Clean
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

    def process_aggregates(self):
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

    def process_order_by(self):
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
                    column_string = self.column_string().format(
                        column=column, separator=""
                    )
                order_crit += self.order_by_format().format(
                    column=column_string,
                    direction=direction.upper(),
                )

            sql += self.order_by_string().format(order_columns=order_crit)
        return sql

    def process_group_by(self):
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

    def process_alias(self, column):
        """
        Compiles an alias for a column.

        Arguments:
            column {string} -- The name of the column.

        Returns:
            self
        """
        return column

    def process_table(self, table):
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

    def process_limit(self):
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

    def process_offset(self):
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

    def process_locks(self):
        base = self.locks.get(self.lock, "")
        if not base:
            return base
        return base + self._lock_modifier_sql(base)

    def _lock_modifier_sql(self, base_lock: str) -> str:
        """Render SKIP LOCKED / NOWAIT / OF modifiers for a row lock.

        Only applies to ``FOR UPDATE`` / ``FOR SHARE`` style locks (the
        Postgres / MySQL family). Grammars whose base lock string is empty
        (SQLite) or not a ``FOR ...`` clause (MSSQL hints) return nothing, so
        the modifiers degrade to a plain lock there rather than emitting
        invalid SQL. Subclasses may override for dialect-specific syntax.
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

    def process_having(self, qmark=False):
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

    def process_wheres(self, qmark=False, strip_first_where=False):
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
                # Case-insensitive: WhereBuilder emits "OR" while QueryBuilder
                # emits "or". Both must route to the OR branch — a strict
                # ``== "or"`` check silently downgraded every WhereBuilder
                # ``or_where`` to AND, producing wrong result sets.
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
                        query_value += self.value_string().format(
                            value=val, separator=","
                        )
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

    def get_true_column_string(self):
        return "{keyword} {column} = '1'"

    def get_false_column_string(self):
        return "{keyword} {column} = '0'"

    def add_binding(self, *bindings):
        """
        Adds one or more bindings to the bindings tuple.

        Arguments:
            binding {string} -- A value to bind.
        """
        self._bindings += bindings

    def column_exists(self, column) -> Self:
        """
        Check if a column exists.

        Arguments:
            column {string} -- The name of the column to check for existence.

        Returns:
            self
        """
        self._column = column
        self._sql = self.process_exists()
        return self

    def table_exists(self) -> Self:
        """
        Checks if a table exists.

        Returns:
            self
        """
        self._sql = self.table_exists_string().format(
            table=self.process_table(self.table),
            database=self.database,
            clean_table=self.table,
        )
        return self

    def wrap_table(self, table_name):
        return self.table_string().format(table=table_name)

    def process_exists(self):
        """
        Specifies the column exists expression.

        Returns:
            self
        """
        return self.column_exists_string().format(
            table=self.process_table(self.table),
            clean_table=self.table,
            value=self._compile_value(self._column),
        )

    def to_sql(self):
        """Clean up and return the compiled SQL string."""
        return _MULTI_SPACE_RE.sub(" ", self._sql.strip())

    def to_qmark(self):
        """Clean up and return the compiled SQL string (qmark variant)."""
        return _MULTI_SPACE_RE.sub(" ", self._sql.strip())

    # TODO: Inspect this can't just be used by another method. seems duplicative
    def process_columns(self, separator="", action="select", qmark=False):
        """
        Specifies the columns in a selection expression.

        Keyword Arguments:
            separator {str} -- The separator used between columns (default: {""})

        Returns:
            self
        """
        sql = ""
        for column in self._columns:
            alias = None
            if isinstance(column, SelectExpression):
                alias = column.alias
                if column.raw:
                    sql += column.column + ", "
                    continue

                column = column.column

            if isinstance(column, SubGroupExpression):
                if qmark:
                    builder_sql = column.builder.to_qmark()
                    if column.builder._bindings:
                        self.add_binding(*column.builder._bindings)
                else:
                    builder_sql = column.builder.to_sql()
                sql += f"({builder_sql}) AS {column.alias}, "
                continue

            sql += self._table_column_string(column, alias=alias, separator=separator)

        if self._aggregates:
            sql += self.process_aggregates()

        if sql == "":
            return "*"

        return sql.rstrip(",").rstrip(", ")

    # TODO: Duplicative?
    def process_values(self, separator="", qmark=False):
        """
        Compiles column values for insert expressions.

        Keyword Arguments:
            separator {str} -- The separator used between columns (default: {""})

        Returns:
            self
        """
        sql = ""
        if self._columns == "*":
            return self._columns
        elif isinstance(self._columns, list):
            for c in self._columns:
                for _column, value in dict(c).items():
                    if qmark:
                        self.add_binding(value)
                        sql += f"'?'{separator}".strip()
                    else:
                        sql += self._compile_value(value, separator=separator)
        else:
            for _column, value in dict(self._columns).items():
                if qmark:
                    self.add_binding(value)
                    sql += f"'?'{separator}".strip()
                else:
                    sql += self._compile_value(value, separator=separator)

        if not qmark:
            return sql[:-2]

        return sql.rstrip(separator.strip())

    def process_column(self, column, separator=""):
        """
        Compiles a column into the column syntax.

        Arguments:
            column {string} -- The name of the column.

        Keyword Arguments:
            separator {string} -- The separator used between columns (default: {""})

        Returns:
            self
        """
        table = None
        if column and "." in column:
            table, column = column.split(".")
        return self.column_string().format(
            column=column,
            separator=separator,
            table=table or self.table,
        )

    def _table_column_string(self, column, alias=None, separator=""):
        """
        Compiles a column into the column syntax.

        Arguments:
            column {string} -- The name of the column.

        Keyword Arguments:
            separator {string} -- The separator used between columns (default: {""})

        Returns:
            self
        """
        table = None
        if column and "." in column:
            table, column = column.split(".")

        if column == "*":
            return self.column_strings.get("select_all").format(
                column=column,
                separator=separator,
                table=self.process_table(table or self.table),
            )

        if alias:
            alias_string = self.subquery_alias_string().format(alias=alias)
        return self.column_strings.get(self._action).format(
            column=column,
            separator=separator,
            alias=" " + alias_string if alias else "",
            table=self.process_table(table or self.table),
        )

    def _compile_value(self, value, separator=""):
        """
        Compiles a value using the value syntax.

        Arguments:
            value {string} -- The value to compile.

        Keyword Arguments:
            separator {string} -- The separator used between columns (default: {""})

        Returns:
            self
        """
        return self.value_string().format(value=value, separator=separator)

    # ── column-reference expressions (F / arithmetic / GREATEST / LEAST) ──

    @staticmethod
    def is_column_expression(value) -> bool:
        """True if ``value`` is one of the column-reference expression nodes
        (``F`` / ``Operation`` / ``Greatest`` / ``Least``).

        These render to *unparameterised* SQL with identifiers quoted by the
        grammar — never as a bound value — so callers and the update/where
        compilers can branch on them. Kept as a single predicate so the
        membership set lives in one place.
        """
        from cara.eloquent.expressions import F, Greatest, Least, Operation

        return isinstance(value, (F, Operation, Greatest, Least))

    def compile_expression(self, expr) -> str:
        """Render a column-reference expression tree to a SQL fragment.

        Walks ``F`` / ``Operation`` / ``Greatest`` / ``Least`` nodes,
        quoting every column reference as an identifier (via
        ``_table_column_string`` so ``table.col`` qualification and the
        grammar's quote chars are honoured) and escaping any non-expression
        operand as a literal value (via ``value_string`` — the same escape
        path the rest of the grammar uses). Nested ``Operation`` nodes are
        wrapped in parentheses so SQL precedence is explicit.

        This is the single rendering seam shared by F-style updates,
        ``where(F(...), op, F(...))`` filters, and the GREATEST/LEAST
        SELECT helpers — none of them bind expression operands as ``%s``.
        """
        from cara.eloquent.expressions import F, Greatest, Least, Operation

        if isinstance(expr, F):
            return self._table_column_string(expr.column, separator="")

        if isinstance(expr, Operation):
            left = self._compile_expression_operand(expr.left)
            right = self._compile_expression_operand(expr.right)
            return f"{left} {expr.operator} {right}"

        if isinstance(expr, (Greatest, Least)):
            rendered = ", ".join(
                self._compile_expression_operand(arg) for arg in expr.arguments
            )
            return f"{expr.function}({rendered})"

        # A bare literal handed straight to compile_expression — escape it
        # as a value so the fragment is still well-formed.
        return self._compile_value(expr).strip()

    def _compile_expression_operand(self, operand) -> str:
        """Render a single operand of an expression tree.

        Column-reference nodes recurse through ``compile_expression``;
        nested ``Operation`` trees additionally get parenthesised so the
        emitted SQL reflects the Python composition order. Anything else is
        a Python literal and is escaped as a value.
        """
        from cara.eloquent.expressions import Operation

        if self.is_column_expression(operand):
            rendered = self.compile_expression(operand)
            if isinstance(operand, Operation):
                return f"({rendered})"
            return rendered

        return self._compile_value(operand).strip()

    def drop_table(self, table) -> Self:
        """
        Specifies a drop table expression.

        Arguments:
            table {string} -- The table to drop.

        Returns:
            self
        """
        self._sql = self.drop_table_string().format(table=self.process_column(table))
        return self

    def drop_table_if_exists(self, table) -> Self:
        """
        Specifies a drop table if exists expression.

        Arguments:
            table {string} -- The name of the table to drop.

        Returns:
            self
        """
        self._sql = self.drop_table_if_exists_string().format(
            table=self.process_column(table)
        )
        return self

    def rename_table(self, current_table_name, new_table_name) -> Self:
        """
        Specifies a rename table expression.

        Arguments:
            current_table_name {string} -- The name of the table currently.
            new_table_name {string} -- The name you want to use now for the table.

        Returns:
            self
        """
        self._sql = self.rename_table_string().format(
            current_table_name=self.process_column(current_table_name),
            new_table_name=self.process_column(new_table_name),
        )
        return self

    def truncate_table(self, table, foreign_keys=False):
        """
        Specifies a truncate table expression.

        Arguments;
            table {string} -- The name of the table to truncate.

        Returns:
            self
        """
        raise NotImplementedError(
            f"'{self.__class__.__name__}' does not support truncating"
        )

    def where_regexp_string(self):
        return "{keyword} {column} REGEXP {value}"

    def where_not_regexp_string(self):
        return "{keyword} {column} NOT REGEXP {value}"

    def _compile_upsert(self, qmark=False):
        """
        Compiles an upsert expression using database-specific syntax.

        Returns:
            self
        """
        # Get all values from upsert data
        all_values = [list(record.values()) for record in self._upsert_values]

        # Rows are canonicalized upstream (QueryBuilder.upsert enforces
        # uniform keys), so the first record's keys ARE the column list.
        columns = list(self._upsert_values[0].keys()) if self._upsert_values else []

        # Build conflict columns string for ON CONFLICT clause
        conflict_columns = ", ".join(
            self.column_string().format(column=col, separator="")
            for col in self._upsert_unique_by
        )

        # Build update columns string (col = EXCLUDED.col) — identifiers
        # quoted through the grammar, not hardcoded double quotes.
        quoted_updates = (
            self.column_string().format(column=col, separator="")
            for col in self._upsert_update
        )
        update_columns = ", ".join(f"{col} = EXCLUDED.{col}" for col in quoted_updates)

        # An explicit empty update list is insert-if-missing: conflicting
        # rows are left untouched (DO NOTHING). ``DO UPDATE SET`` with an
        # empty SET list would be a syntax error.
        if self._upsert_update:
            template = self.upsert_format()
        else:
            template = self.upsert_do_nothing_format()

        self._sql = template.format(
            table=self.process_table(self.table),
            columns=self.columnize_bulk_columns(columns),
            values=self.columnize_bulk_values(all_values, qmark=qmark),
            conflict_columns=conflict_columns,
            update_columns=update_columns,
        )

        return self

    def upsert_format(self):
        raise QueryException(
            f"upsert() is not implemented for the {self.__class__.__name__} dialect."
        )

    def upsert_do_nothing_format(self):
        raise QueryException(
            f"upsert() is not implemented for the {self.__class__.__name__} dialect."
        )
