"""Value, expression and schema SQL operations for ``BaseGrammar``."""

from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401

from cara.eloquent.expressions import (
    F,
    Greatest,
    Least,
    Operation,
    SelectExpression,
    SubGroupExpression,
)


def _grammar_get_true_column_string(self):
    return "{keyword} {column} = '1'"


def _grammar_get_false_column_string(self):
    return "{keyword} {column} = '0'"


def _grammar_add_binding(self, *bindings):
    """
    Adds one or more bindings to the bindings tuple.

    Arguments:
        binding {string} -- A value to bind.
    """
    self._bindings += bindings


def _grammar_column_exists(self, column) -> Self:
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


def _grammar_table_exists(self) -> Self:
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


def _grammar_wrap_table(self, table_name):
    return self.table_string().format(table=table_name)


def _grammar_process_exists(self):
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


def _grammar_process_columns(self, separator="", action="select", qmark=False):
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


def _grammar_process_values(self, separator="", qmark=False):
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


def _grammar_process_column(self, column, separator=""):
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


def _grammar_compile_expression(self, expr) -> str:
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
    if self.is_column_expression(operand):
        rendered = self.compile_expression(operand)
        if isinstance(operand, Operation):
            return f"({rendered})"
        return rendered

    return self._compile_value(operand).strip()


def _grammar_drop_table(self, table) -> Self:
    """
    Specifies a drop table expression.

    Arguments:
        table {string} -- The table to drop.

    Returns:
        self
    """
    self._sql = self.drop_table_string().format(table=self.process_column(table))
    return self


def _grammar_drop_table_if_exists(self, table) -> Self:
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


def _grammar_rename_table(self, current_table_name, new_table_name) -> Self:
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


def _grammar_where_regexp_string(self):
    return "{keyword} {column} REGEXP {value}"


def _grammar_where_not_regexp_string(self):
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
