"""Value, expression and schema SQL operations for ``BaseGrammar``."""

from __future__ import annotations

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


def _grammar_wrap_table(self, table_name):
    return self.table_string().format(table=table_name)


def _grammar_process_columns(self, separator="", qmark=False):
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
        column=self.escape_identifier(column),
        separator=separator,
        table=table or self.table,
    )


def _grammar_escape_identifier(self, name):
    """Make ``name`` safe to place between the dialect's identifier quotes.

    Only strings are touched: ``None`` (no table qualifier) passes through.
    """
    if not isinstance(name, str):
        return name
    quote = self.identifier_quote
    return name.replace(quote, quote + quote)


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

    # The one choke point every builder identifier (``where({key: v})``,
    # ``group_by``, join ``on``, update keys, returning, aggregates) passes
    # through. The templates wrap the name in the dialect's quote character
    # but never escaped it, so ``where({'x"; DROP TABLE users; --': 1})``
    # closed the identifier and ran the rest as SQL. Doubling an embedded
    # quote is the SQL standard escape inside a quoted identifier; a name
    # without one (every ordinary column) compiles byte-identically.
    column = self.escape_identifier(column)
    table = self.escape_identifier(table)

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
    operand as a literal value (``_compile_expression_operand``). Nested
    ``Operation`` nodes are wrapped in parentheses so SQL precedence is
    explicit.

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
    return self._compile_expression_operand(expr)


def _compile_expression_operand(self, operand) -> str:
    """Render a single operand of an expression tree.

    Column-reference nodes recurse through ``compile_expression``;
    nested ``Operation`` trees additionally get parenthesised so the
    emitted SQL reflects the Python composition order. Anything else is
    a Python literal and is escaped as a value.

    ROOT CAUSE: "escaped" was only ever the docstring. The literal was
    spliced in as ``'{value}'`` with its own quotes untouched — on the
    executed path too, where nothing binds an expression operand — so
    ``where(F("name"), "=", "x' OR '1'='1")`` matched every row and
    ``"o'brien"`` was a syntax error. Doubling the single quote is the whole
    of SQL's string-literal escaping on both supported dialects (PostgreSQL
    with its default ``standard_conforming_strings``).
    """
    if self.is_column_expression(operand):
        rendered = self.compile_expression(operand)
        if isinstance(operand, Operation):
            return f"({rendered})"
        return rendered

    return self.expression_literal(str(operand))


def _grammar_expression_literal(self, text: str) -> str:
    """``text`` as a SQL string literal inlined into the statement.

    The literal is part of the statement TEXT, and two readers downstream
    interpret that text again: the connection turns every ``'?'`` into a
    driver placeholder, and psycopg2 reads ``%`` as a format directive
    whenever the statement carries bindings. So ``F("code") == "?"`` consumed
    another clause's binding, and ``"50%"`` beside any bound value raised
    inside the driver. Each dialect's grammar spells those two characters in
    a form neither reader recognises (``PostgresGrammar`` / ``SQLiteGrammar``
    override this); the SQL type stays what a plain literal has. A literal
    without them compiles byte-identically to before.

    This base form — quotes doubled, nothing else — is for a dialect whose
    connection performs neither rewrite.
    """
    return self._compile_value(text.replace("'", "''")).strip()


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
    # The tenant scope's conflict guard: a conflicting row of another tenant
    # is left untouched instead of rewritten (``TenantScope``).
    guard = getattr(self, "_upsert_conflict_guard", None)
    if guard and update_columns:
        guarded = self.column_string().format(column=guard, separator="")
        update_columns += (
            f" WHERE {self.process_table(self.table)}.{guarded} = EXCLUDED.{guarded}"
        )

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
