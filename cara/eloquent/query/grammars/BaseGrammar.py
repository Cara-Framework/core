from __future__ import annotations

import re

from cara.eloquent.expressions import F, Greatest, Least, Operation
from cara.exceptions import QueryException

from . import _BaseGrammarClauses, _BaseGrammarCompilation, _BaseGrammarValues

_MULTI_SPACE_RE = re.compile(r" +")


class BaseGrammar:
    """
    The keys in this dictionary is how the ORM will reference these aggregates.

    The values on the right are the matching functions for the grammar

    Returns:
        [type] -- [description]
    """

    table = "users"

    __init__ = _BaseGrammarCompilation._grammar_initialize
    compile = _BaseGrammarCompilation._grammar_compile
    _compile_select = _BaseGrammarCompilation._compile_select
    _compile_update = _BaseGrammarCompilation._compile_update
    _compile_insert = _BaseGrammarCompilation._compile_insert
    _compile_bulk_create = _BaseGrammarCompilation._compile_bulk_create
    columnize_bulk_columns = _BaseGrammarCompilation._grammar_columnize_bulk_columns
    columnize_bulk_values = _BaseGrammarCompilation._grammar_columnize_bulk_values
    process_value_string = _BaseGrammarCompilation._grammar_process_value_string
    _compile_delete = _BaseGrammarCompilation._compile_delete
    _get_multiple_columns = _BaseGrammarCompilation._get_multiple_columns
    process_joins = _BaseGrammarCompilation._grammar_process_joins
    _compile_key_value_equals = _BaseGrammarCompilation._compile_key_value_equals

    process_aggregates = _BaseGrammarClauses._grammar_process_aggregates
    process_order_by = _BaseGrammarClauses._grammar_process_order_by
    process_group_by = _BaseGrammarClauses._grammar_process_group_by
    process_alias = _BaseGrammarClauses._grammar_process_alias
    process_table = _BaseGrammarClauses._grammar_process_table
    process_limit = _BaseGrammarClauses._grammar_process_limit
    process_offset = _BaseGrammarClauses._grammar_process_offset
    process_locks = _BaseGrammarClauses._grammar_process_locks
    _lock_modifier_sql = _BaseGrammarClauses._lock_modifier_sql
    process_having = _BaseGrammarClauses._grammar_process_having
    process_wheres = _BaseGrammarClauses._grammar_process_wheres

    get_true_column_string = _BaseGrammarValues._grammar_get_true_column_string

    get_false_column_string = _BaseGrammarValues._grammar_get_false_column_string

    add_binding = _BaseGrammarValues._grammar_add_binding

    wrap_table = _BaseGrammarValues._grammar_wrap_table

    def to_sql(self):
        """Clean up and return the compiled SQL string.

        Dialect-neutral: both grammars carried a byte-identical override of
        this, so the shape lives here.
        """
        return _MULTI_SPACE_RE.sub(" ", self._sql.strip().replace(",)", ")"))

    def to_qmark(self):
        """Clean up and return the compiled SQL string (qmark variant)."""
        return _MULTI_SPACE_RE.sub(" ", self._sql.strip())

    process_columns = _BaseGrammarValues._grammar_process_columns

    process_values = _BaseGrammarValues._grammar_process_values

    process_column = _BaseGrammarValues._grammar_process_column

    _table_column_string = _BaseGrammarValues._table_column_string

    _compile_value = _BaseGrammarValues._compile_value

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
        return isinstance(value, (F, Operation, Greatest, Least))

    compile_expression = _BaseGrammarValues._grammar_compile_expression

    _compile_expression_operand = _BaseGrammarValues._compile_expression_operand

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

    where_regexp_string = _BaseGrammarValues._grammar_where_regexp_string

    where_not_regexp_string = _BaseGrammarValues._grammar_where_not_regexp_string

    _compile_upsert = _BaseGrammarValues._compile_upsert

    def upsert_format(self):
        raise QueryException(
            f"upsert() is not implemented for the {self.__class__.__name__} dialect."
        )

    def upsert_do_nothing_format(self):
        raise QueryException(
            f"upsert() is not implemented for the {self.__class__.__name__} dialect."
        )
