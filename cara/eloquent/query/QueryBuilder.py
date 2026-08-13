from __future__ import annotations

import inspect
import json
import logging
from collections.abc import Callable
from copy import deepcopy
from datetime import datetime
from typing import Any

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401

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
from cara.exceptions import (
    Http404Exception,
    InvalidArgumentException,
    ModelNotFoundException,
    MultipleRecordsFoundException,
    QueryException,
)
from cara.facades import DB
from cara.support import Collection

from ..observers import ObservesEvents
from ..pagination import LengthAwarePaginator, SimplePaginator
from ..schema import Schema
from ..scopes import BaseScope
from . import (
    _QueryAggregation,
    _QueryConstraints,
    _QueryCursorPagination,
    _QueryExecution,
    _QueryIteration,
    _QueryPredicates,
    _QueryRelations,
    _QueryResults,
    _QuerySelection,
)
from ._QuerySafety import ORDER_BY_COLUMN_RE as _ORDER_BY_COLUMN_RE
from ._QuerySafety import _is_column_expression
from .EagerRelations import EagerRelations
from .TransactionContext import TransactionContext

_logger = logging.getLogger("cara.eloquent.query")


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
        database_manager: Any = None,
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

        # Resolver-created builders receive their manager directly; model-created
        # builders resolve the application-owned manager through the DB facade.
        self._db_manager = database_manager or DB

        if not self._connection_details:
            self._connection_details = self._db_manager.get_connection_details()

        self.on(connection)

        if grammar:
            self.grammar = grammar

        if connection_class:
            self.connection_class = connection_class

    _set_creates_related = _QuerySelection._qb_set_creates_related
    set_schema = _QuerySelection._qb_set_schema
    shared_lock = _QuerySelection._qb_shared_lock
    lock_for_update = _QuerySelection._qb_lock_for_update
    make_lock = _QuerySelection._qb_make_lock
    reset = _QuerySelection._qb_reset
    get_connection_information = _QuerySelection._qb_get_connection_information
    table = _QuerySelection._qb_table
    get_table_name = _QuerySelection._qb_get_table_name
    begin = _QuerySelection._qb_begin
    get_schema_builder = _QuerySelection._qb_get_schema_builder
    commit = _QuerySelection._qb_commit
    rollback = _QuerySelection._qb_rollback
    transaction = _QuerySelection._qb_transaction
    set_scope = _QuerySelection._qb_set_scope
    set_global_scope = _QuerySelection._qb_set_global_scope
    without_global_scopes = _QuerySelection._qb_without_global_scopes
    remove_global_scope = _QuerySelection._qb_remove_global_scope
    __getattr__ = _QuerySelection._qb_getattr
    on = _QuerySelection._qb_on
    select = _QuerySelection._qb_select
    distinct = _QuerySelection._qb_distinct
    add_select = _QuerySelection._qb_add_select
    statement = _QuerySelection._qb_statement
    select_raw = _QuerySelection._qb_select_raw
    _rendering_grammar = _QuerySelection._qb_rendering_grammar
    _quote_window_identifier = _QuerySelection._qb_quote_window_identifier
    select_window = _QuerySelection._qb_select_window
    select_greatest = _QuerySelection._qb_select_greatest
    select_least = _QuerySelection._qb_select_least

    _select_function_expression = _QueryPredicates._qb_select_function_expression
    get_processor = _QueryPredicates._qb_get_processor
    bulk_create = _QueryPredicates._qb_bulk_create
    create = _QueryPredicates._qb_create
    hydrate = _QueryPredicates._qb_hydrate
    delete = _QueryPredicates._qb_delete
    where = _QueryPredicates._qb_where
    where_from_builder = _QueryPredicates._qb_where_from_builder
    where_like = _QueryPredicates._qb_where_like
    where_not_like = _QueryPredicates._qb_where_not_like
    where_raw = _QueryPredicates._qb_where_raw
    or_where_raw = _QueryPredicates._qb_or_where_raw
    _escape_json_path_segment = staticmethod(
        _QueryPredicates._qb_escape_json_path_segment
    )
    _json_path_sql = staticmethod(_QueryPredicates._qb_json_path_sql)
    where_json_contains = _QueryPredicates._qb_where_json_contains
    or_where_json_contains = _QueryPredicates._qb_or_where_json_contains
    where_json_doesnt_contain = _QueryPredicates._qb_where_json_doesnt_contain
    where_json_path = _QueryPredicates._qb_where_json_path
    or_where_json_path = _QueryPredicates._qb_or_where_json_path
    where_json_length = _QueryPredicates._qb_where_json_length
    where_json_key_exists = _QueryPredicates._qb_where_json_key_exists

    or_where = _QueryRelations._qb_or_where
    where_exists = _QueryRelations._qb_where_exists
    or_where_exists = _QueryRelations._qb_or_where_exists
    where_not_exists = _QueryRelations._qb_where_not_exists
    or_where_not_exists = _QueryRelations._qb_or_where_not_exists
    having = _QueryRelations._qb_having
    having_raw = _QueryRelations._qb_having_raw
    where_null = _QueryRelations._qb_where_null
    or_where_null = _QueryRelations._qb_or_where_null
    where_not_null = _QueryRelations._qb_where_not_null
    _get_date_string = _QueryRelations._qb_get_date_string
    where_date = _QueryRelations._qb_where_date
    or_where_date = _QueryRelations._qb_or_where_date
    between = _QueryRelations._qb_between
    where_between = _QueryRelations._qb_where_between
    where_not_between = _QueryRelations._qb_where_not_between
    not_between = _QueryRelations._qb_not_between
    where_in = _QueryRelations._qb_where_in
    get_relation = _QueryRelations._qb_get_relation
    has = _QueryRelations._qb_has
    or_has = _QueryRelations._qb_or_has

    doesnt_have = _QueryConstraints._qb_doesnt_have
    or_doesnt_have = _QueryConstraints._qb_or_doesnt_have
    where_has = _QueryConstraints._qb_where_has
    or_where_has = _QueryConstraints._qb_or_where_has
    where_doesnt_have = _QueryConstraints._qb_where_doesnt_have
    or_where_doesnt_have = _QueryConstraints._qb_or_where_doesnt_have
    with_count = _QueryConstraints._qb_with_count
    _resolve_relation_descriptor = _QueryConstraints._qb_resolve_relation_descriptor
    with_sum = _QueryConstraints._qb_with_sum
    with_avg = _QueryConstraints._qb_with_avg
    with_min = _QueryConstraints._qb_with_min
    with_max = _QueryConstraints._qb_with_max
    tap = _QueryConstraints._qb_tap
    pipe = _QueryConstraints._qb_pipe
    where_not_in = _QueryConstraints._qb_where_not_in
    join = _QueryConstraints._qb_join
    left_join = _QueryConstraints._qb_left_join
    right_join = _QueryConstraints._qb_right_join
    joins = _QueryConstraints._qb_joins
    join_on = _QueryConstraints._qb_join_on
    where_column = _QueryConstraints._qb_where_column

    limit = _QueryExecution._qb_limit
    offset = _QueryExecution._qb_offset
    update = _QueryExecution._qb_update
    force_update = _QueryExecution._qb_force_update
    set_updates = _QueryExecution._qb_set_updates
    increment = _QueryExecution._qb_increment
    decrement = _QueryExecution._qb_decrement
    sum = _QueryExecution._qb_sum
    count = _QueryExecution._qb_count
    max = _QueryExecution._qb_max
    order_by = _QueryExecution._qb_order_by
    order_by_raw = _QueryExecution._qb_order_by_raw
    group_by = _QueryExecution._qb_group_by
    group_by_raw = _QueryExecution._qb_group_by_raw
    aggregate = _QueryExecution._qb_aggregate
    _run_aggregate = _QueryExecution._qb_run_aggregate

    first = _QueryResults._qb_first
    first_or_create = _QueryResults._qb_first_or_create
    sole = _QueryResults._qb_sole
    sole_value = _QueryResults._qb_sole_value
    first_where = _QueryResults._qb_first_where
    last = _QueryResults._qb_last
    _get_eager_load_result = _QueryResults._qb_get_eager_load_result
    find = _QueryResults._qb_find
    find_or = _QueryResults._qb_find_or
    find_or_fail = _QueryResults._qb_find_or_fail
    find_or_404 = _QueryResults._qb_find_or_404
    first_or_fail = _QueryResults._qb_first_or_fail
    get_primary_key = _QueryResults._qb_get_primary_key
    prepare_result = _QueryResults._qb_prepare_result
    _normalize_eager_specs = staticmethod(_QueryResults._qb_normalize_eager_specs)
    _register_relationships_to_model = _QueryResults._qb_register_relationships_to_model
    _map_related = _QueryResults._qb_map_related
    all = _QueryResults._qb_all
    get = _QueryResults._qb_get
    new_connection = _QueryResults._qb_new_connection
    get_connection = _QueryResults._qb_get_connection
    without_eager = _QueryResults._qb_without_eager
    with_ = _QueryResults._qb_with
    paginate = _QueryResults._qb_paginate
    simple_paginate = _QueryResults._qb_simple_paginate

    set_action = _QueryAggregation._qb_set_action
    get_grammar = _QueryAggregation._qb_get_grammar
    to_sql = _QueryAggregation._qb_to_sql
    explain = _QueryAggregation._qb_explain
    dump_sql = _QueryAggregation._qb_dump_sql
    debug_sql = _QueryAggregation._qb_debug_sql
    run_scopes = _QueryAggregation._qb_run_scopes
    to_qmark = _QueryAggregation._qb_to_qmark
    _append_unions_sql = _QueryAggregation._qb_append_unions_sql
    new = _QueryAggregation._qb_new
    avg = _QueryAggregation._qb_avg
    min = _QueryAggregation._qb_min
    _extract_operator_value = _QueryAggregation._qb_extract_operator_value
    __call__ = _QueryAggregation._qb_call
    macro = _QueryAggregation._qb_macro
    when = _QueryAggregation._qb_when
    unless = _QueryAggregation._qb_unless
    truncate = _QueryAggregation._qb_truncate
    exists = _QueryAggregation._qb_exists
    doesnt_exist = _QueryAggregation._qb_doesnt_exist
    in_random_order = _QueryAggregation._qb_in_random_order
    new_from_builder = _QueryAggregation._qb_new_from_builder
    clone = _QueryAggregation._qb_clone
    get_table_columns = _QueryAggregation._qb_get_table_columns
    get_schema = _QueryAggregation._qb_get_schema
    latest = _QueryAggregation._qb_latest
    oldest = _QueryAggregation._qb_oldest
    value = _QueryAggregation._qb_value
    pluck = _QueryAggregation._qb_pluck

    chunk = _QueryIteration._qb_chunk
    upsert = _QueryIteration._qb_upsert
    bulk_update = _QueryIteration._qb_bulk_update
    cursor = _QueryIteration._qb_cursor
    union = _QueryIteration._qb_union
    union_all = _QueryIteration._qb_union_all
    chunk_by_id = _QueryIteration._qb_chunk_by_id
    lazy = _QueryIteration._qb_lazy
    lazy_by_id = _QueryIteration._qb_lazy_by_id

    # ===== CURSOR PAGINATE =====
    cursor_paginate = _QueryCursorPagination._qb_cursor_paginate


_QueryCursorPagination._bind_query_builder(QueryBuilder)
_QueryIteration._bind_query_builder(QueryBuilder)
_QueryAggregation._bind_query_builder(QueryBuilder)
_QueryResults._bind_query_builder(QueryBuilder)
_QueryExecution._bind_query_builder(QueryBuilder)
_QueryConstraints._bind_query_builder(QueryBuilder)
_QueryRelations._bind_query_builder(QueryBuilder)
_QueryPredicates._bind_query_builder(QueryBuilder)
_QuerySelection._bind_query_builder(QueryBuilder)
