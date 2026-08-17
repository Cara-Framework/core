"""Result retrieval and pagination for ``QueryBuilder``."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from typing import Any, Self

from cara.eloquent.Integrity import is_unique_violation
from cara.exceptions import (
    Http404Exception,
    InvalidArgumentException,
    ModelNotFoundException,
    MultipleRecordsFoundException,
)
from cara.facades import Log
from cara.support import Collection

from ..pagination import LengthAwarePaginator, SimplePaginator

_logger = logging.getLogger("cara.eloquent.query")
QueryBuilder: type


def _bind_query_builder(builder_type: type) -> None:
    global QueryBuilder
    QueryBuilder = builder_type


def _qb_first(self, fields: list[str] | None = None, query: bool = False) -> Any:
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


def _qb_first_or_create(self, wheres, creates: dict | None = None):
    """Get the first record matching the attributes or create it.

    The speculative INSERT owns a transaction boundary so a concurrent
    unique-key loser can roll back before re-querying the winner. This also
    covers relationship builders, which use this QueryBuilder-level variant
    instead of ``Model.first_or_create``.

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

    if record:
        return record
    try:
        with self._db_manager.transaction(self.connection):
            return self.create(total, id_key=self.get_primary_key())
    except Exception as exc:
        if not is_unique_violation(exc):
            raise
        again = self.where(wheres).first()
        if again is not None:
            return again
        raise


def _qb_sole(self, query=False):
    """Gets the only record matching a given criteria."""

    result = self.limit(2).get()

    if result.is_empty():
        raise ModelNotFoundException()

    if result.count() > 1:
        raise MultipleRecordsFoundException()

    return result.first()


def _qb_sole_value(self, column: str, query=False):
    return self.sole()[column]


def _qb_first_where(self, column, *args):
    """Gets the first record with the given key / value pair."""
    if not args:
        return self.where_not_null(column).first()
    return self.where(column, *args).first()


def _qb_last(self, column=None, query=False):
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


def _qb_get_eager_load_result(self, related, collection):
    return related.eager_load_from_collection(collection)


def _qb_find(self, record_id: Any, column: str | None = None, query: bool = False) -> Any:
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


def _qb_find_or(
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


def _qb_find_or_fail(self, record_id, column=None):
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


def _qb_find_or_404(self, record_id, column=None):
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


def _qb_first_or_fail(self, query=False):
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


def _qb_get_primary_key(self):
    return self._model.get_primary_key()


def _qb_prepare_result(self, result, collection=False):
    if self._model and result:
        # eager load here
        hydrated_model = self._model.hydrate(result)

        if (
            self._eager_relation.relations
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


def _qb_normalize_eager_specs(raw_list):
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


def _qb_register_relationships_to_model(
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
    if isinstance(hydrated_model, Collection) and isinstance(related_result, Collection):
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


def _qb_map_related(self, related_result, related):
    return related.map_related(related_result)


def _qb_all(self, selects=None, query=False):
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


def _qb_get(self, selects: list[str] | None = None) -> Any:
    """
    Run the SELECT query and return a collection of results.

    Returns:
        Collection of Model instances or list of dicts.
    """
    selects = selects or []
    self.select(*selects)
    result = self.new_connection().query(self.to_qmark(), self._bindings)

    return self.prepare_result(result, collection=True)


def _qb_new_connection(self) -> Any:
    if self._connection:
        return self._connection

    self._connection = self._db_manager.create_connection_instance(
        self.connection, self._schema
    )
    return self._connection


def _qb_get_connection(self) -> Any:
    return self._connection


def _qb_without_eager(self) -> Self:
    self._should_eager = False
    return self


def _qb_with(self, *eagers) -> Self:
    try:
        self._eager_relation.register(*eagers)
    except Exception as e:
        Log.error("Eager relation register failed: %s", str(e))
        raise
    return self


def _qb_paginate(self, per_page, page=1):
    # Sanitise inputs — coerce to int, clamp to safe bounds.
    try:
        per_page = max(1, min(int(per_page), self._MAX_PER_PAGE))
    except TypeError, ValueError:
        per_page = 15
    try:
        page = max(1, min(int(page), self._MAX_PAGE))
    except TypeError, ValueError:
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
            _logger.warning("primary key detection failed for pagination", exc_info=True)
            pk = None
        if pk:
            self.order_by(pk, "ASC")

    result = self.limit(per_page).offset(offset).get()
    total = new_from_builder.count()

    paginator = LengthAwarePaginator(result, per_page, page, total)
    return paginator


def _qb_simple_paginate(self, per_page, page=1):
    # Sanitise inputs — coerce to int, clamp to safe bounds.
    try:
        per_page = max(1, min(int(per_page), self._MAX_PER_PAGE))
    except TypeError, ValueError:
        per_page = 15
    try:
        page = max(1, min(int(page), self._MAX_PAGE))
    except TypeError, ValueError:
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
