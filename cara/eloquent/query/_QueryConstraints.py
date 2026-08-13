"""Relationship, ordering and row-window constraints for ``QueryBuilder``."""

from __future__ import annotations

import inspect
import inspect as _inspect
import logging
from typing import Self

from cara.eloquent.expressions import (
    JoinClause,
    QueryExpression,
    SubSelectExpression,
)

_logger = logging.getLogger("cara.eloquent.query")
QueryBuilder: type


def _bind_query_builder(builder_type: type) -> None:
    global QueryBuilder
    QueryBuilder = builder_type


def _qb_doesnt_have(self, *relationships) -> Self:
    if not self._model:
        raise AttributeError(
            "You must specify a model in order to use the 'doesnt_have' relationship methods"
        )

    for relationship in relationships:
        if "." in relationship:
            last_builder = self._model.builder
            split_count = len(relationship.split("."))
            for index, split_relationship in enumerate(relationship.split(".")):
                related = last_builder.get_relation(split_relationship)
                if index + 1 == split_count:
                    last_builder = related.query_has(
                        last_builder,
                        method="where_exists",
                    )
                    continue

                last_builder = related.query_has(
                    last_builder,
                    method="where_not_exists",
                )
        else:
            related = self._resolve_relation_descriptor(relationship)
            related.query_has(self, method="where_not_exists")
    return self


def _qb_or_doesnt_have(self, *relationships) -> Self:
    if not self._model:
        raise AttributeError(
            "You must specify a model in order to use the 'doesnt_have' relationship methods"
        )

    for relationship in relationships:
        if "." in relationship:
            last_builder = self._model.builder
            split_count = len(relationship.split("."))
            for index, split_relationship in enumerate(relationship.split(".")):
                related = last_builder.get_relation(split_relationship)
                if index + 1 == split_count:
                    last_builder = related.query_has(
                        last_builder,
                        method="where_exists",
                    )
                    continue

                last_builder = related.query_has(
                    last_builder,
                    method="or_where_not_exists",
                )
        else:
            related = self._resolve_relation_descriptor(relationship)
            related.query_has(self, method="or_where_not_exists")
    return self


def _qb_where_has(self, relationship, callback) -> Self:
    if not self._model:
        raise AttributeError(
            "You must specify a model in order to use 'has' relationship methods"
        )

    if "." in relationship:
        last_builder = self._model.builder
        splits = relationship.split(".")
        split_count = len(splits)
        for index, split_relationship in enumerate(splits):
            related = last_builder.get_relation(split_relationship)

            if index + 1 == split_count:
                last_builder = related.query_where_exists(
                    last_builder,
                    callback,
                    method="where_exists",
                )
                continue
            last_builder = related.query_has(last_builder, method="where_exists")
    else:
        related = self._resolve_relation_descriptor(relationship)
        related.query_where_exists(self, callback, method="where_exists")
    return self


def _qb_or_where_has(self, relationship, callback) -> Self:
    if not self._model:
        raise AttributeError(
            "You must specify a model in order to use 'has' relationship methods"
        )

    if "." in relationship:
        last_builder = self._model.builder
        splits = relationship.split(".")
        split_count = len(splits)
        for index, split_relationship in enumerate(splits):
            related = last_builder.get_relation(split_relationship)
            if index + 1 == split_count:
                last_builder = related.query_where_exists(
                    last_builder,
                    callback,
                    method="where_exists",
                )
                continue

            last_builder = related.query_has(last_builder, method="or_where_exists")
    else:
        related = self._resolve_relation_descriptor(relationship)
        related.query_where_exists(self, callback, method="or_where_exists")
    return self


def _qb_where_doesnt_have(self, relationship, callback) -> Self:
    if not self._model:
        raise AttributeError(
            "You must specify a model in order to use the 'doesnt_have' relationship methods"
        )

    if "." in relationship:
        last_builder = self._model.builder
        split_count = len(relationship.split("."))
        for index, split_relationship in enumerate(relationship.split(".")):
            related = last_builder.get_relation(split_relationship)
            if index + 1 == split_count:
                last_builder = last_builder.get_relation(
                    split_relationship
                ).query_where_exists(
                    last_builder,
                    callback,
                    method="where_not_exists",
                )
                continue

            last_builder = related.query_has(last_builder, method="where_not_exists")
    else:
        related = self._resolve_relation_descriptor(relationship)
        related.query_where_exists(self, callback, method="where_not_exists")
    return self


def _qb_or_where_doesnt_have(self, relationship, callback) -> Self:
    if not self._model:
        raise AttributeError(
            "You must specify a model in order to use the 'doesnt_have' relationship methods"
        )

    if "." in relationship:
        last_builder = self._model.builder
        split_count = len(relationship.split("."))
        for index, split_relationship in enumerate(relationship.split(".")):
            related = last_builder.get_relation(split_relationship)
            if index + 1 == split_count:
                last_builder = last_builder.get_relation(
                    split_relationship
                ).query_where_exists(
                    last_builder,
                    callback,
                    method="or_where_not_exists",
                )
                continue

            last_builder = related.query_has(
                last_builder,
                method="or_where_not_exists",
            )
    else:
        related = self._resolve_relation_descriptor(relationship)
        related.query_where_exists(self, callback, method="or_where_not_exists")
    return self


def _qb_with_count(self, *relationships, callback=None):
    """
    Add ``{relationship}_count`` to the selected columns for each
    relation. Laravel parity: accepts a single string, multiple
    positional strings, a list/tuple, or a dict of
    ``{relation: callback}`` for constrained counts.

    Examples::

        Post.with_count("comments")
        Post.with_count("comments", "likes")
        Post.with_count(["comments", "likes"])
        Post.with_count({"comments": lambda q: q.where("approved", True)})
    """
    if not relationships:
        return self

    # Flatten heterogeneous inputs to a list of ``(name, callback)``
    # pairs so callers can mix-and-match shapes.
    pairs = []

    def _push(spec, inherited_cb):
        if spec is None:
            return
        if isinstance(spec, str):
            pairs.append((spec, inherited_cb))
        elif isinstance(spec, (list, tuple, set)):
            for item in spec:
                _push(item, inherited_cb)
        elif isinstance(spec, dict):
            for name, cb in spec.items():
                if isinstance(name, str):
                    pairs.append((name, cb if callable(cb) else inherited_cb))

    for r in relationships:
        _push(r, callback)

    self.select(*self._model.get_selects())
    builder = self
    for name, cb in pairs:
        rel = self._resolve_relation_descriptor(name)
        builder = rel.get_with_count_query(builder, callback=cb, relation_name=name)
    return builder


def _qb_resolve_relation_descriptor(self, name):
    """Fetch the relationship *descriptor* (not an instance-level proxy).

    ``getattr(instance, rel_name)`` triggers HasMany/HasOne.__get__ on
    instance paths which lazy-loads; we need the raw descriptor for
    subquery building. Walk the MRO to find it.
    """

    owner = self._model if _inspect.isclass(self._model) else type(self._model)
    rel = owner.__dict__.get(name)
    if rel is None:
        for base in owner.__mro__:
            if name in base.__dict__:
                rel = base.__dict__[name]
                break
    if rel is None:
        raise AttributeError(f"Relation '{name}' is not defined on {owner.__name__}")
    return rel


def _qb_with_sum(self, relationship, column, callback=None):
    """Eager load a relationship's SUM aggregate.

    Adds {relationship}_{column}_sum attribute to each model.

    Example:
        Model.with_sum("items", "amount").get()
        # model.items_amount_sum = 150.00
    """
    self.select(*self._model.get_selects())
    return self._resolve_relation_descriptor(relationship).get_with_sum_query(
        self, column, callback=callback, relation_name=relationship
    )


def _qb_with_avg(self, relationship, column, callback=None):
    """Eager load a relationship's AVG aggregate.

    Adds {relationship}_{column}_avg attribute to each model.

    Example:
        Model.with_avg("items", "amount").get()
        # model.items_amount_avg = 75.50
    """
    self.select(*self._model.get_selects())
    return self._resolve_relation_descriptor(relationship).get_with_avg_query(
        self, column, callback=callback, relation_name=relationship
    )


def _qb_with_min(self, relationship, column, callback=None):
    """Eager load a relationship's MIN aggregate.

    Adds {relationship}_{column}_min attribute to each model.

    Example:
        Model.with_min("items", "amount").get()
        # model.items_amount_min = 10.00
    """
    self.select(*self._model.get_selects())
    return self._resolve_relation_descriptor(relationship).get_with_min_query(
        self, column, callback=callback, relation_name=relationship
    )


def _qb_with_max(self, relationship, column, callback=None):
    """Eager load a relationship's MAX aggregate.

    Adds {relationship}_{column}_max attribute to each model.

    Example:
        Model.with_max("items", "amount").get()
        # model.items_amount_max = 200.00
    """
    self.select(*self._model.get_selects())
    return self._resolve_relation_descriptor(relationship).get_with_max_query(
        self, column, callback=callback, relation_name=relationship
    )


def _qb_tap(self, callback) -> Self:
    """Execute callback with the builder and return the builder for chaining.

    Useful for debugging or side effects without breaking the chain.

    Example:
        Model.active().tap(lambda q: print(q.to_sql())).get()
    """
    callback(self)
    return self


def _qb_pipe(self, callback):
    """Pass the builder to a callback and return the callback's result.

    Unlike tap(), pipe() returns what the callback returns.

    Example:
        result = Model.active().pipe(lambda q: q.count() > 0)
    """
    return callback(self)


def _qb_where_not_in(self, column, wheres=None) -> Self:
    """
    Specifies where a column does not contain a list of a values.

    Arguments:
        column {string} -- The name of the column.

    Keyword Arguments:
        wheres {list} -- A list of values (default: {[]})

    Returns:
        self
    """

    wheres = wheres or []

    if isinstance(wheres, QueryBuilder):
        self._wheres += (
            (
                QueryExpression(
                    column,
                    "NOT IN",
                    SubSelectExpression(wheres),
                )
            ),
        )
    elif not wheres:
        # Empty exclusion list is almost always a caller bug — e.g.
        # ``Model.where_not_in('id', external_ids).update({...})`` where
        # ``external_ids`` came back empty. Silently dropping the
        # clause would turn that into "update everything". Emit an
        # explicit always-true predicate so the SQL still reflects
        # intent ("nothing to exclude") and remains well-formed.
        self._wheres += (
            (
                QueryExpression(
                    column="1 = 1",
                    equality="",
                    value=None,
                    value_type="RAW",
                    keyword="AND",
                    raw=True,
                )
            ),
        )
    else:
        # Same defensive cleanup as ``where_in`` — drop None values
        # so we never emit literal ``NOT IN ('None', …)`` (which
        # would match every row in the table, the opposite of what
        # ``NOT IN (NULL, …)`` would actually evaluate to). If every
        # value was None, treat it as "nothing to exclude".
        cleaned = [v for v in wheres if v is not None]
        if not cleaned:
            self._wheres += (
                (
                    QueryExpression(
                        column="1 = 1",
                        equality="",
                        value=None,
                        value_type="RAW",
                        keyword="AND",
                        raw=True,
                    )
                ),
            )
        else:
            self._wheres += ((QueryExpression(column, "NOT IN", cleaned)),)
    return self


def _qb_join(
    self,
    table: str,
    column1=None,
    equality=None,
    column2=None,
    clause="inner",
) -> Self:
    """
    Specifies a join expression.

    Arguments:
        table {string} -- The name of the table or an instance of JoinClause.
        column1 {string} -- The name of the foreign table.
        equality {string} -- The equality to join on.
        column2 {string} -- The name of the local column.

    Keyword Arguments:
        clause {string} -- The action clause. (default: {"inner"})

    Returns:
        self
    """
    if inspect.isfunction(column1):
        self._joins += (column1(JoinClause(table, clause=clause)),)
    elif isinstance(table, str):
        self._joins += (JoinClause(table, clause=clause).on(column1, equality, column2),)
    else:
        self._joins += (table,)
    return self


def _qb_left_join(
    self,
    table,
    column1=None,
    equality=None,
    column2=None,
):
    """
    A helper method to add a left join expression.

    Arguments:
        table {string} -- The name of the table to join on.
        column1 {string} -- The name of the foreign table.
        equality {string} -- The equality to join on.
        column2 {string} -- The name of the local column.

    Returns:
        self
    """
    return self.join(
        table=table,
        column1=column1,
        equality=equality,
        column2=column2,
        clause="left",
    )


def _qb_right_join(
    self,
    table,
    column1=None,
    equality=None,
    column2=None,
):
    """
    A helper method to add a right join expression.

    Arguments:
        table {string} -- The name of the table to join on.
        column1 {string} -- The name of the foreign table.
        equality {string} -- The equality to join on.
        column2 {string} -- The name of the local column.

    Returns:
        self
    """
    return self.join(
        table=table,
        column1=column1,
        equality=equality,
        column2=column2,
        clause="right",
    )


def _qb_joins(self, *relationships, clause="inner") -> Self:
    for relationship in relationships:
        getattr(self._model, relationship).joins(self, clause=clause)

    return self


def _qb_join_on(self, relationship, callback=None, clause="inner") -> Self:
    relation = getattr(self._model, relationship)
    relation.joins(self, clause=clause)

    if callback:
        new_from_builder = self.new_from_builder()
        new_from_builder.table(relation.get_builder().get_table_name())
        self.where_from_builder(callback(new_from_builder))

    return self


def _qb_where_column(self, column1, column2) -> Self:
    """
    Specifies where two columns equal eachother.

    Arguments:
        column1 {string} -- The name of the column.
        column2 {string} -- The name of the column.

    Returns:
        self
    """
    self._wheres += ((QueryExpression(column1, "=", column2, "column")),)
    return self
