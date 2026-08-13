"""Evaluate a validated filter tree against in-memory rows.

``evaluate_tree`` is :func:`~cara.filtering.TreeCompiler.compile_tree`'s
twin for feeds the database never sees — computed projections whose
verdicts exist only in Python (guardrail evaluations, intelligence
buckets). Same honesty contract, mirrored: a group toggle is a real OR,
``in`` is real set membership, and negative operators on a missing
(``None``) value are truthy — "everything that isn't X" honestly
includes the rows where X was never computed, exactly as the SQL
compiler includes NULLs on nullable columns.

Entity values evaluate through the SAME ``resolutions`` mapping the app
built by resolving + AUTHORIZING every public id in the tree. A missing
resolution raises — it must never degrade into an unfiltered feed.
"""

from __future__ import annotations

import datetime as _datetime
from collections.abc import Callable, Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from cara.exceptions import FilterTreeCompileError

from .FilterTree import FilterTree
from .TreeCondition import TreeCondition
from .TreeField import (
    KIND_BOOLEAN,
    KIND_DATE,
    KIND_ENTITY,
    KIND_NUMBER,
    OP_AFTER,
    OP_BEFORE,
    OP_BETWEEN,
    OP_CONTAINS,
    OP_EMPTY,
    OP_GT,
    OP_GTE,
    OP_IN,
    OP_IS,
    OP_IS_NOT,
    OP_LAST_DAYS,
    OP_LT,
    OP_LTE,
    OP_NOT_CONTAINS,
    OP_NOT_EMPTY,
    OP_NOT_IN,
    TreeField,
)
from .TreeGroup import TreeGroup

_UTC = _datetime.UTC


def evaluate_tree(
    tree: FilterTree,
    values: Callable[[str], Any],
    *,
    resolutions: Mapping[str, Mapping[str, Any]] | None = None,
) -> bool:
    """Whether one row matches ``tree`` — ``True`` when the tree is empty.

    ``values`` maps a field id to the row's value for it (the caller owns
    the projection's shape); ``resolutions`` maps
    ``field_id -> {public_id -> bound value}`` for entity fields, exactly
    as the SQL compiler takes them.
    """
    if tree.is_empty:
        return True
    verdicts = (_evaluate_node(tree, node, values, resolutions) for node in tree.nodes)
    return any(verdicts) if tree.connective == "any" else all(verdicts)


def _evaluate_node(
    tree: FilterTree,
    node: TreeCondition | TreeGroup,
    values: Callable[[str], Any],
    resolutions: Mapping[str, Mapping[str, Any]] | None,
) -> bool:
    if isinstance(node, TreeGroup):
        verdicts = (
            _evaluate_condition(tree, condition, values, resolutions)
            for condition in node.conditions
        )
        return any(verdicts) if node.connective == "any" else all(verdicts)
    return _evaluate_condition(tree, node, values, resolutions)


def _evaluate_condition(
    tree: FilterTree,
    condition: TreeCondition,
    values: Callable[[str], Any],
    resolutions: Mapping[str, Mapping[str, Any]] | None,
) -> bool:
    field = tree.schema.field(condition.field)
    if field is None:  # pragma: no cover - parse() already guarantees this
        raise FilterTreeCompileError(f"Unknown field {condition.field!r}.")
    bound = _bound_values(field, condition, resolutions)
    return _apply_operator(field, condition.operator, bound, values(field.id))


def _bound_values(
    field: TreeField,
    condition: TreeCondition,
    resolutions: Mapping[str, Mapping[str, Any]] | None,
) -> tuple[Any, ...]:
    """Coerce canonical string values into comparison values — the same
    coercions the SQL compiler binds."""
    if field.kind == KIND_ENTITY and condition.operator in (OP_IN, OP_NOT_IN):
        table = (resolutions or {}).get(field.id) or {}
        missing = [value for value in condition.values if value not in table]
        if missing:
            raise FilterTreeCompileError(
                f"Entity field {field.id!r} has unresolved ids: {missing[:3]!r}."
                " Resolve + authorize every id before evaluating."
            )
        return tuple(table[value] for value in condition.values)
    if field.kind == KIND_BOOLEAN:
        return tuple(value == "true" for value in condition.values)
    if field.kind == KIND_NUMBER:
        return tuple(
            int(value) if field.integer else Decimal(value) for value in condition.values
        )
    return condition.values


def _day_start(value: str) -> _datetime.datetime:
    return _datetime.datetime.combine(
        _datetime.date.fromisoformat(value), _datetime.time.min, tzinfo=_UTC
    )


def _as_moment(value: Any) -> _datetime.datetime | None:
    if value is None:
        return None
    if isinstance(value, _datetime.datetime):
        return value if value.tzinfo else value.replace(tzinfo=_UTC)
    moment = _datetime.datetime.fromisoformat(str(value))
    return moment if moment.tzinfo else moment.replace(tzinfo=_UTC)


def _apply_operator(
    field: TreeField, operator: str, bound: tuple[Any, ...], actual: Any
) -> bool:
    # ── value-less ──────────────────────────────────────────────────
    if operator == OP_EMPTY:
        return actual is None
    if operator == OP_NOT_EMPTY:
        return actual is not None

    # ── calendar-day comparisons (UTC whole days) ───────────────────
    if field.kind == KIND_DATE:
        moment = _as_moment(actual)
        if operator == OP_BEFORE:
            return moment is not None and moment < _day_start(str(bound[0]))
        if operator == OP_AFTER:
            return moment is not None and moment >= _day_start(
                str(bound[0])
            ) + _datetime.timedelta(days=1)
        if operator == OP_BETWEEN:
            return moment is not None and _day_start(
                str(bound[0])
            ) <= moment < _day_start(str(bound[1])) + _datetime.timedelta(days=1)
        if operator == OP_LAST_DAYS:
            # Wall-clock evaluated per call, mirroring the compiler's
            # SQL-side NOW(): the canonical tree pins the N, not the moment.
            now = _datetime.datetime.now(_UTC)
            return moment is not None and moment >= now - _datetime.timedelta(
                days=int(str(bound[0]))
            )

    if field.kind == KIND_NUMBER and actual is not None:
        try:
            actual = Decimal(str(actual))
        except InvalidOperation, TypeError, ValueError:
            return False
        if not actual.is_finite():
            return False

    # ── set membership ──────────────────────────────────────────────
    if operator == OP_IN:
        return actual is not None and actual in bound
    if operator == OP_NOT_IN:
        return actual is None or actual not in bound

    # ── equality ────────────────────────────────────────────────────
    if operator == OP_IS:
        return actual is not None and actual == bound[0]
    if operator == OP_IS_NOT:
        return actual is None or actual != bound[0]

    # ── text containment ────────────────────────────────────────────
    if operator == OP_CONTAINS:
        return actual is not None and str(bound[0]).lower() in str(actual).lower()
    if operator == OP_NOT_CONTAINS:
        return actual is None or str(bound[0]).lower() not in str(actual).lower()

    # ── numeric comparisons ─────────────────────────────────────────
    if operator == OP_GT:
        return actual is not None and actual > bound[0]
    if operator == OP_GTE:
        return actual is not None and actual >= bound[0]
    if operator == OP_LT:
        return actual is not None and actual < bound[0]
    if operator == OP_LTE:
        return actual is not None and actual <= bound[0]
    if operator == OP_BETWEEN and field.kind == KIND_NUMBER:
        return actual is not None and bound[0] <= actual <= bound[1]

    raise FilterTreeCompileError(  # pragma: no cover - parse() blocks this
        f"Operator {operator!r} is not evaluable for field {field.id!r}."
    )


__all__ = ["evaluate_tree"]
