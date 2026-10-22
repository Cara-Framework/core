"""Compile a validated filter tree to one SQL predicate.

``compile_tree`` renders a :class:`~cara.filtering.Tree.FilterTree`
into a self-contained ``(fragment, params)`` pair — the same contract
as ``Filter.where_sql`` — so the SAME compiled predicate drops into a
query-builder chain (``query.where_raw(fragment, params)``) or into a
hand-written aliased SQL string. The caller ALWAYS ands the fragment
inside its base scope (tenant, grants, soft-deletes); a user tree can
only ever narrow.

Honesty contract: every operator here IS the SQL it claims. A group
toggle is a real ``OR``; ``in`` is a real ``= ANY``; nothing is
approximated. Negative operators on NULLABLE fields include NULL rows
(``is not X`` honestly reads "everything that isn't X", and a NULL
isn't X); on non-nullable fields they compile to the plain form.

Entity values are compiled from the ``resolutions`` mapping the app
built by resolving + AUTHORIZING every public id in the tree (see
``FilterTree.entity_values``). A missing resolution is a programming
error and raises — it must never degrade into an unfiltered query.

Date operators cut calendar days in the ``zone`` the caller passes — the
calendar the product reports in. A schema with a date field refuses to
compile without one (even for an empty tree, so a call site that forgets
it fails on its first request rather than the day someone adds a date
filter); there is no UTC default to fall back to silently.
"""

from __future__ import annotations

import datetime as _datetime
from collections.abc import Mapping
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from cara.exceptions import FilterTreeCompileError
from cara.support import like_contains

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


def compile_tree(
    tree: FilterTree,
    *,
    zone: ZoneInfo | None = None,
    resolutions: Mapping[str, Mapping[str, Any]] | None = None,
    ctx: Mapping[str, str] | None = None,
) -> tuple[str, list[Any]] | None:
    """Render ``tree`` to ``(fragment, params)`` — ``None`` when empty.

    ``zone`` is the calendar the date operators cut days in; it is
    required whenever the schema has a date field. ``resolutions`` maps
    ``field_id -> {public_id -> bound value}`` for entity fields. ``ctx``
    resolves ``{alias}`` placeholders in field columns so one schema
    renders against any table aliasing.
    """
    if tree.schema.has_dates and zone is None:
        raise FilterTreeCompileError(
            f"Schema {tree.schema.name!r} has date fields: compile it with the"
            " zone its calendar days are cut in."
        )
    if tree.is_empty:
        return None
    root_joiner = " OR " if tree.connective == "any" else " AND "
    fragments: list[str] = []
    params: list[Any] = []
    for node in tree.nodes:
        if isinstance(node, TreeGroup):
            joiner = " OR " if node.connective == "any" else " AND "
            parts: list[str] = []
            for condition in node.conditions:
                sql, values = _compile_condition(tree, condition, zone, resolutions, ctx)
                parts.append(sql)
                params.extend(values)
            fragments.append("(" + joiner.join(parts) + ")")
        else:
            sql, values = _compile_condition(tree, node, zone, resolutions, ctx)
            fragments.append(sql)
            params.extend(values)
    return "(" + root_joiner.join(fragments) + ")", params


def _compile_condition(
    tree: FilterTree,
    condition: TreeCondition,
    zone: ZoneInfo | None,
    resolutions: Mapping[str, Mapping[str, Any]] | None,
    ctx: Mapping[str, str] | None,
) -> tuple[str, list[Any]]:
    field = tree.schema.field(condition.field)
    if field is None:  # pragma: no cover - parse() already guarantees this
        raise FilterTreeCompileError(f"Unknown field {condition.field!r}.")
    values = _bound_values(field, condition, resolutions)
    if field.sql is not None:
        fragment, params = field.sql(condition.operator, values, dict(ctx or {}))
        return f"({fragment})", list(params)
    column = _render_column(field, ctx)
    return _render_operator(field, column, condition.operator, values, zone)


def _bound_values(
    field: TreeField,
    condition: TreeCondition,
    resolutions: Mapping[str, Mapping[str, Any]] | None,
) -> tuple[Any, ...]:
    """Coerce canonical string values into SQL bind values."""
    if field.kind == KIND_ENTITY and condition.operator in (OP_IN, OP_NOT_IN):
        table = (resolutions or {}).get(field.id) or {}
        missing = [value for value in condition.values if value not in table]
        if missing:
            raise FilterTreeCompileError(
                f"Entity field {field.id!r} has unresolved ids: {missing[:3]!r}."
                " Resolve + authorize every id before compiling."
            )
        return tuple(table[value] for value in condition.values)
    if field.kind == KIND_BOOLEAN:
        return tuple(value == "true" for value in condition.values)
    if field.kind == KIND_NUMBER:
        return tuple(
            int(value) if field.integer else Decimal(value) for value in condition.values
        )
    return condition.values


def _render_column(field: TreeField, ctx: Mapping[str, str] | None) -> str:
    column = field.column or ""
    if "{" not in column:
        return column
    try:
        return column.format_map(dict(ctx or {}))
    except KeyError as exc:
        raise FilterTreeCompileError(
            f"Field {field.id!r} column needs ctx alias {exc.args[0]!r}."
        ) from None


def _day_start(value: Any, zone: ZoneInfo) -> _datetime.datetime:
    """The UTC instant ``value``'s day opens in ``zone``. A local midnight
    the zone skips (a DST jump at 00:00) resolves to the first instant
    that day really has."""
    day = _datetime.date.fromisoformat(str(value))
    return _datetime.datetime.combine(day, _datetime.time.min, tzinfo=zone).astimezone(
        _UTC
    )


def _next_day_start(value: Any, zone: ZoneInfo) -> _datetime.datetime:
    # The NEXT day's own start, never ``+ 24h``: a DST day is 23 or 25 hours.
    day = _datetime.date.fromisoformat(str(value)) + _datetime.timedelta(days=1)
    return _day_start(day.isoformat(), zone)


def _render_operator(
    field: TreeField,
    column: str,
    operator: str,
    values: tuple[Any, ...],
    zone: ZoneInfo | None,
) -> tuple[str, list[Any]]:
    # ── value-less ──────────────────────────────────────────────────
    if operator == OP_EMPTY:
        return f"{column} IS NULL", []
    if operator == OP_NOT_EMPTY:
        return f"{column} IS NOT NULL", []

    # ── set membership ──────────────────────────────────────────────
    if operator == OP_IN:
        return f"{column} = ANY(%s)", [list(values)]
    if operator == OP_NOT_IN:
        if field.nullable:
            return f"({column} IS NULL OR NOT ({column} = ANY(%s)))", [list(values)]
        return f"NOT ({column} = ANY(%s))", [list(values)]

    # ── equality ────────────────────────────────────────────────────
    if operator == OP_IS:
        return f"{column} = %s", [values[0]]
    if operator == OP_IS_NOT:
        if field.nullable:
            return f"({column} IS NULL OR {column} != %s)", [values[0]]
        return f"{column} != %s", [values[0]]

    # ── text containment ────────────────────────────────────────────
    if operator == OP_CONTAINS:
        return f"{column} ILIKE %s ESCAPE '\\'", [like_contains(str(values[0]))]
    if operator == OP_NOT_CONTAINS:
        pattern = like_contains(str(values[0]))
        if field.nullable:
            return f"({column} IS NULL OR {column} NOT ILIKE %s ESCAPE '\\')", [pattern]
        return f"{column} NOT ILIKE %s ESCAPE '\\'", [pattern]

    # ── numeric comparisons ─────────────────────────────────────────
    if operator == OP_GT:
        return f"{column} > %s", [values[0]]
    if operator == OP_GTE:
        return f"{column} >= %s", [values[0]]
    if operator == OP_LT:
        return f"{column} < %s", [values[0]]
    if operator == OP_LTE:
        return f"{column} <= %s", [values[0]]
    if operator == OP_BETWEEN and field.kind == KIND_NUMBER:
        return f"{column} BETWEEN %s AND %s", [values[0], values[1]]

    # ── calendar-day comparisons over TIMESTAMPTZ ───────────────────
    # Whole days of ``zone``'s calendar, bound as the UTC instants they
    # open at so the bare column keeps its index: "before 2026-08-01"
    # excludes that day, "after 2026-08-01" starts the NEXT day,
    # "between" is inclusive of both named days ([start, end-exclusive)
    # on day boundaries). A day is 23 or 25 hours across a DST change;
    # each boundary is that day's own local midnight, never "+ 24h".
    if field.kind == KIND_DATE:
        if zone is None:  # pragma: no cover - compile_tree refuses this first
            raise FilterTreeCompileError(f"Date field {field.id!r} needs a zone.")
        if operator == OP_BEFORE:
            return f"{column} < %s", [_day_start(values[0], zone)]
        if operator == OP_AFTER:
            return f"{column} >= %s", [_next_day_start(values[0], zone)]
        if operator == OP_BETWEEN:
            return (
                f"({column} >= %s AND {column} < %s)",
                [_day_start(values[0], zone), _next_day_start(values[1], zone)],
            )
        if operator == OP_LAST_DAYS:
            # Calendar-aligned like every period selector: today plus the
            # N - 1 days before it, opening at that day's local midnight.
            # NOW() stays SQL-side so the window is evaluated per
            # execution; the canonical tree (and so the cursor scope) pins
            # the N, not the wall-clock moment. The subtraction runs on the
            # zone's wall clock (a naive timestamp), so a DST change inside
            # the window cannot shift the opening midnight by an hour.
            return (
                f"{column} >= ((date_trunc('day', NOW() AT TIME ZONE %s)"
                f" - (%s * INTERVAL '1 day')) AT TIME ZONE %s)",
                [zone.key, int(str(values[0])) - 1, zone.key],
            )

    raise FilterTreeCompileError(  # pragma: no cover - parse() blocks this
        f"Operator {operator!r} is not compilable for field {field.id!r}."
    )


__all__ = ["compile_tree"]
