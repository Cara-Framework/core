"""Raw-SQL fragment helpers — the small pieces every hand-written query rebuilt.

Repositories that drop to ``DB.select`` / ``DB.statement`` kept re-writing
the same five mechanics, each copy with its own edge cases:

* a row-value ``(a, b) IN ((%s, %s), ...)`` predicate — written inline at
  every call site, some deduplicating the pairs, some sorting them, none
  refusing an empty list (``IN ()`` is a syntax error) or a ragged row;
* a multi-row ``VALUES (...), (...)`` list for a bulk ``INSERT … ON
  CONFLICT`` or an ``UPDATE … FROM (VALUES …)``;
* the intra-batch dedupe an ``ON CONFLICT DO UPDATE`` needs — two rows with
  the same conflict key in ONE statement raise ``cardinality_violation``
  (SQLSTATE 21000, "command cannot affect row a second time") and abort
  the whole batch; only one side had learned that;
* encoding a ``jsonb`` bind value — with stdlib ``json.dumps`` a ``Decimal``
  raised ``TypeError`` (or was flattened to a float, which §7 forbids);
* decoding a ``jsonb`` column a raw select handed back as TEXT.

Generic — no domain vocabulary. Column names and SQL expressions passed in
are the CALLER's constants (never user input); every value travels as a
bound parameter.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
from typing import Any

from cara.support import json_dumps


def row_value(row: Any, key: str, default: Any = None) -> Any:
    """``row[key]`` for a mapping row, ``row.key`` for a model/record row.

    Raw selects return dicts and the ORM returns models; a reader that takes
    either asks through this. ``None`` row (a missed ``select_one``) and a
    missing key both answer ``default``.
    """

    if row is None:
        return default
    if isinstance(row, Mapping):
        return row.get(key, default)
    return getattr(row, key, default)


def in_list(values: Iterable[Any]) -> tuple[str, list[Any]]:
    """``%s, %s, ...`` + bindings for ``col IN (...)``, duplicates dropped.

    First-seen order is kept, so a caller that wants a stable statement
    (plan cache, deterministic test) sorts BEFORE calling. An empty input
    raises: ``IN ()`` is a syntax error and ``IN (NULL)`` / ``NOT IN
    (NULL)`` silently match nothing — the caller decides what "no values"
    means. Prefer ``col = ANY(%s)`` with one list binding where the column
    type allows it; this is for the cases it does not.
    """

    ordered = list(dict.fromkeys(values))
    if not ordered:
        raise ValueError("in_list() needs at least one value; guard the empty case.")
    return ", ".join(["%s"] * len(ordered)), ordered


def row_values_in(
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    negate: bool = False,
) -> tuple[str, list[Any]]:
    """Row-value ``(c1, c2) [NOT] IN ((%s, %s), ...)`` predicate + bindings.

    Matches every tuple in ONE scan instead of one query per tuple. Rows are
    deduplicated (first-seen order kept); an empty input or a row whose
    width differs from ``columns`` raises rather than emitting broken SQL or
    misaligned bindings. ``columns`` may be SQL expressions
    (``"data->>'key'"``) — they are the caller's constants.
    """

    width = len(columns)
    if width == 0:
        raise ValueError("row_values_in() needs at least one column.")
    ordered = list(dict.fromkeys(tuple(row) for row in rows))
    if not ordered:
        raise ValueError("row_values_in() needs at least one row; guard the empty case.")
    for row in ordered:
        if len(row) != width:
            raise ValueError(
                f"row_values_in(): row {row!r} has {len(row)} values for {width} columns."
            )
    tuple_sql = "(" + ", ".join(["%s"] * width) + ")"
    operator = "NOT IN" if negate else "IN"
    predicate = (
        f"({', '.join(columns)}) {operator} ({', '.join([tuple_sql] * len(ordered))})"
    )
    return predicate, [value for row in ordered for value in row]


def values_rows(
    rows: Iterable[Sequence[Any]],
    *,
    casts: Sequence[str | None] | None = None,
    tail: Sequence[str] = (),
) -> tuple[str, list[Any]]:
    """``(%s, %s::jsonb, NOW()), (...)`` + bindings for a multi-row VALUES list.

    ``casts`` names a Postgres cast per bound column (``None`` = none) —
    needed for ``jsonb`` and for any column a ``FROM (VALUES …)`` join would
    otherwise read as ``text``. ``tail`` appends constant SQL to every row
    (``NOW()`` for timestamps). Rows are NOT deduplicated here: for an
    ``ON CONFLICT DO UPDATE`` run :func:`dedupe_last` first.
    """

    materialized = [tuple(row) for row in rows]
    if not materialized:
        raise ValueError("values_rows() needs at least one row; guard the empty case.")
    width = len(materialized[0])
    if casts is not None and len(casts) != width:
        raise ValueError(f"values_rows(): {len(casts)} casts for {width} columns.")
    for row in materialized:
        if len(row) != width:
            raise ValueError(
                f"values_rows(): row {row!r} has {len(row)} values, expected {width}."
            )
    cells = [
        "%s" if casts is None or not casts[index] else f"%s::{casts[index]}"
        for index in range(width)
    ]
    cells.extend(tail)
    row_sql = "(" + ", ".join(cells) + ")"
    return ", ".join([row_sql] * len(materialized)), [
        value for row in materialized for value in row
    ]


def dedupe_last[T](rows: Iterable[T], key: Callable[[T], Hashable]) -> list[T]:
    """Collapse rows sharing a conflict key — the LAST one wins.

    One ``INSERT … ON CONFLICT DO UPDATE`` may not touch the same target row
    twice (SQLSTATE 21000 aborts the whole batch). Last-wins matches what
    sequential single-row upserts would have left behind; the surviving row
    keeps the position of the key's first appearance.
    """

    collapsed: dict[Hashable, T] = {}
    for row in rows:
        collapsed[key(row)] = row
    return list(collapsed.values())


def json_param(value: Any, *, empty_as_null: bool = False) -> str | None:
    """Encode a value for a ``jsonb`` bind parameter (``%s::jsonb``).

    ``None`` stays SQL ``NULL``. Encoding goes through cara's one wire rule
    (``json_dumps``): a ``Decimal`` keeps its digits instead of raising or
    becoming a float. ``empty_as_null`` maps ``{}`` / ``[]`` / ``""`` to
    ``NULL`` for columns where an empty document means "unknown".
    """

    if value is None:
        return None
    if empty_as_null and not value:
        return None
    return json_dumps(value)


def json_column(value: Any, default: Any = None) -> Any:
    """A ``jsonb`` column value as Python, whatever the driver handed back.

    psycopg2 decodes ``jsonb`` into dict/list, but a raw select through a
    cast, a ``->`` on a text path or a cache round-trip hands it back as
    ``str`` / ``bytes``. Text is parsed; malformed text, an empty string and
    ``NULL`` all answer ``default`` — a reader never crashes on a bad
    document. Already-decoded values pass through unchanged.
    """

    if value is None:
        return default
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, str):
        if not value.strip():
            return default
        try:
            return json.loads(value)
        except ValueError:
            return default
    return value


__all__ = [
    "dedupe_last",
    "in_list",
    "json_column",
    "json_param",
    "row_value",
    "row_values_in",
    "values_rows",
]
