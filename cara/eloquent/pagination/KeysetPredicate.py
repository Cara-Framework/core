"""Composite keyset (seek) predicates — the WHERE half of cursor pagination.

``cara.http.Cursor`` authenticates a cursor: it mints and verifies the
``(sort_value, id)`` pair a page ends on.  This module turns that verified
pair back into the comparison that finds the NEXT page.  The two halves are
one mechanism; splitting them left every application to re-derive the same
four-case operator table and the same tie-breaking algebra by hand.

A keyset predicate is subtle in exactly four places, and all four are
answered here rather than at each call site:

* **Composite sort key.** Sorting by a non-unique column alone cannot
  paginate: rows sharing a sort value have no defined order, so a page
  boundary that falls inside a tie drops or repeats rows.  Every predicate
  built here therefore compares the sort value AND a unique tie-breaker.
* **The tie-breaker must be unique** and must be the SAME column, in the
  same direction, as the query's second ``ORDER BY`` term.  A predicate
  that disagrees with its ``ORDER BY`` silently skips rows — it does not
  fail.
* **Direction.** ``direction`` is the direction the query sorts in;
  ``reverse`` walks that same order backwards (a previous-page cursor).
  The comparison operator falls out of both together.
* **Boundary.** Keyset paging is normally exclusive — the cursor row was
  already shown.  ``inclusive`` is for resuming a stream AT a known
  position (a replay, a "jump to this row" deep link) rather than after it.

SQL SAFETY — ``sort_sql`` and ``id_sql`` are interpolated into the emitted
fragment verbatim, because they are expressions, not values: a sort key is
often ``COALESCE(...)`` or a ``CASE``, which no bind parameter can carry.
They must therefore come from an application-owned allow-list of sortable
columns, never from request input.  Only the cursor's VALUES are bound, as
``%s`` placeholders in the order returned.

NULLs — neither form is meaningful when the sort expression can be NULL:
row comparison yields NULL and the expanded form's ``=`` never matches, so
the page silently ends early.  Keyset only over NOT NULL expressions (or
wrap them in ``COALESCE`` with a sentinel that sorts the same way).
"""

from __future__ import annotations

from typing import Any, Literal

SortDirection = Literal["asc", "desc"]
KeysetForm = Literal["row", "expanded"]

# The comparison a forward seek uses, by (sort direction, walking backwards).
# Reversing the walk mirrors the operator; it does NOT change the query's
# ORDER BY, which the caller must reverse itself.
_OPERATORS: dict[tuple[str, bool], str] = {
    ("asc", False): ">",
    ("asc", True): "<",
    ("desc", False): "<",
    ("desc", True): ">",
}


def keyset_operator(
    direction: SortDirection,
    *,
    reverse: bool = False,
    inclusive: bool = False,
) -> str:
    """The comparison operator a keyset seek uses.

    ``direction`` is the query's sort direction, ``reverse`` walks it
    backwards, and ``inclusive`` keeps the boundary row.  Exposed on its own
    because a caller composing the predicate through the query builder's
    fluent API needs the same operator this module's SQL forms embed —
    there is one right answer and it should not be re-derived.
    """

    strict = _OPERATORS[(_direction(direction), bool(reverse))]
    return f"{strict}=" if inclusive else strict


def keyset_predicate(
    sort_value: Any,
    primary_key: Any,
    *,
    sort_sql: str,
    id_sql: str = "id",
    direction: SortDirection,
    reverse: bool = False,
    inclusive: bool = False,
    form: KeysetForm = "row",
) -> tuple[str, list[Any]]:
    """Build a composite keyset predicate plus its bindings.

    Args:
        sort_value: The sort value the previous page ended on (the cursor's
            ``v``).
        primary_key: That row's unique tie-breaker (the cursor's ``id``).
        sort_sql: The sort expression, exactly as the query's first
            ``ORDER BY`` term writes it (qualify it yourself:
            ``"orders.created_at"``).  From an allow-list — see the module
            docstring.
        id_sql: The unique tie-breaker expression, matching the query's
            second ``ORDER BY`` term.
        direction: The direction the query sorts in.
        reverse: Walk that order backwards (a previous-page seek).  The
            caller must also reverse its ``ORDER BY`` and re-reverse the
            returned rows, or the "previous page" arrives inside out.
        inclusive: Keep the cursor row itself instead of starting after it.
        form: ``"row"`` emits the row-value comparison
            ``(sort, id) > (%s, %s)`` — two bindings, and the shape
            PostgreSQL can drive straight off a composite index.
            ``"expanded"`` emits ``(sort > %s OR (sort = %s AND id > %s))``
            — three bindings, and the shape to use when ``sort_sql`` is a
            plain column on engines whose planner handles the OR form
            better, or when a row comparison is unsupported.

    Returns:
        ``(fragment, bindings)``.  The fragment is fully parenthesised, so
        it can be ``AND``-ed into an existing ``WHERE`` without further
        grouping.  Bindings are positional, in placeholder order.

    Raises:
        ValueError: on an unknown direction or form, or a blank
        ``sort_sql`` / ``id_sql``.
    """

    sort_expression = _expression(sort_sql, "sort_sql")
    id_expression = _expression(id_sql, "id_sql")
    strict = keyset_operator(direction, reverse=reverse)
    boundary = keyset_operator(direction, reverse=reverse, inclusive=inclusive)

    if form == "row":
        return (
            f"({sort_expression}, {id_expression}) {boundary} (%s, %s)",
            [sort_value, primary_key],
        )
    if form == "expanded":
        # The OUTER comparison stays strict even when inclusive: widening it
        # to ``>=`` would match every row sharing the cursor's sort value,
        # tie-breaker ignored — a whole tie replayed onto the next page.
        return (
            f"({sort_expression} {strict} %s OR "
            f"({sort_expression} = %s AND {id_expression} {boundary} %s))",
            [sort_value, sort_value, primary_key],
        )
    raise ValueError("form must be 'row' or 'expanded'")


def _direction(value: Any) -> str:
    # ``isinstance`` first: an unhashable argument would make the membership
    # test raise TypeError, and a caller passing junk deserves the same
    # ValueError as one passing "ascending".
    if not isinstance(value, str) or value not in {"asc", "desc"}:
        raise ValueError("direction must be 'asc' or 'desc'")
    return value


def _expression(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty SQL expression")
    return value
