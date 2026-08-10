"""Composite keyset predicates — the WHERE half of cursor pagination.

The emitted-SQL tests pin the exact fragment and binding order, because
both shapes are spliced verbatim into application queries and a silent
change to either would move every paginated endpoint's plan.

The executed tests are the ones that matter: a keyset predicate that
disagrees with its ``ORDER BY``, or that mishandles a tie, does not raise —
it drops or repeats rows. So the behavioural tests page a real table with a
deliberately tie-heavy sort key and assert the walk visits every row
exactly once, in both forms and both directions.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent.pagination.KeysetPredicate import keyset_operator, keyset_predicate

# ── the operator table ────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("direction", "reverse", "inclusive", "expected"),
    [
        ("asc", False, False, ">"),
        ("asc", False, True, ">="),
        ("asc", True, False, "<"),
        ("asc", True, True, "<="),
        ("desc", False, False, "<"),
        ("desc", False, True, "<="),
        ("desc", True, False, ">"),
        ("desc", True, True, ">="),
    ],
)
def test_the_operator_falls_out_of_direction_walk_and_boundary(
    direction, reverse, inclusive, expected
) -> None:
    assert keyset_operator(direction, reverse=reverse, inclusive=inclusive) == expected


# ── emitted SQL: row-value form ───────────────────────────────────────


def test_row_form_compares_the_whole_key_with_two_bindings() -> None:
    fragment, bindings = keyset_predicate(
        "2026-08-08T00:00:00+00:00",
        41,
        sort_sql="p.created_at",
        id_sql="p.id",
        direction="asc",
    )

    assert fragment == "(p.created_at, p.id) > (%s, %s)"
    assert bindings == ["2026-08-08T00:00:00+00:00", 41]


def test_row_form_flips_the_operator_for_a_descending_sort() -> None:
    fragment, _bindings = keyset_predicate(
        7, 41, sort_sql="p.rank", id_sql="p.id", direction="desc"
    )

    assert fragment == "(p.rank, p.id) < (%s, %s)"


def test_row_form_widens_the_whole_comparison_when_inclusive() -> None:
    fragment, _bindings = keyset_predicate(
        7, 41, sort_sql="rank", direction="asc", inclusive=True
    )

    assert fragment == "(rank, id) >= (%s, %s)"


# ── emitted SQL: expanded form ────────────────────────────────────────


def test_expanded_form_repeats_the_sort_value_for_the_tie_branch() -> None:
    fragment, bindings = keyset_predicate(
        7,
        41,
        sort_sql="notification.created_at",
        id_sql="notification.id",
        direction="desc",
        form="expanded",
    )

    assert fragment == (
        "(notification.created_at < %s OR "
        "(notification.created_at = %s AND notification.id < %s))"
    )
    assert bindings == [7, 7, 41]


def test_expanded_form_keeps_its_outer_comparison_strict_when_inclusive() -> None:
    """Widening the OUTER branch would match the cursor row's ENTIRE tie
    group regardless of the tie-breaker — a whole tie replayed onto the
    next page. Only the tie branch may include the boundary."""

    fragment, _bindings = keyset_predicate(
        7, 41, sort_sql="rank", direction="asc", inclusive=True, form="expanded"
    )

    assert fragment == "(rank > %s OR (rank = %s AND id >= %s))"


def test_the_id_expression_defaults_to_the_bare_primary_key() -> None:
    fragment, _bindings = keyset_predicate(
        7, 41, sort_sql="rank", direction="asc", form="expanded"
    )

    assert fragment == "(rank > %s OR (rank = %s AND id > %s))"


def test_a_computed_sort_expression_is_interpolated_verbatim() -> None:
    """Sort keys are expressions, not values — a CASE or COALESCE cannot
    ride a bind parameter, which is why the allow-list contract exists."""

    sort_sql = "(CASE WHEN o.priority THEN 1 ELSE 0 END)"
    fragment, bindings = keyset_predicate(
        1, 41, sort_sql=sort_sql, id_sql="o.feed_id", direction="desc", form="expanded"
    )

    assert fragment == (f"({sort_sql} < %s OR ({sort_sql} = %s AND o.feed_id < %s))")
    assert bindings == [1, 1, 41]


def test_a_null_sort_value_still_binds_rather_than_being_inlined() -> None:
    _fragment, bindings = keyset_predicate(None, 41, sort_sql="rank", direction="asc")

    assert bindings == [None, 41]


# ── refusals ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("direction", ["ASC", "ascending", "", None, 1, ["asc"]])
def test_an_unknown_direction_is_refused(direction) -> None:
    with pytest.raises(ValueError, match="direction"):
        keyset_predicate(7, 41, sort_sql="rank", direction=direction)


def test_an_unknown_form_is_refused() -> None:
    with pytest.raises(ValueError, match="form"):
        keyset_predicate(7, 41, sort_sql="rank", direction="asc", form="tuple")


@pytest.mark.parametrize("blank", ["", "   ", None, 7])
def test_a_blank_sort_expression_is_refused(blank) -> None:
    with pytest.raises(ValueError, match="sort_sql"):
        keyset_predicate(7, 41, sort_sql=blank, direction="asc")


@pytest.mark.parametrize("blank", ["", "   ", None, 7])
def test_a_blank_tie_breaker_expression_is_refused(blank) -> None:
    with pytest.raises(ValueError, match="id_sql"):
        keyset_predicate(7, 41, sort_sql="rank", id_sql=blank, direction="asc")


# ── executed behaviour ────────────────────────────────────────────────

# Deliberately tie-heavy: five rows over three distinct sort values, with
# the largest tie group straddling any sane page boundary.
_ROWS = [
    (1, "b"),
    (2, "b"),
    (3, "a"),
    (4, "c"),
    (5, "b"),
]


def _connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, grade TEXT NOT NULL)")
    connection.executemany("INSERT INTO item (id, grade) VALUES (?, ?)", _ROWS)
    return connection


def _select(connection, fragment, bindings, *, direction, reverse=False):
    """Run one keyset page. ``%s`` is cara's query paramstyle; sqlite3 wants
    ``?``, and translating here keeps the emitted-SQL assertions above
    honest about what applications actually receive."""

    walked = "desc" if (direction == "asc") == reverse else "asc"
    order = "DESC" if walked == "desc" else "ASC"
    sql = (
        f"SELECT id, grade FROM item WHERE {fragment.replace('%s', '?')} "
        f"ORDER BY grade {order}, id {order}"
    )
    return connection.execute(sql, bindings).fetchall()


def _ordered(direction: str) -> list[tuple[int, str]]:
    rows = sorted(_ROWS, key=lambda row: (row[1], row[0]))
    if direction == "desc":
        rows.reverse()
    return [(row[0], row[1]) for row in rows]


@pytest.mark.parametrize("form", ["row", "expanded"])
@pytest.mark.parametrize("direction", ["asc", "desc"])
def test_paging_the_whole_table_visits_every_row_exactly_once(form, direction) -> None:
    """The defect a keyset predicate produces is silent: a boundary that
    lands inside a tie drops rows or shows them twice. Walking the entire
    table two rows at a time is what actually proves it does neither."""

    connection = _connection()
    expected = _ordered(direction)
    page_size = 2
    seen: list[tuple[int, str]] = []
    cursor: tuple[str, int] | None = None

    while True:
        if cursor is None:
            fragment, bindings = "1=1", []
        else:
            fragment, bindings = keyset_predicate(
                cursor[0],
                cursor[1],
                sort_sql="grade",
                id_sql="id",
                direction=direction,
                form=form,
            )
        page = _select(connection, fragment, bindings, direction=direction)[:page_size]
        if not page:
            break
        seen.extend(page)
        cursor = (page[-1][1], page[-1][0])

    assert seen == expected


@pytest.mark.parametrize("direction", ["asc", "desc"])
def test_both_forms_select_the_same_rows_from_the_same_cursor(direction) -> None:
    """Row-value and expanded are two spellings of one predicate. Products
    pick between them on planner grounds, so they must never disagree."""

    connection = _connection()
    for boundary in _ROWS:
        row_sql, row_bindings = keyset_predicate(
            boundary[1],
            boundary[0],
            sort_sql="grade",
            id_sql="id",
            direction=direction,
            form="row",
        )
        expanded_sql, expanded_bindings = keyset_predicate(
            boundary[1],
            boundary[0],
            sort_sql="grade",
            id_sql="id",
            direction=direction,
            form="expanded",
        )

        assert _select(connection, row_sql, row_bindings, direction=direction) == _select(
            connection, expanded_sql, expanded_bindings, direction=direction
        )


@pytest.mark.parametrize("form", ["row", "expanded"])
def test_the_boundary_row_is_excluded_by_default_and_kept_when_inclusive(form) -> None:
    connection = _connection()

    exclusive, exclusive_bindings = keyset_predicate(
        "b", 2, sort_sql="grade", id_sql="id", direction="asc", form=form
    )
    inclusive, inclusive_bindings = keyset_predicate(
        "b",
        2,
        sort_sql="grade",
        id_sql="id",
        direction="asc",
        inclusive=True,
        form=form,
    )

    assert _select(connection, exclusive, exclusive_bindings, direction="asc") == [
        (5, "b"),
        (4, "c"),
    ]
    assert _select(connection, inclusive, inclusive_bindings, direction="asc") == [
        (2, "b"),
        (5, "b"),
        (4, "c"),
    ]


@pytest.mark.parametrize("form", ["row", "expanded"])
def test_reversing_the_walk_returns_the_rows_before_the_cursor(form) -> None:
    """A previous-page seek keeps the query's sort direction and flips the
    comparison; the caller reverses ORDER BY and re-reverses the rows."""

    connection = _connection()
    fragment, bindings = keyset_predicate(
        "b", 5, sort_sql="grade", id_sql="id", direction="asc", reverse=True, form=form
    )

    page = _select(connection, fragment, bindings, direction="asc", reverse=True)

    # Walked backwards, then re-reversed into the query's own order.
    assert list(reversed(page)) == [(3, "a"), (1, "b"), (2, "b")]


def test_a_tie_group_is_never_split_across_a_reversed_boundary() -> None:
    """The tie branch is where the expanded form's asymmetry lives: its
    outer comparison stays strict, so the tie-breaker alone decides inside
    a tie — in both walking directions."""

    connection = _connection()
    fragment, bindings = keyset_predicate(
        "b",
        2,
        sort_sql="grade",
        id_sql="id",
        direction="asc",
        reverse=True,
        form="expanded",
    )

    page = _select(connection, fragment, bindings, direction="asc", reverse=True)

    assert list(reversed(page)) == [(3, "a"), (1, "b")]
