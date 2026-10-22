"""``compile_tree`` — the tree-to-SQL honesty contract.

Every operator compiles to exactly the SQL it claims: real ``= ANY``,
real ``OR`` groups, NULL-inclusive negatives on nullable fields only,
escaped ILIKE containment, day-boundary date math in the caller's
zone (DST days included), and entity values bound from the app's
resolved + authorized mapping (an unresolved id raises instead of
degrading the filter).
"""

from __future__ import annotations

import datetime
import json
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from cara.exceptions import FilterSchemaError, FilterTreeCompileError
from cara.filtering import (
    FilterTree,
    TreeField,
    TreeSchema,
    compile_tree,
)

_CHN = "CHN" + "0123456789ABCDEFGHJKMNPQRS"
_UTC = datetime.UTC
_LA = ZoneInfo("America/Los_Angeles")


def _schema() -> TreeSchema:
    return TreeSchema(
        "tests.compile",
        (
            TreeField(
                "status",
                "select",
                column="{listing}.status",
                options=(("active", "Live"), ("draft", "Draft"), ("error", "Error")),
            ),
            TreeField("channel", "entity", column="channel_id", prefix="CHN"),
            TreeField(
                "linked",
                "boolean",
                sql=lambda op, values, ctx: (
                    "product_id IS NOT NULL" if values[0] else "product_id IS NULL",
                    [],
                ),
            ),
            TreeField("title", "text", column="title", nullable=True),
            TreeField("price", "number", column="price", nullable=True),
            TreeField("updated", "date", column="updated_at"),
        ),
    )


def _compile(nodes, **kwargs):
    tree = FilterTree.parse(json.dumps(nodes), _schema())
    kwargs.setdefault("zone", _LA)
    return compile_tree(tree, **kwargs)


def _at(*parts: int) -> datetime.datetime:
    return datetime.datetime(*parts, tzinfo=_UTC)


def test_empty_tree_compiles_to_none():
    assert compile_tree(FilterTree.empty(_schema()), zone=_LA) is None


def test_a_date_schema_refuses_to_compile_without_a_zone_even_when_empty():
    with pytest.raises(FilterTreeCompileError, match="date fields"):
        compile_tree(FilterTree.empty(_schema()))
    with pytest.raises(FilterTreeCompileError, match="date fields"):
        compile_tree(
            FilterTree.parse(
                json.dumps([{"f": "price", "o": "gt", "v": ["1"]}]), _schema()
            )
        )


def test_a_date_field_never_renders_through_a_sql_hook():
    # The hook is never handed the zone, so it could only cut its days in
    # another calendar than the one the caller compiles with.
    with pytest.raises(FilterSchemaError, match="renders on a column"):
        TreeField("placed", "date", sql=lambda op, values, ctx: ("TRUE", []))


def test_a_schema_without_dates_compiles_without_a_zone():
    schema = TreeSchema(
        "tests.compile.nodates", (TreeField("price", "number", column="price"),)
    )
    assert not schema.has_dates
    assert _schema().has_dates
    tree = FilterTree.parse(json.dumps([{"f": "price", "o": "gt", "v": ["1"]}]), schema)
    assert compile_tree(tree) == ("(price > %s)", [Decimal("1")])


def test_root_nodes_and_join_and_groups_or_join():
    sql, params = _compile(
        [
            {"f": "status", "o": "in", "v": ["active", "draft"]},
            {
                "any": [
                    {"f": "status", "o": "in", "v": ["error"]},
                    {"f": "linked", "o": "is", "v": ["false"]},
                ]
            },
        ],
        ctx={"listing": "l"},
    )
    assert sql == (
        "(l.status = ANY(%s) AND (l.status = ANY(%s) OR (product_id IS NULL)))"
    )
    assert params == [["active", "draft"], ["error"]]


def test_ctx_aliases_resolve_and_missing_alias_raises():
    sql, _ = _compile(
        [{"f": "status", "o": "in", "v": ["active"]}], ctx={"listing": "listing"}
    )
    assert sql.startswith("(listing.status")
    with pytest.raises(FilterTreeCompileError, match="ctx alias"):
        _compile([{"f": "status", "o": "in", "v": ["active"]}])


def test_entity_values_bind_through_resolutions_and_missing_ids_raise():
    sql, params = _compile(
        [{"f": "channel", "o": "in", "v": [_CHN]}],
        resolutions={"channel": {_CHN: 42}},
    )
    assert sql == "(channel_id = ANY(%s))"
    assert params == [[42]]
    with pytest.raises(FilterTreeCompileError, match="unresolved"):
        _compile([{"f": "channel", "o": "in", "v": [_CHN]}])


def test_negatives_include_null_only_on_nullable_fields():
    sql, params = _compile(
        [{"f": "status", "o": "not_in", "v": ["draft"]}], ctx={"listing": "l"}
    )
    assert sql == "(NOT (l.status = ANY(%s)))"
    sql, params = _compile([{"f": "title", "o": "is_not", "v": ["x"]}])
    assert sql == "((title IS NULL OR title != %s))"
    assert params == ["x"]
    sql, params = _compile([{"f": "title", "o": "not_contains", "v": ["x"]}])
    assert sql == "((title IS NULL OR title NOT ILIKE %s ESCAPE '\\'))"
    assert params == ["%x%"]


def test_contains_escapes_like_metacharacters():
    sql, params = _compile([{"f": "title", "o": "contains", "v": ["50%_off"]}])
    assert sql == "(title ILIKE %s ESCAPE '\\')"
    assert params == ["%50\\%\\_off%"]


def test_empty_operators_compile_to_is_null():
    sql, _ = _compile([{"f": "price", "o": "empty", "v": []}])
    assert sql == "(price IS NULL)"
    sql, _ = _compile([{"f": "price", "o": "not_empty", "v": []}])
    assert sql == "(price IS NOT NULL)"


def test_number_comparisons_bind_typed_values():
    sql, params = _compile([{"f": "price", "o": "between", "v": ["100", "20"]}])
    assert sql == "(price BETWEEN %s AND %s)"
    assert params == [Decimal("20"), Decimal("100")]
    sql, params = _compile([{"f": "price", "o": "gt", "v": ["9.5"]}])
    assert params == [Decimal("9.5")]


def test_number_bindings_preserve_decimal_precision() -> None:
    _, params = _compile([{"f": "price", "o": "gt", "v": ["99999999999.999999"]}])

    assert params == [Decimal("99999999999.999999")]


def test_date_operators_bind_the_zones_day_starts_on_the_bare_column():
    # Pacific daylight time: a day opens at 07:00Z.
    sql, params = _compile([{"f": "updated", "o": "before", "v": ["2026-08-01"]}])
    assert sql == "(updated_at < %s)"
    assert params == [_at(2026, 8, 1, 7)]
    sql, params = _compile([{"f": "updated", "o": "after", "v": ["2026-08-01"]}])
    assert sql == "(updated_at >= %s)"
    assert params == [_at(2026, 8, 2, 7)]
    sql, params = _compile(
        [{"f": "updated", "o": "between", "v": ["2026-08-03", "2026-08-01"]}]
    )
    assert sql == "((updated_at >= %s AND updated_at < %s))"
    assert params == [_at(2026, 8, 1, 7), _at(2026, 8, 4, 7)]
    # Pacific standard time: a day opens at 08:00Z.
    _, params = _compile(
        [{"f": "updated", "o": "between", "v": ["2026-12-10", "2026-12-10"]}]
    )
    assert params == [_at(2026, 12, 10, 8), _at(2026, 12, 11, 8)]


def test_dst_days_are_23_and_25_hours_long():
    _, params = _compile(
        [{"f": "updated", "o": "between", "v": ["2026-03-08", "2026-03-08"]}]
    )
    assert params == [_at(2026, 3, 8, 8), _at(2026, 3, 9, 7)]
    assert params[1] - params[0] == datetime.timedelta(hours=23)
    _, params = _compile(
        [{"f": "updated", "o": "between", "v": ["2026-11-01", "2026-11-01"]}]
    )
    assert params == [_at(2026, 11, 1, 7), _at(2026, 11, 2, 8)]
    assert params[1] - params[0] == datetime.timedelta(hours=25)


def test_a_moment_after_utc_midnight_belongs_to_the_zones_previous_day():
    moment = _at(2026, 10, 21, 6, 30)  # 2026-10-20 23:30 in Los Angeles
    _, (start, stop) = _compile(
        [{"f": "updated", "o": "between", "v": ["2026-10-20", "2026-10-20"]}]
    )
    assert start <= moment < stop
    _, (after,) = _compile([{"f": "updated", "o": "after", "v": ["2026-10-20"]}])
    assert not moment >= after
    # The same tree cut in UTC reads the reverse.
    utc = ZoneInfo("UTC")
    _, (start, stop) = _compile(
        [{"f": "updated", "o": "between", "v": ["2026-10-20", "2026-10-20"]}], zone=utc
    )
    assert not start <= moment < stop
    _, (after,) = _compile(
        [{"f": "updated", "o": "after", "v": ["2026-10-20"]}], zone=utc
    )
    assert moment >= after


def test_last_days_opens_at_the_zones_local_midnight_n_minus_1_days_back():
    sql, params = _compile([{"f": "updated", "o": "last_days", "v": ["7"]}])
    assert sql == (
        "(updated_at >= ((date_trunc('day', NOW() AT TIME ZONE %s)"
        " - (%s * INTERVAL '1 day')) AT TIME ZONE %s))"
    )
    assert params == ["America/Los_Angeles", 6, "America/Los_Angeles"]
    _, params = _compile([{"f": "updated", "o": "last_days", "v": ["1"]}])
    assert params == ["America/Los_Angeles", 0, "America/Los_Angeles"]


def test_custom_sql_hook_owns_its_fragment():
    sql, params = _compile([{"f": "linked", "o": "is", "v": ["true"]}])
    assert sql == "((product_id IS NOT NULL))"
    assert params == []


def test_root_any_compiles_to_or_of_ands():
    raw = (
        '{"any":[{"f":"status","o":"in","v":["active"]},'
        '{"all":[{"f":"status","o":"in","v":["error"]},'
        '{"f":"linked","o":"is","v":["false"]}]}]}'
    )
    tree = FilterTree.parse(raw, _schema())
    sql, params = compile_tree(tree, zone=_LA, ctx={"listing": "l"})
    assert sql == (
        "(l.status = ANY(%s) OR (l.status = ANY(%s) AND (product_id IS NULL)))"
    )
    assert params == [["active"], ["error"]]
