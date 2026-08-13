"""``compile_tree`` — the tree-to-SQL honesty contract.

Every operator compiles to exactly the SQL it claims: real ``= ANY``,
real ``OR`` groups, NULL-inclusive negatives on nullable fields only,
escaped ILIKE containment, UTC day-boundary date math, and entity
values bound from the app's resolved + authorized mapping (an
unresolved id raises instead of degrading the filter).
"""

from __future__ import annotations

import datetime
import json
from decimal import Decimal

import pytest

from cara.exceptions import FilterTreeCompileError
from cara.filtering import (
    FilterTree,
    TreeField,
    TreeSchema,
    compile_tree,
)

_CHN = "CHN" + "0123456789ABCDEFGHJKMNPQRS"
_UTC = datetime.UTC


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
    return compile_tree(tree, **kwargs)


def test_empty_tree_compiles_to_none():
    assert compile_tree(FilterTree.empty(_schema())) is None


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


def test_date_operators_use_utc_day_boundaries():
    sql, params = _compile([{"f": "updated", "o": "before", "v": ["2026-08-01"]}])
    assert sql == "(updated_at < %s)"
    assert params == [datetime.datetime(2026, 8, 1, tzinfo=_UTC)]
    sql, params = _compile([{"f": "updated", "o": "after", "v": ["2026-08-01"]}])
    assert sql == "(updated_at >= %s)"
    assert params == [datetime.datetime(2026, 8, 2, tzinfo=_UTC)]
    sql, params = _compile(
        [{"f": "updated", "o": "between", "v": ["2026-08-03", "2026-08-01"]}]
    )
    assert sql == "((updated_at >= %s AND updated_at < %s))"
    assert params == [
        datetime.datetime(2026, 8, 1, tzinfo=_UTC),
        datetime.datetime(2026, 8, 4, tzinfo=_UTC),
    ]
    sql, params = _compile([{"f": "updated", "o": "last_days", "v": ["7"]}])
    assert sql == "(updated_at >= NOW() - (%s * INTERVAL '1 day'))"
    assert params == [7]


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
    sql, params = compile_tree(tree, ctx={"listing": "l"})
    assert sql == (
        "(l.status = ANY(%s) OR (l.status = ANY(%s) AND (product_id IS NULL)))"
    )
    assert params == [["active"], ["error"]]
