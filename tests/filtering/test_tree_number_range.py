"""A number field refuses a value the database cannot hold.

``Decimal`` reads ``1e999999`` and ``1e-99999`` as finite numbers, so they
passed the kind's validation — and PostgreSQL's ``numeric`` (131072 digits
before the point, 16383 after) refused them at execution: a 500 for a value
the parser had accepted (filters review, 2026-11-11: ``/listings`` Price
``gt 1e999999``). Scientific notation reaches either bound in a few
characters, so the length cap never stopped it. The parse is the one place
that can say "expects numbers" with a path; it says it here.
"""

from __future__ import annotations

import json

import pytest

from cara.exceptions import FilterTreeError
from cara.filtering import FilterTree, TreeField, TreeSchema


def _schema() -> TreeSchema:
    return TreeSchema(
        "tests.number_range",
        (TreeField("price", "number", column="price", min_value=0),),
    )


def _parse(value: str) -> FilterTree:
    return FilterTree.parse(
        json.dumps([{"f": "price", "o": "gt", "v": [value]}]), _schema()
    )


@pytest.mark.parametrize("value", ["1e999999", "1e131072", "1e-16384", "1e-99999"])
def test_a_number_beyond_the_databases_numeric_is_refused(value):
    with pytest.raises(FilterTreeError, match="expects numbers"):
        _parse(value)


@pytest.mark.parametrize(
    "value", ["0", "10.5", "1e1", "1e131071", "1e-16383", "0.000001"]
)
def test_a_number_the_database_can_hold_still_parses(value):
    assert not _parse(value).is_empty
