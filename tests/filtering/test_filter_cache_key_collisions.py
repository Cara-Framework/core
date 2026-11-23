"""Two filter states with different SQL never share a cache key.

``FilterSet.cache_key`` is what ``FilterPipeline.cached(...)`` hashes into the
cache entry of a list page. It is assembled from ``name=value`` fragments
joined with ``|``, lists joined with ``,`` and dicts written ``{k=v;k=v}`` —
and the values used to be written into it raw, so a value containing one of
those characters spelled the key of a different filter state. A client could
then make the cached page of one filter combination answer for another.

Only the structural characters are escaped, so every key without them is
byte for byte what it was before (no cache is invalidated).
"""

from __future__ import annotations

from typing import Any

import pytest

from cara.filtering import Filter, FilterSet


class _Scalar(Filter):
    def __init__(self, name: str):
        self.name = name

    def validation_rules(self) -> dict[str, str]:
        return {self.name: "nullable"}

    def parse(self, payload: dict[str, Any]) -> Any:
        return payload.get(self.name)

    def where_sql(self, value: Any, *, ctx: Any = None) -> tuple[str, list[Any]]:
        return f"{self.name} = %s", [value]


_FILTERS = FilterSet([_Scalar("q"), _Scalar("stock"), _Scalar("brand"), _Scalar("price")])


def _key(state: dict[str, Any]) -> str:
    return _FILTERS.cache_key(state)


@pytest.mark.parametrize(
    ("one", "other"),
    [
        # a scalar spelling a second filter's fragment
        ({"q": "tv|stock=1"}, {"q": "tv", "stock": True}),
        # one list value containing the list separator vs two values
        ({"brand": ["a,b"]}, {"brand": ["a", "b"]}),
        # a dict value spelling a second dict entry
        ({"price": {"min": "1;max=9"}}, {"price": {"min": "1", "max": "9"}}),
        # a scalar spelling a dict
        ({"price": "{min=1}"}, {"price": {"min": "1"}}),
        # an escaped-looking literal vs the character it would escape to
        ({"q": "a%7Cb"}, {"q": "a|b"}),
    ],
)
def test_different_filter_states_get_different_keys(one, other):
    assert _FILTERS.where_clauses(one) != _FILTERS.where_clauses(other)
    assert _key(one) != _key(other)


def test_keys_without_structural_characters_are_unchanged():
    assert _key({}) == "no_filters"
    assert _key({"q": " Sony TV ", "stock": True}) == "q=Sony TV|stock=1"
    assert _key({"brand": ["lg", "sony", " lg "]}) == "brand=lg,lg,sony"
    assert _key({"price": {"min": 10, "max": 99.5}}) == "price={max=99.5;min=10}"
    assert _key({"stock": False, "q": None}) == "q=|stock=0"


def test_the_key_stays_independent_of_payload_and_list_order():
    assert _key({"q": "tv", "brand": ["b", "a"]}) == _key(
        {"brand": ["a", "b"], "q": "tv"}
    )


def test_the_url_codec_is_not_escaped():
    # ``encode_value`` feeds ``urlencode``, which does its own quoting.
    assert _Scalar("q").encode_value("tv|stock=1") == {"q": "tv|stock=1"}
    assert _Scalar("brand").encode_value(["a,b"]) == {"brand": "a,b"}
