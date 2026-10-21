"""QueryStringParser: bracket notation and bounded allocation.

The parser runs on every request's raw query string before routing,
auth or throttling, and on x-www-form-urlencoded bodies. A sparse index
pads its list up to the position, and the old cap applied per segment,
so ``a[10000]a[10000]...`` or ``k0[10000]=1&k1[10000]=1...`` allocated
10k slots per segment — gigabytes from a 15 KB query. Padding now draws
on one budget per parse; an index it cannot pay for is an object key.
"""

from __future__ import annotations

from cara.http.request.utils.QueryStringParser import (
    _MAX_PAD_SLOTS,
    QueryStringParser,
)


def _parse(query: str) -> dict:
    return QueryStringParser().parse(query)


def _list_slots(value) -> int:
    """Total list slots in a parsed tree (iterative, any depth)."""
    total, stack = 0, [value]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            total += len(node)
            stack.extend(node)
        elif isinstance(node, dict):
            stack.extend(node.values())
    return total


def test_dense_indices_build_lists():
    assert _parse("tags[0]=a&tags[1]=b") == {"tags": ["a", "b"]}
    assert _parse("users[0][name]=J") == {"users": [{"name": "J"}]}
    assert _parse("items[]=a&items[]=b") == {"items": ["a", "b"]}


def test_out_of_order_small_indices_keep_their_positions():
    assert _parse("tags[1]=b&tags[0]=a") == {"tags": ["a", "b"]}


def test_chained_sparse_segments_stay_within_the_budget():
    query = "a" + "[10000]a" * 199 + "[10000]=1"

    parsed = _parse(query)

    assert _list_slots(parsed) <= _MAX_PAD_SLOTS


def test_many_sparse_keys_share_one_budget():
    query = "&".join(f"k{i}[10000][x]=1" for i in range(1200))

    parsed = _parse(query)

    assert _list_slots(parsed) <= _MAX_PAD_SLOTS
    assert parsed["k0"] == {"10000": {"x": "1"}}


def test_index_past_the_budget_is_an_object_key():
    assert _parse("a[5000]=x") == {"a": {"5000": "x"}}
    assert _parse("a[99999999]=x") == {"a": {"99999999": "x"}}


def test_numeric_lookalike_indices_do_not_raise():
    # ``'²'.isdigit()`` is True but ``int('²')`` raises; 4300+ digits
    # exceed ``int``'s string-conversion limit. Both crashed the
    # request before routing.
    assert _parse("a[%C2%B2]=1") == {"a": {"²": "1"}}

    digits = "1" * 5000
    assert _parse(f"a[{digits}]=1") == {"a": {digits: "1"}}
