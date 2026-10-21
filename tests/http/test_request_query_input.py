"""``Request.all()`` keeps explicit query arrays as lists.

``all()`` used to flatten any one-item query list unless its key ended
in ``[]`` — but the parser has already stripped the brackets, so the
guard never fired: ``?ids[]=5`` came back as ``'5'`` while
``?ids[]=5&ids[]=6`` came back as a list. An ``array`` rule 422'd the
one-element case and a loop over the value walked its characters.
"""

from __future__ import annotations

import importlib

import pytest

from cara.http import Request

_body_parsing = importlib.import_module("cara.http.request.mixins.MakesBodyParsing")


@pytest.fixture(autouse=True)
def _body_limits(monkeypatch) -> None:
    """Body parsing reads boot config; pin its limits so no app is needed."""
    monkeypatch.setattr(
        _body_parsing,
        "_body_limits",
        lambda: {"MAX_BODY_SIZE": 1 << 20, "MAX_FILE_SIZE": 1 << 20, "MAX_FILES": 1},
    )


def _query_request(query: bytes) -> Request:
    async def receive() -> dict:
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(None).load({"type": "http", "query_string": query}, receive)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (b"ids[]=5", {"ids": ["5"]}),
        (b"ids[0]=5", {"ids": ["5"]}),
        (b"ids[]=5&ids[]=6", {"ids": ["5", "6"]}),
        (b"a=1", {"a": "1"}),
        (b"a=1&a=2", {"a": "1"}),
    ],
)
async def test_all_keeps_explicit_query_arrays_as_lists(
    query: bytes, expected: dict
) -> None:
    assert await _query_request(query).all() == expected


@pytest.mark.parametrize(
    ("query", "name", "expected"),
    [
        (b"ids[]=5", "ids", ["5"]),
        (b"ids[]=5", "ids[]", ["5"]),
        (b"ids[0]=5", "ids", ["5"]),
        (b"f[b][]=5", "f.b", ["5"]),
        (b"a=1", "a", "1"),
        (b"a=1", "missing[]", []),
    ],
)
def test_query_accessor_agrees_with_all(query: bytes, name: str, expected) -> None:
    assert _query_request(query).query(name) == expected
