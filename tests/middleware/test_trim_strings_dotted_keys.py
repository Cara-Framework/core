"""``TrimStrings`` trims a parameter whose NAME contains a dot.

It wrote each trimmed value back through ``InputBag.set``, which reads a dot
in the name as a nested path. So a dotted query parameter did not get trimmed
— it crashed the request:

* ``?utm.source=x`` → ``set`` created ``utm → source`` in the dict the loop
  was iterating: ``RuntimeError: dictionary changed size during iteration``;
* ``?a=1&a.b=x`` → ``set`` indexed into the string ``"1"``: ``TypeError``.

Both are a 500 any unauthenticated caller can trigger on every route.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

from cara.http import Request
from cara.middleware.http.TrimStrings import TrimStrings


def _request(query: bytes) -> Request:
    request = Request(MagicMock())
    request.load(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [],
            "query_string": query,
        },
        None,
    )
    return request


def _trim(query: bytes) -> dict:
    request = _request(query)
    middleware = TrimStrings.__new__(TrimStrings)

    async def next_fn(passed):
        return passed

    assert asyncio.run(middleware.handle(request, next_fn)) is request
    return request.query()


def test_dotted_parameter_name_is_trimmed_in_place():
    assert _trim(b"utm.source=%20news%20&name=%20Ada%20") == {
        "utm.source": "news",
        "name": "Ada",
    }


def test_dotted_name_beside_its_own_prefix_does_not_crash():
    assert _trim(b"a=%201%20&a.b=%20x%20") == {"a": "1", "a.b": "x"}


def test_nested_and_list_values_are_left_as_they_were():
    assert _trim(b"filter[name]=%20x%20&ids[]=%201%20&q=%20y%20") == {
        "filter": {"name": " x "},
        "ids": [" 1 "],
        "q": "y",
    }
