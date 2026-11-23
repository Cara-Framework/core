"""``Router.find_with_params``: the route and its parameters from ONE match.

The conductor found the route (one regex match per tried route) and then
called ``route.set_params_from_path`` — a second regex match of the same path
that also wrote the parameters onto the ``Route`` object every concurrent
request shares. The match that selects the route now returns what it
captured; nothing is stored on the route.
"""

from __future__ import annotations

import pytest

from cara.exceptions import MethodNotAllowedException, RouteNotFoundException
from cara.routing import Route, RouteCompiler, Router


def _handler(_request=None, _response=None):
    return None


@pytest.fixture
def regex_matches(monkeypatch) -> list[str]:
    """Count compiled-regex matches; the two-match paths must not run."""
    matched: list[str] = []
    real_match = RouteCompiler.match

    def counting_match(self, path):
        matched.append(path)
        return real_match(self, path)

    def second_match(self, *_args, **_kwargs):
        raise AssertionError("the path was matched a second time")

    monkeypatch.setattr(RouteCompiler, "match", counting_match)
    monkeypatch.setattr(RouteCompiler, "matches", second_match)
    monkeypatch.setattr(RouteCompiler, "extract_parameters", second_match)
    return matched


def test_the_route_and_its_parameters_come_from_one_match(regex_matches):
    route = Route.get("/orders/@id:int/lines/@line", _handler)
    router = Router(None, route)

    found, params = router.find_with_params("/orders/42/lines/a%20b", "GET")

    assert found is route
    assert params == {"id": "42", "line": "a b"}
    assert regex_matches == ["/orders/42/lines/a%20b"]


def test_nothing_is_written_onto_the_shared_route(regex_matches):
    route = Route.get("/orders/@id", _handler)
    router = Router(None, route)

    _found, first = router.find_with_params("/orders/1", "GET")
    _found, second = router.find_with_params("/orders/2", "GET")

    assert (first, second) == ({"id": "1"}, {"id": "2"})
    assert route.get_params() == {}
    assert first is not second


def test_an_absent_optional_parameter_is_none():
    router = Router(None, Route.get("/reports/@page?", _handler))

    assert router.find_with_params("/reports", "GET")[1] == {"page": None}


def test_find_is_find_with_params_without_the_parameters():
    route = Route.get("/orders/@id", _handler)
    router = Router(None, route)

    assert router.find("/orders/7", "GET") is route


def test_misses_still_raise_what_find_raised():
    router = Router(None, Route.post("/orders/@id", _handler))

    with pytest.raises(MethodNotAllowedException):
        router.find_with_params("/orders/9", "GET")
    with pytest.raises(RouteNotFoundException):
        router.find_with_params("/nowhere", "GET")
