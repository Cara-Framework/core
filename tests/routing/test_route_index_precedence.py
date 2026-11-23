"""``Router.find`` narrows its candidates without changing which route wins.

The router used to try every route of the request's method, one regex after
another, in registration order: ~300 failed matches for a late route of a
330-route table, and every route of every method for a 404. ``_RouteIndex``
files routes under their leading literal segments so only the routes that
could match are tried.

What must not move is PRECEDENCE: of two routes that both match, the one
registered first wins. These tests pin that against a reference full scan —
over hand-picked cases and over random route tables — and pin the number of
patterns actually tried.
"""

from __future__ import annotations

import random

import pytest

from cara.exceptions import MethodNotAllowedException, RouteNotFoundException
from cara.routing import Route, RouteCompiler, Router


def _handler(_request=None, _response=None):
    return None


def _full_scan(router: Router, path: str, method: str):
    """What the router did before the index: first match in registration order."""
    for route in router.routes_by_method.get(method.upper(), []):
        if route.matches(path, method):
            return route
    return None


def _filler(count: int = 12) -> list[Route]:
    """Unrelated routes, enough to push a bucket past the direct-scan size."""
    return [Route.get(f"/filler{index}/@id", _handler) for index in range(count)]


# ── Precedence ───────────────────────────────────────────────────────


def test_parameter_route_registered_first_still_shadows_a_later_literal():
    by_id = Route.get("/orders/@id", _handler)
    export = Route.get("/orders/export", _handler)
    router = Router(None, _filler(), by_id, export)

    assert router.find("/orders/export", "GET") is by_id


def test_literal_route_registered_first_wins_over_a_later_parameter_route():
    export = Route.get("/orders/export", _handler)
    by_id = Route.get("/orders/@id", _handler)
    router = Router(None, _filler(), export, by_id)

    assert router.find("/orders/export", "GET") is export
    assert router.find("/orders/42", "GET") is by_id


def test_root_level_parameter_route_competes_with_every_literal_route():
    catch_all = Route.get("/@tenant/@rest:any", _handler)
    literal = Route.get("/api/health", _handler)
    router = Router(None, _filler(), catch_all, literal)

    assert router.find("/api/health", "GET") is catch_all


def test_optional_trailing_parameter_and_trailing_slash_resolve_as_before():
    listing = Route.get("/reports/@year?", _handler)
    router = Router(None, _filler(), listing)

    for path in ("/reports", "/reports/", "/reports/2026", "/reports/2026/"):
        assert router.find(path, "GET") is listing


def test_root_route_and_paths_without_a_leading_slash():
    home = Route.get("/", _handler)
    router = Router(None, _filler(), home)

    assert router.find("/", "GET") is home
    assert router.find("", "GET") is home
    with pytest.raises(RouteNotFoundException):
        router.find("filler1/5", "GET")


def test_path_with_a_trailing_newline_resolves_as_the_full_scan_does():
    # ``$`` also matches before a final newline, so this path has always
    # matched; the index must not turn it into a 404.
    health = Route.get("/health", _handler)
    nested = Route.get("/api/health", _handler)
    router = Router(None, _filler(), health, nested)

    assert router.find("/health\n", "GET") is health
    assert router.find("/api/health\n", "GET") is nested


def test_route_with_its_own_matcher_is_always_a_candidate():
    class _AnywhereRoute(Route):
        def matches(self, path: str, method: str) -> bool:
            return path == "/somewhere/else"

    odd = _AnywhereRoute("/declared/here", _handler, ["get"])
    router = Router(None, _filler(), odd)

    assert router.find("/somewhere/else", "GET") is odd


# ── The index follows the route table ────────────────────────────────


def test_route_added_after_the_first_lookup_is_found():
    router = Router(None, _filler())
    with pytest.raises(RouteNotFoundException):
        router.find("/late/7", "GET")

    late = Route.get("/late/@id", _handler)
    router.add(late)

    assert router.find("/late/7", "GET") is late


def test_regrouping_a_registered_route_moves_it_in_the_index():
    moved = Route.get("/widgets/@id", _handler)
    router = Router(None, _filler(), moved)
    assert router.find("/widgets/3", "GET") is moved

    Route.group(moved, prefix="/v2")

    assert router.find("/v2/widgets/3", "GET") is moved
    with pytest.raises(RouteNotFoundException):
        router.find("/widgets/3", "GET")


def test_first_compile_of_a_new_route_does_not_invalidate_indexes():
    # The OPTIONS preflight builds a throwaway Route per request; if that
    # counted as a re-compile every preflight would rebuild every index.
    before = RouteCompiler.recompiles

    Route.get("/brand/new/@id", _handler)

    assert RouteCompiler.recompiles == before


def test_method_not_allowed_and_preflight_still_see_every_method():
    router = Router(
        None,
        _filler(),
        Route.get("/things/@id", _handler),
        Route.delete("/things/@id", _handler),
        [Route.post(f"/other{index}", _handler) for index in range(10)],
    )

    with pytest.raises(MethodNotAllowedException) as refused:
        router.find("/things/9", "POST")
    assert refused.value.allowed == ["GET", "HEAD", "DELETE"]
    assert router.get_allowed_methods("/nothing/here") == []


# ── How many patterns are tried ──────────────────────────────────────


def test_a_late_route_is_found_without_trying_the_routes_before_it(monkeypatch):
    routes = []
    for index in range(150):
        routes.append(Route.get(f"/api/res{index}", _handler))
        routes.append(Route.get(f"/api/res{index}/@id", _handler))
    router = Router(None, routes)

    # Selection goes through ``Route.match`` — one regex match that also
    # returns the captured parameters — so that is what is counted.
    tried: list[str] = []
    real_match = Route.match

    def counting_match(self, path, method):
        tried.append(self.url)
        return real_match(self, path, method)

    monkeypatch.setattr(Route, "match", counting_match)

    assert router.find("/api/res149/abc", "GET") is routes[-1]
    # Only the two routes filed under ``/api/res149`` — it was all 300.
    assert tried == ["/api/res149", "/api/res149/@id"]

    tried.clear()
    with pytest.raises(RouteNotFoundException):
        router.find("/api/nope/abc", "GET")
    assert tried == []  # was every route of every method


# ── Differential: random tables against the full scan ────────────────

_LITERALS = ("api", "orders", "export", "v1", "a", "b")
_PARAMETERS = ("@id", "@id:int", "@slug:slug", "@page?", "@rest:any", "@flag:bool")
_PATH_SEGMENTS = (*_LITERALS, "42", "true", "x-y", "", "zzz")


def _random_url(rng: random.Random) -> str:
    parts = [
        rng.choice(_PARAMETERS) if rng.random() < 0.35 else rng.choice(_LITERALS)
        for _ in range(rng.randint(0, 4))
    ]
    return "/" + "/".join(parts)


def _random_path(rng: random.Random) -> str:
    path = "/".join(rng.choice(_PATH_SEGMENTS) for _ in range(rng.randint(0, 5)))
    if rng.random() < 0.9:
        path = "/" + path
    if rng.random() < 0.15:
        path += "/"
    if rng.random() < 0.05:
        path += "\n"
    return path


@pytest.mark.parametrize("seed", range(12))
def test_index_and_full_scan_agree_on_random_tables(seed):
    rng = random.Random(seed)
    factories = (Route.get, Route.post, Route.delete, Route.any)
    routes = [rng.choice(factories)(_random_url(rng), _handler) for _ in range(90)]
    router = Router(None, routes)

    for _ in range(400):
        path = _random_path(rng)
        for method in ("GET", "POST", "DELETE", "PATCH"):
            expected = _full_scan(router, path, method)
            assert router._first_match(method, path, method) is expected, (
                seed,
                method,
                path,
            )
