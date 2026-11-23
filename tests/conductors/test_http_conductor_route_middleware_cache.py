"""A route's middleware stack is resolved once, and never served stale.

``HttpConductor.get_route_middleware`` ran on every request: each declared
alias was resolved through the capsule and the stack was sorted again, which
rebuilds the capsule's priority index each time (measured 2026-11-23: ~3 us
per request for a five-entry declaration, ~0.25 us once kept). Routes declare
their middleware once and the capsule is filled at boot, so the ordered stack
is kept per declaration.

Every capsule registration method bumps ``MiddlewareCapsule.version``; the
conductor's cached stacks are served only while that counter is the one they
were resolved at. Both halves are pinned here — the resolver runs once across N requests, and every way of
registering late is honoured by the very next request.
"""

from __future__ import annotations

import pytest

from cara.conductors.http.HttpConductor import HttpConductor
from cara.exceptions import MiddlewareNotFoundException
from cara.middleware import MiddlewareCapsule


class _Base:
    def __init__(self, application=None) -> None:
        self.application = application

    @classmethod
    def create_with_parameters(cls, application, parameters):
        instance = cls(application)
        instance.parameters = list(parameters)
        return instance


class _Auth(_Base): ...


class _Throttle(_Base): ...


class _Tenant(_Base): ...


class _Can(_Base): ...


class _Json(_Base): ...


class _Cors(_Base): ...


class _OtherAuth(_Base): ...


class _Application:
    def __init__(self, capsule) -> None:
        self.capsule = capsule

    def make(self, name: str):
        assert name == "middleware_http"
        return self.capsule


class _Route:
    def __init__(self, *middleware) -> None:
        self._middleware = list(middleware)

    def get_middleware(self) -> list:
        return self._middleware


def _capsule() -> MiddlewareCapsule:
    capsule = MiddlewareCapsule(application=None)
    capsule.add_alias("auth", _Auth)
    capsule.add_alias("throttle", _Throttle)
    capsule.add_alias("tenant", _Tenant)
    capsule.add_alias("can", _Can)
    capsule.add_to_group("api", _Json)
    capsule.add_to_group("api", _Cors)
    capsule.set_priority([_Throttle, _Auth, _Tenant, _Can])
    return capsule


_DECLARED = ("can:orders.read", "api", "tenant", "auth:jwt", "throttle:api")


def _names(stack: list) -> list[str]:
    return [getattr(mw, "__base_middleware__", mw).__name__ for mw in stack]


@pytest.fixture
def calls(monkeypatch) -> dict[str, int]:
    """Count the capsule's two per-request costs: resolution and the sort."""
    counted = {"resolve": 0, "sort": 0}
    resolve = MiddlewareCapsule.resolve_middleware
    sort = MiddlewareCapsule.sort_by_priority

    def _resolve(self, middleware):
        counted["resolve"] += 1
        return resolve(self, middleware)

    def _sort(self, middleware):
        counted["sort"] += 1
        return sort(self, middleware)

    monkeypatch.setattr(MiddlewareCapsule, "resolve_middleware", _resolve)
    monkeypatch.setattr(MiddlewareCapsule, "sort_by_priority", _sort)
    return counted


def test_the_resolver_runs_once_across_many_requests(calls):
    capsule = _capsule()
    conductor = HttpConductor(_Application(capsule))
    route = _Route(*_DECLARED)

    stacks = [conductor.get_route_middleware(route) for _ in range(500)]

    assert calls == {"resolve": len(_DECLARED), "sort": 1}
    assert _names(stacks[0]) == [
        "_Throttle",
        "_Auth",
        "_Tenant",
        "_Can",
        "_Json",
        "_Cors",
    ]
    assert all(stack == stacks[0] for stack in stacks)


def test_the_cached_stack_is_what_an_uncached_resolution_returns(calls):
    capsule = _capsule()
    conductor = HttpConductor(_Application(capsule))
    route = _Route(*_DECLARED)

    kept = [conductor.get_route_middleware(route) for _ in range(3)][-1]
    fresh = HttpConductor._resolve_route_middleware(capsule, tuple(_DECLARED))

    # Same classes in the same order, parameterized proxies included.
    assert kept == fresh


def test_each_request_gets_its_own_list(calls):
    conductor = HttpConductor(_Application(_capsule()))
    route = _Route(*_DECLARED)

    first = conductor.get_route_middleware(route)
    first.clear()  # a pipeline is free to consume the list it was handed

    assert len(conductor.get_route_middleware(route)) == 6


def test_routes_with_the_same_declaration_share_one_resolution(calls):
    conductor = HttpConductor(_Application(_capsule()))

    for _ in range(10):
        conductor.get_route_middleware(_Route(*_DECLARED))
        conductor.get_route_middleware(_Route("auth:jwt"))

    assert calls == {"resolve": len(_DECLARED) + 1, "sort": 2}


def test_a_route_that_declares_more_later_is_resolved_again(calls):
    conductor = HttpConductor(_Application(_capsule()))
    route = _Route("auth:jwt")
    assert _names(conductor.get_route_middleware(route)) == ["_Auth"]

    route.get_middleware().append("throttle:api")

    assert _names(conductor.get_route_middleware(route)) == ["_Throttle", "_Auth"]


# ── late registrations: honoured by the very next request ─────────────────
def test_a_late_alias_registration_is_honoured(calls):
    capsule = _capsule()
    conductor = HttpConductor(_Application(capsule))
    route = _Route("auth:jwt", "verified")

    with pytest.raises(MiddlewareNotFoundException):
        conductor.get_route_middleware(route)
    # Still unresolved on the next request: a miss is never remembered.
    with pytest.raises(MiddlewareNotFoundException):
        conductor.get_route_middleware(route)

    capsule.add_alias("verified", _Cors)

    assert _names(conductor.get_route_middleware(route)) == ["_Auth", "_Cors"]


def test_an_alias_replaced_under_the_same_name_is_honoured(calls):
    """Nothing about the capsule's SIZE changes here; only its content."""
    capsule = _capsule()
    conductor = HttpConductor(_Application(capsule))
    route = _Route("auth:jwt", "tenant")
    assert _names(conductor.get_route_middleware(route)) == ["_Auth", "_Tenant"]

    capsule.add_alias("auth", _OtherAuth)

    assert _names(conductor.get_route_middleware(route)) == ["_Tenant", "_OtherAuth"]


def test_a_late_group_member_and_a_cleared_group_are_honoured(calls):
    capsule = _capsule()
    conductor = HttpConductor(_Application(capsule))
    route = _Route("api")
    assert _names(conductor.get_route_middleware(route)) == ["_Json", "_Cors"]

    capsule.add_to_group("api", _Tenant)
    assert _names(conductor.get_route_middleware(route)) == ["_Tenant", "_Json", "_Cors"]

    capsule.remove("api")
    assert conductor.get_route_middleware(route) == []


def test_a_late_priority_change_is_honoured(calls):
    capsule = _capsule()
    conductor = HttpConductor(_Application(capsule))
    route = _Route("auth:jwt", "throttle:api")
    assert _names(conductor.get_route_middleware(route)) == ["_Throttle", "_Auth"]

    # Same length, different order.
    capsule.set_priority([_Auth, _Throttle, _Tenant, _Can])
    assert _names(conductor.get_route_middleware(route)) == ["_Auth", "_Throttle"]

    # Back again, through the registration method that bumps ``version``.
    capsule.set_priority([_Throttle, _Auth, _Tenant, _Can])
    assert _names(conductor.get_route_middleware(route)) == ["_Throttle", "_Auth"]


def test_a_rebound_capsule_is_honoured(calls):
    application = _Application(_capsule())
    conductor = HttpConductor(application)
    route = _Route("auth:jwt")
    assert _names(conductor.get_route_middleware(route)) == ["_Auth"]

    replacement = _capsule()
    replacement.add_alias("auth", _OtherAuth)
    application.capsule = replacement

    assert _names(conductor.get_route_middleware(route)) == ["_OtherAuth"]


def test_a_registration_drops_every_cached_stack_then_settles_again(calls):
    capsule = _capsule()
    conductor = HttpConductor(_Application(capsule))
    route = _Route(*_DECLARED)
    conductor.get_route_middleware(route)
    capsule.add_alias("verified", _Cors)
    calls.update(resolve=0, sort=0)

    for _ in range(50):
        conductor.get_route_middleware(route)

    assert calls == {"resolve": len(_DECLARED), "sort": 1}


# ── what is never cached ──────────────────────────────────────────────────
def test_a_capsule_that_is_not_the_framework_class_is_never_cached():
    """A subclass or a double may resolve from state the conductor cannot
    compare, so it is asked on every request, exactly as before."""
    asked: list[str] = []

    class _OwnCapsule(MiddlewareCapsule):
        def resolve_middleware(self, middleware):
            asked.append(middleware)
            return super().resolve_middleware(middleware)

    capsule = _OwnCapsule(application=None)
    capsule.add_alias("auth", _Auth)
    conductor = HttpConductor(_Application(capsule))
    route = _Route("auth")

    for _ in range(3):
        assert conductor.get_route_middleware(route) == [_Auth]

    assert asked == ["auth", "auth", "auth"]


def test_a_conductor_assembled_without_init_still_resolves(calls):
    conductor = HttpConductor.__new__(HttpConductor)
    conductor.application = _Application(_capsule())
    route = _Route("auth:jwt", "throttle:api")

    for _ in range(3):
        assert _names(conductor.get_route_middleware(route)) == ["_Throttle", "_Auth"]

    assert calls == {"resolve": 2, "sort": 1}
