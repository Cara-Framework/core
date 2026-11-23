"""A parameterized route middleware is prepared once, not once per request.

The HTTP conductor resolves a route's middleware on every request. For a
declaration such as ``throttle:api`` that used to mean, per request:

* ``MiddlewareCapsule`` built a brand-new proxy CLASS (``type()`` work, and a
  class is cyclic garbage only the cycle collector frees);
* ``MiddlewareParameterParser`` re-ran ``inspect.signature`` on the middleware's
  ``__init__`` and re-evaluated each of its string annotations.

Measured on ``throttle:api``: 18 us per resolve-and-instantiate before, 2.3 us
after. These tests pin the counts and that nothing request-scoped is shared.
"""

from __future__ import annotations

import importlib

from cara.middleware.Middleware import Middleware
from cara.middleware.MiddlewareCapsule import MiddlewareCapsule

_parser_module = importlib.import_module("cara.middleware.MiddlewareParameterParser")


class _Throttle(Middleware):
    def __init__(self, application, limiter: str | None = None):
        super().__init__(application)
        self.limiter_name = limiter

    async def handle(self, request, next_fn):
        return await next_fn(request)


class _Guarded(Middleware):
    def __init__(self, application, guards: list[str] | None = None):
        super().__init__(application)
        self.guards = guards

    async def handle(self, request, next_fn):
        return await next_fn(request)


def _capsule() -> MiddlewareCapsule:
    capsule = MiddlewareCapsule(application=None)
    capsule.add_alias("throttle", _Throttle)
    capsule.add_alias("auth", _Guarded)
    return capsule


def test_same_declaration_resolves_to_one_proxy_class():
    capsule = _capsule()

    first = capsule.resolve_middleware("throttle:api")
    second = capsule.resolve_middleware("throttle:api")

    assert first is second
    assert first.__base_middleware__ is _Throttle
    assert first.__name__ == "_ThrottleWithParams"


def test_different_parameters_get_different_proxies():
    capsule = _capsule()

    api = capsule.resolve_middleware("throttle:api")
    login = capsule.resolve_middleware("throttle:login")

    assert api is not login
    assert api("app").limiter_name == "api"
    assert login("app").limiter_name == "login"


def test_each_request_still_gets_its_own_middleware_instance():
    proxy = _capsule().resolve_middleware("auth:jwt,session")

    one, two = proxy("app"), proxy("app")

    assert one._instance is not two._instance
    assert one.guards == ["jwt", "session"]
    # A middleware that mutates its own parameters must not reach the next
    # request's instance.
    one.guards.append("leaked")
    assert two.guards == ["jwt", "session"]
    assert proxy("app").guards == ["jwt", "session"]


def test_an_alias_rebound_to_another_class_is_not_served_the_old_proxy():
    capsule = _capsule()
    before = capsule.resolve_middleware("throttle:api")

    class _OtherThrottle(_Throttle):
        pass

    capsule.add_alias("throttle", _OtherThrottle)
    after = capsule.resolve_middleware("throttle:api")

    assert after is not before
    assert after.__base_middleware__ is _OtherThrottle


def test_signature_is_inspected_once_per_middleware_class(monkeypatch):
    class _Counted(Middleware):
        def __init__(self, application, count: int = 5, label: str | None = None):
            super().__init__(application)
            self.count = count
            self.label = label

        async def handle(self, request, next_fn):
            return await next_fn(request)

    inspected: list[object] = []
    real_signature = _parser_module.inspect.signature

    def counting_signature(target, *args, **kwargs):
        inspected.append(target)
        return real_signature(target, *args, **kwargs)

    monkeypatch.setattr(_parser_module.inspect, "signature", counting_signature)

    capsule = MiddlewareCapsule(application=None)
    capsule.add_alias("counted", _Counted)
    for _ in range(10):
        instance = capsule.resolve_middleware("counted:7,x")("app")
        assert (instance.count, instance.label) == (7, "x")

    assert inspected.count(_Counted.__init__) == 1
