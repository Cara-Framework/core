"""The HTTP conductor keeps per-request state per request, and builds the rest once.

Three defects in the one singleton that serves every request:

  * route parameters were written onto the shared ``Route``
    (``route.set_params_from_path``), so two concurrent requests on the same
    route left the later request's parameters on it for both;
  * a ``ResetHttpAuth`` was constructed for every request, though it holds
    nothing but the application;
  * the global middleware stack was resolved and priority-sorted again on
    every request. It is now served by ``_MiddlewareStackCache`` (as the route
    stacks are) and refreshed when ``MiddlewareCapsule.version`` changes.
"""

from __future__ import annotations

import asyncio
import importlib
from typing import Any

import pytest

from cara.middleware import MiddlewareCapsule
from cara.routing import Route, Router

_conductor_mod = importlib.import_module("cara.conductors.http.HttpConductor")
HttpConductor = _conductor_mod.HttpConductor


def _handler(_request=None, _response=None):
    return None


class _Request:
    def __init__(self) -> None:
        self.params: dict[str, Any] = {}
        self.route = None

    def load(self, scope: dict, _receive: Any) -> _Request:
        self.path, self.method = scope["path"], scope["method"]
        return self

    def set_route(self, route) -> _Request:
        self.route = route
        return self

    def load_params(self, params: dict[str, Any]) -> _Request:
        self.params = params
        return self


class _Response:
    def __init__(self) -> None:
        self.payload = None
        self.sent = False

    def json(self, payload) -> None:
        self.payload = payload

    def is_sent(self) -> bool:
        return self.sent

    async def __call__(self, _scope, _receive, _send) -> None:
        self.sent = True


class _ResetAuth:
    built = 0

    def __init__(self, application) -> None:
        type(self).built += 1
        self.application = application

    async def terminate(self, _request, _response) -> None:
        return None


class _Application:
    def __init__(self, route: Route) -> None:
        self.capsule = MiddlewareCapsule(application=None)
        self.router = Router(None, route)

    def make(self, name: str):
        assert name == "middleware_http"
        return self.capsule


class _Mw:
    async def handle(self, request, next_fn):
        return await next_fn(request)


class _Other(_Mw): ...


@pytest.fixture
def reset_auth(monkeypatch) -> type[_ResetAuth]:
    _ResetAuth.built = 0
    monkeypatch.setattr(_conductor_mod, "ResetHttpAuth", _ResetAuth)
    return _ResetAuth


async def _serve(conductor: HttpConductor, path: str) -> _Request:
    request = _Request()
    scope = {"type": "http", "path": path, "method": "GET"}
    await conductor._handle_request(scope, None, None, request, _Response())
    return request


@pytest.mark.asyncio
async def test_concurrent_requests_keep_their_own_parameters(reset_auth):
    """Both requests are inside the controller at once; each reads its own id
    and the shared route carries none."""
    route = Route.get("/orders/@id", _handler)
    both_inside = asyncio.Barrier(2)
    seen: dict[str, Any] = {}

    async def controller(arguments):
        request, _response = arguments
        await both_inside.wait()
        seen[request.path] = request.params
        return {"id": request.params["id"]}

    route.controller = type("_C", (), {"handle": staticmethod(controller)})()
    conductor = HttpConductor(_Application(route))
    conductor.router = conductor.application.router

    first, second = await asyncio.gather(
        _serve(conductor, "/orders/1"), _serve(conductor, "/orders/2")
    )

    assert (first.params, second.params) == ({"id": "1"}, {"id": "2"})
    assert seen == {"/orders/1": {"id": "1"}, "/orders/2": {"id": "2"}}
    assert route.get_params() == {}


@pytest.mark.asyncio
async def test_reset_http_auth_is_built_once_per_conductor(reset_auth):
    route = Route.get("/ping", _handler)

    async def controller(_arguments):
        return {"ok": True}

    route.controller = type("_C", (), {"handle": staticmethod(controller)})()
    conductor = HttpConductor(_Application(route))
    conductor.router = conductor.application.router

    for _ in range(3):
        await _serve(conductor, "/ping")

    assert reset_auth.built == 1


def test_the_global_stack_is_resolved_once_per_capsule_version(monkeypatch):
    route = Route.get("/ping", _handler)
    application = _Application(route)
    capsule = application.capsule
    capsule.add_global(_Mw)
    conductor = HttpConductor(application)
    resolved = {"n": 0}
    real = MiddlewareCapsule.get_global_middleware

    def counting(self):
        resolved["n"] += 1
        return real(self)

    monkeypatch.setattr(MiddlewareCapsule, "get_global_middleware", counting)

    stacks = [conductor.get_global_middleware() for _ in range(50)]
    assert resolved["n"] == 1
    assert stacks[0] == [_Mw]
    stacks[0].clear()  # a pipeline may consume the list it was handed
    assert conductor.get_global_middleware() == [_Mw]

    capsule.add_global(_Other)
    assert conductor.get_global_middleware() == [_Mw, _Other]
    capsule.set_priority([_Other, _Mw])
    assert conductor.get_global_middleware() == [_Other, _Mw]
    assert resolved["n"] == 3
