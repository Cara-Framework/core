"""The root Gate resolves the user the auth middleware put on the request.

The provider's resolver used to be ``Auth.user()``, which re-derives identity
through the guard named in ``request._route_auth_guard``. A product credential
that is no registered guard (an API token stamped as ``api_token``) raised
"Guard not found", the Gate swallowed it, and a bare ``Gate.allows``, the
``can:`` middleware or a request-less ``@can`` judged a guest.
"""

from __future__ import annotations

import asyncio
import contextvars
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cara.authorization.AuthorizationProvider import AuthorizationProvider
from cara.decorators import can
from cara.http import current_request
from cara.middleware.http.CanPerform import CanPerform


class _User:
    id = 7


def _root_gate():
    application = MagicMock()
    AuthorizationProvider(application)._add_gate_service({})
    (name, gate), _ = application.bind.call_args
    assert name == "gate"
    return gate


def _token_request(user):
    # The stamped guard is deliberately unregistered: identity must come from
    # the request, never from re-running a guard.
    return SimpleNamespace(user=lambda: user, _route_auth_guard="custom", params={})


def _in_request(request, fn):
    def run():
        current_request.set(request)
        return fn()

    return contextvars.copy_context().run(run)


def test_bare_gate_check_sees_the_request_user():
    user = _User()
    gate = _root_gate()
    gate.define("orders.view", lambda acting, *_a: acting is user)

    assert _in_request(_token_request(user), lambda: gate.allows("orders.view"))


@pytest.mark.asyncio
async def test_can_middleware_passes_for_the_request_user():
    user = _User()
    gate = _root_gate()
    gate.define("orders.view", lambda acting, *_a: acting is user)
    application = MagicMock()
    application.make.return_value = gate
    middleware = CanPerform(application, "orders.view")
    request = _token_request(user)

    async def next_fn(_request):
        return "ok"

    async def run():
        current_request.set(request)
        return await middleware.handle(request, next_fn)

    # A task runs in a copy of the context, so the binding stays in it.
    assert await asyncio.create_task(run()) == "ok"


def test_request_less_can_decorator_sees_the_request_user(monkeypatch):
    user = _User()
    gate = _root_gate()
    gate.define("orders.view", lambda acting, *_a: acting is user)
    monkeypatch.setattr("cara.decorators.Authorization.Gate", gate)

    @can("orders.view")
    def handler():
        return "ok"

    assert _in_request(_token_request(user), handler) == "ok"
