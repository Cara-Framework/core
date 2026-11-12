"""``ws.auth`` answers for the handshake only, never for what runs after it.

``Authenticate.handle`` wrapped ``await next_fn(socket)`` in the same
``except Exception`` as the credential check. ``next_fn`` is everything behind
the middleware: ``ws.throttle`` (listed after it, so it can bucket by account)
and the controller's whole session. So:

* a throttled reconnect — ``Throttle`` closes 4008 and raises
  ``WebSocketException`` — was logged at ERROR as "WebSocket auth error" with a
  full traceback and re-raised as 4006 "Unauthorized WebSocket": 387 such
  errors in the API log 2026-11-11 09:45 → 2026-11-12 00:32, each burst the
  dashboard tabs reconnecting after an API reload;
* any WebSocketException a live session ends with (a client gone mid-send is
  4002), which the conductor treats as benign, became an auth error too;
* a real controller crash was reported as an auth failure and closed 4006
  instead of reaching the conductor's 1011 path.

The credential check keeps its catch-all (a guard that raises refuses the
socket); the rest of the pipeline propagates untouched.
"""

from __future__ import annotations

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

from cara.exceptions import WebSocketException
from cara.middleware.ws.Authenticate import Authenticate

module = sys.modules["cara.middleware.ws.Authenticate"]


def _middleware(*, authenticated: bool = True, raises: Exception | None = None):
    middleware = Authenticate.__new__(Authenticate)

    async def _authenticate(_socket):
        if raises is not None:
            raise raises
        return authenticated

    middleware._authenticate_socket = _authenticate
    middleware._origin_is_allowed = lambda _socket: True
    return middleware


def _socket():
    socket = MagicMock()
    socket.close = AsyncMock()
    socket.scope = {"type": "websocket", "headers": [], "path": "/ws/events"}
    return socket


@pytest.fixture
def errors(monkeypatch) -> list[str]:
    logged: list[str] = []
    log = MagicMock()
    log.error = lambda message, *args, **_kwargs: logged.append(message % args)
    monkeypatch.setattr(module, "Log", log)
    return logged


def test_a_throttled_handshake_keeps_its_4008_and_is_not_an_auth_error(errors):
    """Pinned wrong behaviour: re-raised as 4006 and logged at ERROR."""

    async def throttled(_socket):
        raise WebSocketException("WebSocket connect rate exceeded (>30 per 60s)", 4008)

    with pytest.raises(WebSocketException) as raised:
        asyncio.run(_middleware().handle(_socket(), throttled))

    assert raised.value.code == 4008
    assert errors == []


def test_a_session_that_crashes_after_auth_is_not_reported_as_unauthorized(errors):
    async def crashing_controller(_socket):
        raise RuntimeError("controller bug")

    with pytest.raises(RuntimeError, match="controller bug"):
        asyncio.run(_middleware().handle(_socket(), crashing_controller))

    assert errors == []


def test_a_guard_that_raises_still_refuses_the_socket(errors):
    """Unchanged: the credential check itself keeps its catch-all."""

    async def never(_socket):
        pytest.fail("the pipeline must not run for a socket that failed auth")

    with pytest.raises(WebSocketException) as raised:
        asyncio.run(
            _middleware(raises=RuntimeError("guard exploded")).handle(_socket(), never)
        )

    assert raised.value.code == 4006
    assert len(errors) == 1


def test_an_authenticated_socket_runs_the_pipeline(errors):
    ran: list[bool] = []

    async def controller(_socket):
        ran.append(True)
        return "done"

    assert asyncio.run(_middleware().handle(_socket(), controller)) == "done"
    assert ran == [True]
