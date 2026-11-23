"""The WebSocket handshake bucket is the ROUTE, not the path a client typed.

The live defect: ``ws.throttle`` keyed its bucket on the raw request path, so
every value of a route parameter was a bucket of its own. On a route like
``/ws/products/@product_id`` one address opened unlimited handshakes by
counting the id upward. The key is now the pattern of the route the conductor
attached to the socket; the raw path is used only when no route is attached.
"""

from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from cara.exceptions import WebSocketException
from cara.middleware.ws import Throttle
from cara.rates import Limit, RateLimitDecision

_ws_module = sys.modules["cara.middleware.ws.Throttle"]


def _throttle() -> Throttle:
    """The middleware without the provider boot ``Middleware.__init__`` runs."""
    middleware = Throttle.__new__(Throttle)
    middleware.name = "ws_connect"
    return middleware


def _socket(path: str, route_url: str | None):
    socket = SimpleNamespace(
        scope={
            "type": "websocket",
            "client": ("203.0.113.42", 51234),
            "headers": [],
            "path": path,
        },
        route=None if route_url is None else SimpleNamespace(url=route_url),
        close=AsyncMock(),
        user=lambda: None,
    )
    return socket


@pytest.fixture
def one_bucket_of_two(monkeypatch) -> list[str]:
    """A GCRA stand-in: each key admits two handshakes, then refuses."""
    spent: list[str] = []

    def attempt(limit: Limit) -> RateLimitDecision:
        spent.append(limit.key)
        allowed = spent.count(limit.key) <= 2
        return RateLimitDecision(
            allowed=allowed,
            limit=2,
            period_seconds=60,
            remaining=0,
            retry_after=0 if allowed else 30,
            reset_after=60,
        )

    monkeypatch.setattr(_ws_module, "attempt_rate_limit", attempt)
    monkeypatch.setattr(_ws_module, "Log", MagicMock())
    monkeypatch.setattr(Throttle, "_limits", lambda self: (2, 60))
    return spent


def _handshake(socket) -> None:
    asyncio.run(_throttle().handle(socket, AsyncMock(return_value="ok")))


def test_varying_a_route_parameter_does_not_mint_a_fresh_bucket(
    one_bucket_of_two,
) -> None:
    _handshake(_socket("/ws/products/1", "/ws/products/@product_id"))
    _handshake(_socket("/ws/products/2", "/ws/products/@product_id"))

    refused = _socket("/ws/products/3", "/ws/products/@product_id")
    with pytest.raises(WebSocketException) as refusal:
        _handshake(refused)

    assert refusal.value.code == 4008
    refused.close.assert_awaited_once_with(code=4008)
    assert set(one_bucket_of_two) == {
        "ws:ws_connect:203.0.113.42:/ws/products/@product_id"
    }


def test_different_routes_keep_their_own_buckets(one_bucket_of_two) -> None:
    _handshake(_socket("/ws/products/1", "/ws/products/@product_id"))
    _handshake(_socket("/ws/deals", "/ws/deals"))

    assert one_bucket_of_two == [
        "ws:ws_connect:203.0.113.42:/ws/products/@product_id",
        "ws:ws_connect:203.0.113.42:/ws/deals",
    ]


def test_a_socket_without_a_route_falls_back_to_its_path(one_bucket_of_two) -> None:
    _handshake(_socket("/ws/raw", None))

    assert one_bucket_of_two == ["ws:ws_connect:203.0.113.42:/ws/raw"]
