"""A WebSocket handshake spends the same GCRA authority HTTP does, per channel.

``ws.throttle`` is the only limit an upgrade meets: the HTTP capsule, and
``ThrottleRequests`` with it, never sees a ``websocket`` scope. These pin what a
product relies on when it lists the alias on a route — a refused or unmetered
handshake is closed with 4008 before the route runs, the bucket is one per
throttle name, client address and channel path, and the client cannot choose
its address.
"""

from __future__ import annotations

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

from cara.exceptions import ServiceUnavailableException, WebSocketException
from cara.middleware.ws import Throttle
from cara.rates import Limit, RateLimitDecision

_ws_module = sys.modules["cara.middleware.ws.Throttle"]

_ALLOWED = RateLimitDecision(
    allowed=True, limit=5, period_seconds=60, remaining=4, retry_after=0, reset_after=12
)
_REFUSED = RateLimitDecision(
    allowed=False, limit=5, period_seconds=60, remaining=0, retry_after=12, reset_after=60
)


def _throttle(name: str = "ws_connect") -> Throttle:
    """The middleware without the provider boot ``Middleware.__init__`` runs."""
    middleware = Throttle.__new__(Throttle)
    middleware.name = name
    return middleware


def _socket(*, client=("203.0.113.42", 51234), path="/ws/deals", headers=()):
    socket = MagicMock()
    socket.close = AsyncMock()
    socket.scope = {
        "type": "websocket",
        "client": client,
        "headers": list(headers),
        "path": path,
    }
    return socket


def _run(throttle: Throttle, socket, next_fn):
    return asyncio.run(throttle.handle(socket, next_fn))


@pytest.fixture
def spent(monkeypatch) -> list[Limit]:
    """Every ``Limit`` a handshake spends, each answered as allowed."""
    limits: list[Limit] = []

    def attempt(limit: Limit) -> RateLimitDecision:
        limits.append(limit)
        return _ALLOWED

    monkeypatch.setattr(_ws_module, "attempt_rate_limit", attempt)
    monkeypatch.setattr(_ws_module, "Log", MagicMock())
    monkeypatch.setattr(Throttle, "_limits", lambda self: (5, 60))
    return limits


@pytest.fixture
def only_these_proxies_are_trusted(monkeypatch) -> None:
    monkeypatch.setattr(
        _ws_module,
        "_is_trusted_proxy",
        lambda address: address in {"127.0.0.1", "10.0.0.2"},
    )


def test_an_allowed_handshake_reaches_the_route(spent) -> None:
    next_fn = AsyncMock(return_value="ok")
    socket = _socket()

    assert _run(_throttle(), socket, next_fn) == "ok"

    next_fn.assert_awaited_once_with(socket)
    socket.close.assert_not_awaited()
    assert [(limit.limit, limit.period_seconds) for limit in spent] == [(5, 60)]


def test_a_refused_handshake_is_closed_with_4008_before_the_route_runs(
    spent, monkeypatch
) -> None:
    monkeypatch.setattr(_ws_module, "attempt_rate_limit", lambda limit: _REFUSED)
    next_fn = AsyncMock()
    socket = _socket()

    with pytest.raises(WebSocketException) as refusal:
        _run(_throttle(), socket, next_fn)

    assert refusal.value.code == 4008
    socket.close.assert_awaited_once_with(code=4008)
    next_fn.assert_not_awaited()


def test_an_unmetered_handshake_is_closed_rather_than_let_through(
    spent, monkeypatch
) -> None:
    def backend_down(limit: Limit) -> RateLimitDecision:
        raise ServiceUnavailableException("Rate limiter temporarily unavailable")

    monkeypatch.setattr(_ws_module, "attempt_rate_limit", backend_down)
    next_fn = AsyncMock()
    socket = _socket()

    with pytest.raises(WebSocketException) as refusal:
        _run(_throttle(), socket, next_fn)

    assert refusal.value.code == 4008
    socket.close.assert_awaited_once_with(code=4008)
    next_fn.assert_not_awaited()


def test_each_throttle_name_address_and_channel_is_its_own_bucket(spent) -> None:
    next_fn = AsyncMock(return_value="ok")
    client = ("198.51.100.7", 1234)

    _run(_throttle(), _socket(client=client, path="/ws/deals"), next_fn)
    _run(_throttle(), _socket(client=client, path="/ws/live/products"), next_fn)
    _run(_throttle("ws_notify"), _socket(client=client, path="/ws/deals"), next_fn)

    assert [limit.key for limit in spent] == [
        "ws:ws_connect:198.51.100.7:/ws/deals",
        "ws:ws_connect:198.51.100.7:/ws/live/products",
        "ws:ws_notify:198.51.100.7:/ws/deals",
    ]


def test_an_untrusted_peer_cannot_choose_its_bucket(
    only_these_proxies_are_trusted,
) -> None:
    socket = _socket(headers=[(b"x-forwarded-for", b"8.8.8.8")])

    assert Throttle._client_ip(socket) == "203.0.113.42"


def test_a_trusted_proxy_hands_over_the_closest_untrusted_hop(
    only_these_proxies_are_trusted,
) -> None:
    socket = _socket(
        client=("127.0.0.1", 51234),
        headers=[(b"x-forwarded-for", b"1.2.3.4, 8.8.8.8, 10.0.0.2")],
    )

    assert Throttle._client_ip(socket) == "8.8.8.8"


def test_without_a_forwarded_header_the_peer_is_the_client(
    only_these_proxies_are_trusted,
) -> None:
    assert Throttle._client_ip(_socket(client=("127.0.0.1", 1))) == "127.0.0.1"


def test_a_scope_without_a_client_shares_one_named_bucket(
    only_these_proxies_are_trusted,
) -> None:
    assert Throttle._client_ip(_socket(client=None)) == "unknown"
