"""A header-authenticated WebSocket handshake checks its credential once.

``Authenticate._authenticate_socket`` asked ``guard.validate_token(token)``
and, on yes, called ``guard._resolve_user_from_token(token)``. For the stock
``JWTGuard`` the first IS the second compared against None, so every handshake
verified the signature twice, read the three revocation keys from the cache
twice and loaded the user twice.

Also pinned: the token is read from the ``Authorization`` header alone, as
latin-1. Decoding every header as strict UTF-8 refused a connection with a
valid token because some OTHER header carried a high byte.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

from cara.authentication import JWTGuard
from cara.middleware.ws.Authenticate import Authenticate


class _CountingGuard(JWTGuard):
    """A JWTGuard whose credential resolution is counted, not executed."""

    def __init__(self, user):
        # No ``super().__init__``: the real one needs PyJWT, a secret and a
        # user model, none of which this test is about.
        self.resolutions = 0
        self._resolved = user

    def _resolve_user_from_token(self, token):
        self.resolutions += 1
        return self._resolved if token == "good" else None


class _StricterGuard(_CountingGuard):
    """Overrides ``validate_token`` — its extra check must keep running."""

    def validate_token(self, token):
        return False


def _middleware(guard) -> Authenticate:
    middleware = Authenticate.__new__(Authenticate)
    middleware.guards = ["jwt"]
    middleware.application = MagicMock()
    middleware.application.make.return_value.guard.return_value = guard
    return middleware


def _socket(*headers: tuple[bytes, bytes]):
    socket = MagicMock()
    socket.scope = {
        "type": "websocket",
        "headers": list(headers),
        "query_string": b"",
        "subprotocols": [],
    }
    return socket


def test_valid_token_resolves_the_user_exactly_once():
    user = object()
    guard = _CountingGuard(user)
    socket = _socket((b"authorization", b"Bearer good"))

    assert asyncio.run(_middleware(guard)._authenticate_socket(socket)) is True

    assert guard.resolutions == 1  # was 2
    assert socket._user is user


def test_rejected_token_is_checked_once_and_refused():
    guard = _CountingGuard(object())
    socket = _socket((b"authorization", b"Bearer bad"))

    assert asyncio.run(_middleware(guard)._authenticate_socket(socket)) is False
    assert guard.resolutions == 1


def test_guard_with_its_own_validate_token_is_still_asked_first():
    guard = _StricterGuard(object())
    socket = _socket((b"authorization", b"Bearer good"))

    assert asyncio.run(_middleware(guard)._authenticate_socket(socket)) is False
    assert guard.resolutions == 0


def test_high_byte_in_another_header_does_not_hide_the_token():
    middleware = _middleware(_CountingGuard(object()))
    socket = _socket(
        (b"user-agent", b"Caf\xe9Client/1.0"),
        (b"authorization", b"Bearer good"),
    )

    assert middleware._extract_token(socket) == "good"


def test_token_still_falls_back_to_the_subprotocol():
    middleware = _middleware(_CountingGuard(object()))
    socket = _socket((b"user-agent", b"plain"))
    socket.scope["subprotocols"] = ["bearer good"]

    assert middleware._extract_token(socket) == "good"
