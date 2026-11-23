"""The single-check fast path is taken only for the STOCK ``validate_token``.

``Authenticate._user_for_token`` skips ``guard.validate_token`` when it is the
stock ``JWTGuard`` one (that check IS the user resolution compared against
None). The decision has to be made on the callable the instance would really
run: a ``validate_token`` set on the instance, or patched onto the class after
import, may refuse what the resolver accepts, and skipping it opened a socket
the guard meant to refuse.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

from cara.authentication import JWTGuard
from cara.middleware.ws.Authenticate import Authenticate


class _Guard(JWTGuard):
    """A JWTGuard whose user resolution is counted, not executed."""

    def __init__(self):
        # No ``super().__init__``: PyJWT, a secret and a user model are not
        # what this test is about.
        self.resolutions = 0
        self.user = object()

    def _resolve_user_from_token(self, token):
        self.resolutions += 1
        return self.user


def _authenticate(guard) -> tuple[bool, MagicMock]:
    middleware = Authenticate.__new__(Authenticate)
    middleware.guards = ["jwt"]
    middleware.application = MagicMock()
    middleware.application.make.return_value.guard.return_value = guard
    socket = MagicMock()
    socket.scope = {
        "type": "websocket",
        "headers": [(b"authorization", b"Bearer good")],
        "query_string": b"",
        "subprotocols": [],
    }
    return asyncio.run(middleware._authenticate_socket(socket)), socket


def test_an_instance_level_validate_token_is_called_and_honoured():
    guard = _Guard()
    asked = []
    guard.validate_token = lambda token: asked.append(token) or False

    accepted, _ = _authenticate(guard)

    assert accepted is False
    assert asked == ["good"]
    assert guard.resolutions == 0


def test_a_class_patched_validate_token_is_called_and_honoured(monkeypatch):
    asked = []
    monkeypatch.setattr(
        _Guard, "validate_token", lambda self, token: asked.append(token) or False
    )

    accepted, _ = _authenticate(_Guard())

    assert accepted is False
    assert asked == ["good"]


def test_a_replaced_check_that_says_yes_still_resolves_the_user():
    guard = _Guard()
    guard.validate_token = lambda token: True

    accepted, socket = _authenticate(guard)

    assert accepted is True
    assert socket._user is guard.user
    assert guard.resolutions == 1


def test_the_stock_check_takes_the_single_resolution_path():
    guard = _Guard()

    accepted, socket = _authenticate(guard)

    assert accepted is True
    assert socket._user is guard.user
    assert guard.resolutions == 1
