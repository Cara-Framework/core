"""A WebSocket ticket asks the same revocation question as the bearer token it
was minted from.

``consume_websocket_ticket`` checked the token FAMILY only. A ticket minted
before its access token was logged out (the per-token blacklist), or before a
``revoke_user_sessions`` ("log out everywhere" after a password or role change),
still opened a socket for its whole 30 seconds — and the socket stayed open.
"""

from __future__ import annotations

import importlib

from cara.authentication.contracts import Authenticatable
from cara.testing.fakes import CacheFake


class User(Authenticatable):
    id = 7

    def get_auth_id(self):
        return self.id

    def get_auth_version(self):
        return 3


def guard(monkeypatch):
    module = importlib.import_module("cara.authentication.guards.JWTGuard")
    lifecycle = importlib.import_module("cara.authentication.guards._JWTTokenLifecycle")
    cache = CacheFake()
    monkeypatch.setattr(module, "Cache", cache)
    monkeypatch.setattr(lifecycle, "Cache", cache)

    class Users:
        @classmethod
        def authenticate_jwt(cls, user_id, _claims):
            return User() if str(user_id) == "7" else None

    monkeypatch.setattr(module.JWTGuard, "_load_user_class", lambda *_: Users)
    return module.JWTGuard(
        application=None,
        secret="x" * 48,
        ttl=900,
        refresh_ttl=259_200,
        issuer="test-api",
        audience="test-clients",
    )


def test_a_ticket_dies_with_its_logged_out_access_token(monkeypatch):
    jwt_guard = guard(monkeypatch)
    access = jwt_guard.generate_access_token(User())
    ticket = jwt_guard.issue_websocket_ticket(access)

    jwt_guard._blacklist_token(access)  # what logout() does

    assert jwt_guard.validate_token(access) is False
    assert jwt_guard.consume_websocket_ticket(ticket) is None


def test_a_ticket_dies_with_a_user_wide_revocation(monkeypatch):
    jwt_guard = guard(monkeypatch)
    access = jwt_guard.generate_access_token(User())
    ticket = jwt_guard.issue_websocket_ticket(access)

    jwt_guard.revoke_user_sessions(7)

    assert jwt_guard.validate_token(access) is False
    assert jwt_guard.consume_websocket_ticket(ticket) is None


def test_a_ticket_dies_with_its_revoked_family(monkeypatch):
    jwt_guard = guard(monkeypatch)
    access = jwt_guard.generate_access_token(User())
    ticket = jwt_guard.issue_websocket_ticket(access)

    jwt_guard.revoke_token_family(jwt_guard._decode_signed_token(access)["fid"])

    assert jwt_guard.consume_websocket_ticket(ticket) is None


def test_an_unrevoked_ticket_opens_exactly_one_socket(monkeypatch):
    jwt_guard = guard(monkeypatch)
    ticket = jwt_guard.issue_websocket_ticket(jwt_guard.generate_access_token(User()))

    user = jwt_guard.consume_websocket_ticket(ticket)

    assert isinstance(user, User)
    assert jwt_guard.consume_websocket_ticket(ticket) is None
