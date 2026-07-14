from __future__ import annotations

import importlib

import jwt
import pytest

from cara.authentication.contracts import Authenticatable
from cara.exceptions import AuthenticationConfigurationException
from cara.testing.fakes import CacheFake


class User(Authenticatable):
    id = 7
    auth_version = 3

    def get_auth_id(self):
        return self.id

    def get_auth_version(self):
        return self.auth_version


def guard(monkeypatch):
    module = importlib.import_module("cara.authentication.guards.JWTGuard")
    cache = CacheFake()
    monkeypatch.setattr(module, "Cache", cache)

    class Users:
        @classmethod
        def find(cls, user_id):
            return User() if str(user_id) == "7" else None

    monkeypatch.setattr(module.JWTGuard, "_load_user_class", lambda *_: Users)
    return module.JWTGuard(
        application=None,
        secret="x" * 48,
        ttl=900,
        refresh_ttl=259_200,
        issuer="test-api",
        audience="test-clients",
    ), cache


def test_token_pair_has_required_bound_claims(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)
    pair = jwt_guard.generate_token_pair(User())
    access = jwt.decode(
        pair["access_token"],
        jwt_guard.secret,
        algorithms=[jwt_guard.algorithm],
        issuer="test-api",
        audience="test-clients",
    )
    refresh = jwt.decode(
        pair["refresh_token"],
        jwt_guard.secret,
        algorithms=[jwt_guard.algorithm],
        issuer="test-api",
        audience="test-clients",
    )

    assert access["typ"] == "access"
    assert refresh["typ"] == "refresh"
    assert access["fid"] == refresh["fid"]
    assert access["jti"] != refresh["jti"]
    assert access["ver"] == 3


def test_refresh_reuse_revokes_entire_family(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)
    pair = jwt_guard.generate_token_pair(User())

    assert jwt_guard.consume_refresh_token(pair["refresh_token"]) is True
    assert jwt_guard.consume_refresh_token(pair["refresh_token"]) is False
    assert jwt_guard.validate_token(pair["access_token"]) is False


def test_guard_rejects_weak_secret(monkeypatch) -> None:
    module = importlib.import_module("cara.authentication.guards.JWTGuard")
    monkeypatch.setattr(module.JWTGuard, "_load_user_class", lambda *_: object)
    with pytest.raises(AuthenticationConfigurationException):
        module.JWTGuard(application=None, secret="short")


def test_websocket_ticket_is_opaque_and_single_use(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)
    access = jwt_guard.generate_token_pair(User())["access_token"]

    ticket = jwt_guard.issue_websocket_ticket(access)

    assert access not in ticket
    assert jwt_guard.consume_websocket_ticket(ticket) is not None
    assert jwt_guard.consume_websocket_ticket(ticket) is None
