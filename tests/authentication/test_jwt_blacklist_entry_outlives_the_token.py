"""A logged-out access token's blacklist entry lives at least as long as the token.

``_blacklist_token`` computed the entry's TTL as ``int(exp - now)``: the fraction
was cut off, so the entry expired up to a second BEFORE the token did and a
logged-out token was accepted again for the last moment of its life. It is now
rounded up.
"""

from __future__ import annotations

import importlib
import types

import pytest

from cara.authentication.contracts import Authenticatable
from cara.testing.fakes import CacheFake


class User(Authenticatable):
    id = 7

    def get_auth_id(self):
        return self.id

    def get_auth_version(self):
        return 3


class _Users:
    @classmethod
    def authenticate_jwt(cls, user_id, _claims):
        return User() if str(user_id) == "7" else None


@pytest.fixture
def subject(monkeypatch):
    module = importlib.import_module("cara.authentication.guards.JWTGuard")
    lifecycle = importlib.import_module("cara.authentication.guards._JWTTokenLifecycle")
    cache = CacheFake()
    monkeypatch.setattr(module, "Cache", cache)
    monkeypatch.setattr(lifecycle, "Cache", cache)
    monkeypatch.setattr(module.JWTGuard, "_load_user_class", lambda *_: _Users)
    jwt_guard = module.JWTGuard(
        application=None,
        secret="x" * 48,
        ttl=900,
        refresh_ttl=259_200,
        issuer="test-api",
        audience="test-clients",
    )
    return jwt_guard, lifecycle, cache


@pytest.mark.parametrize("remaining", [899.25, 899.75, 0.4])
def test_the_entry_does_not_expire_before_the_token(subject, monkeypatch, remaining):
    jwt_guard, lifecycle, cache = subject
    token = jwt_guard.generate_access_token(User())
    claims = jwt_guard._decode_signed_token(token)
    # The logout happens with ``remaining`` seconds of the token's life left.
    monkeypatch.setattr(
        lifecycle, "time", types.SimpleNamespace(time=lambda: claims["exp"] - remaining)
    )

    jwt_guard._blacklist_token(token)

    entry_ttl = cache.ttl_of(lifecycle._blacklist_key(claims))
    assert entry_ttl is not None
    assert entry_ttl >= remaining  # int() gave 899 / 899 / 0 (no entry at all)
