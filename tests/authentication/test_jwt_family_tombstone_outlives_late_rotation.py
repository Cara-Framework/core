"""A revoked token family stays revoked for longer than any refresh token it covers.

The tombstone lived exactly ``refresh_ttl`` from the revocation. A rotation that
claimed its refresh token just before the revocation mints its pair just after
it, and that pair expires ``refresh_ttl`` from the LATER mint: the tombstone
ended first and the pair of a revoked family was honoured again.
"""

from __future__ import annotations

import importlib

from cara.authentication.contracts import Authenticatable
from cara.testing.fakes import CacheFake

_REFRESH_TTL = 259_200


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
    jwt_guard = module.JWTGuard(
        application=None,
        secret="x" * 48,
        ttl=900,
        refresh_ttl=_REFRESH_TTL,
        issuer="test-api",
        audience="test-clients",
    )
    return jwt_guard, cache


def test_the_tombstone_outlives_a_pair_minted_after_the_revocation(monkeypatch):
    jwt_guard, cache = guard(monkeypatch)
    family = jwt_guard._decode_signed_token(jwt_guard.generate_refresh_token(User()))[
        "fid"
    ]

    jwt_guard.revoke_token_family(family)

    tombstone = cache.ttl_of(f"jwt_family_revoke:{family}")
    # A rotation stalled for minutes between its claim and its mint still
    # lands inside the tombstone (was exactly ``refresh_ttl``).
    assert tombstone > _REFRESH_TTL
    assert tombstone - _REFRESH_TTL >= 60


def test_a_descendant_of_a_revoked_family_is_refused(monkeypatch):
    jwt_guard, _ = guard(monkeypatch)
    family = jwt_guard._decode_signed_token(jwt_guard.generate_refresh_token(User()))[
        "fid"
    ]
    jwt_guard.revoke_token_family(family)

    descendant = jwt_guard.generate_refresh_token(User(), family_id=family)

    assert jwt_guard.consume_refresh_token_user(descendant) is None
