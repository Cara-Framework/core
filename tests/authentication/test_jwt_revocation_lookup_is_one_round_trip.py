"""Every revocation entry of a bearer token is read in ONE cache round trip.

``_decode_token`` asked the cache three sequential strict GETs — the per-token
blacklist, the family tombstone and the user-wide cutoff — three network waits
on every authenticated request. It now reads all three with one
``Cache.many([...], strict=True)``; the decisions are the same, and a cache
outage still maps to the same ``ServiceUnavailableException``.
"""

from __future__ import annotations

import importlib

import pytest

from cara.authentication.contracts import Authenticatable
from cara.exceptions import ServiceUnavailableException, TokenBlacklistedException
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


class _CountingCache(CacheFake):
    """CacheFake that records every read and can fail every read."""

    def __init__(self):
        super().__init__()
        self.reads: list[tuple[str, tuple, bool]] = []
        self.down = False

    def get(self, key, default=None, *, strict=True):
        self.reads.append(("get", (key,), strict))
        if self.down:
            raise ConnectionError("redis is down")
        return super().get(key, default, strict=strict)

    def many(self, keys, default=None, driver_name=None, *, strict=True):
        keys = list(keys)
        self.reads.append(("many", tuple(keys), strict))
        if self.down:
            raise ConnectionError("redis is down")
        return {key: CacheFake.get(self, key, default) for key in keys}


@pytest.fixture
def subject(monkeypatch):
    module = importlib.import_module("cara.authentication.guards.JWTGuard")
    lifecycle = importlib.import_module("cara.authentication.guards._JWTTokenLifecycle")
    cache = _CountingCache()
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
    token = jwt_guard.generate_access_token(User())
    claims = jwt_guard._decode_signed_token(token)
    return jwt_guard, lifecycle, cache, token, claims


def _keys(lifecycle, claims):
    return (
        lifecycle._blacklist_key(claims),
        f"jwt_family_revoke:{claims['fid']}",
        f"jwt_user_revoke:{claims['sub']}",
    )


def test_a_live_token_costs_one_strict_round_trip(subject):
    jwt_guard, lifecycle, cache, token, claims = subject

    assert jwt_guard._decode_token(token)["jti"] == claims["jti"]

    assert cache.reads == [("many", _keys(lifecycle, claims), True)]


def test_skipping_the_blacklist_drops_only_the_blacklist_key(subject):
    jwt_guard, lifecycle, cache, token, claims = subject
    cache.put(lifecycle._blacklist_key(claims), True)

    jwt_guard._decode_token(token, check_token_blacklist=False)

    assert cache.reads == [("many", _keys(lifecycle, claims)[1:], True)]


@pytest.mark.parametrize(
    "revoke, reason",
    [
        (lambda g, token, claims: g._blacklist_token(token), "blacklisted"),
        (lambda g, token, claims: g.revoke_token_family(claims["fid"]), "family"),
        (lambda g, token, claims: g.revoke_user_sessions(claims["sub"]), "cutoff"),
    ],
    ids=["blacklist", "family", "user-cutoff"],
)
def test_each_revocation_is_still_decided(subject, revoke, reason):
    jwt_guard, _, cache, token, claims = subject
    revoke(jwt_guard, token, claims)
    cache.reads.clear()

    with pytest.raises(TokenBlacklistedException, match=reason):
        jwt_guard._decode_token(token)
    assert len(cache.reads) == 1


def test_a_cache_outage_is_a_service_outage_not_a_verdict(subject):
    jwt_guard, _, cache, token, _ = subject
    cache.down = True

    with pytest.raises(ServiceUnavailableException):
        jwt_guard._decode_token(token)
    assert len(cache.reads) == 1
