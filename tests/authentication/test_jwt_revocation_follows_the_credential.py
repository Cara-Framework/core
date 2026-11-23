"""A revoked JWT stays revoked under every spelling that still verifies.

The per-token blacklist and the refresh one-time claim used to be keyed on a
hash of the token TEXT. A JWS signature segment has spare spellings — base64url
padding (``<token>=``) and non-canonical trailing bits — that decode to the
same bytes and verify as the same credential, so a logged-out access token was
accepted again with one ``=`` appended, and a rotated refresh token could be
redeemed a second time without tripping reuse detection. The key is now the
verified ``jti`` claim.
"""

from __future__ import annotations

import importlib
import string

import pytest

from cara.authentication.contracts import Authenticatable
from cara.testing.fakes import CacheFake

_BASE64URL = string.ascii_uppercase + string.ascii_lowercase + string.digits + "-_"


class User(Authenticatable):
    id = 7
    auth_version = 3

    def get_auth_id(self):
        return self.id

    def get_auth_version(self):
        return self.auth_version


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
    ), lifecycle


def other_spellings(jwt_guard, token: str) -> list[str]:
    """Every re-spelling of ``token`` the signature check still accepts."""
    last = _BASE64URL.index(token[-1])
    # The final signature character carries unused low bits: characters that
    # differ only there decode to the same signature bytes.
    candidates = [token + "="] + [
        token[:-1] + _BASE64URL[(last & ~3) | bits]
        for bits in range(4)
        if (last & ~3) | bits != last
    ]
    accepted = []
    for candidate in candidates:
        try:
            jwt_guard._decode_signed_token(candidate)
        except jwt_guard._jwt.InvalidTokenError:
            continue
        accepted.append(candidate)
    return accepted


def test_the_blacklist_entry_is_keyed_on_the_verified_jti(monkeypatch) -> None:
    jwt_guard, lifecycle = guard(monkeypatch)
    token = jwt_guard.generate_access_token(User())
    claims = jwt_guard._decode_signed_token(token)

    jwt_guard._blacklist_token(token)

    assert jwt_guard._is_blacklisted({"jti": claims["jti"]}) is True
    assert lifecycle._blacklist_key(claims) == lifecycle._blacklist_key(
        {"jti": claims["jti"]}
    )
    # Nothing derived from the token text is consulted any more.
    assert jwt_guard._is_blacklisted({"jti": "another-credential-entirely"}) is False


def test_a_logged_out_token_is_refused_under_every_spelling(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)
    token = jwt_guard.generate_access_token(User())
    spellings = other_spellings(jwt_guard, token)
    assert jwt_guard.validate_token(token) is True
    for spelling in spellings:
        assert jwt_guard.validate_token(spelling) is True

    jwt_guard._blacklist_token(token)

    assert jwt_guard.validate_token(token) is False
    for spelling in spellings:
        assert jwt_guard.validate_token(spelling) is False, spelling[-4:]


def test_blacklisting_one_spelling_revokes_the_credential(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)
    token = jwt_guard.generate_access_token(User())
    spellings = other_spellings(jwt_guard, token)
    if not spellings:
        pytest.skip("this PyJWT accepts exactly one spelling of a token")

    jwt_guard._blacklist_token(spellings[0])

    assert jwt_guard.validate_token(token) is False


def test_a_burned_refresh_token_cannot_be_redeemed_under_another_spelling(
    monkeypatch,
) -> None:
    jwt_guard, _ = guard(monkeypatch)
    refresh = jwt_guard.generate_token_pair(User())["refresh_token"]
    family = jwt_guard._decode_signed_token(refresh)["fid"]
    spellings = other_spellings(jwt_guard, refresh)

    assert jwt_guard.consume_refresh_token_user(refresh) is not None
    assert jwt_guard._is_family_revoked(family) is False

    for spelling in spellings:
        assert jwt_guard.consume_refresh_token_user(spelling) is None, spelling[-4:]
    if spellings:
        # A second use of one credential is the leaked-token signal, however
        # it is spelled: the whole login family goes.
        assert jwt_guard._is_family_revoked(family) is True


def test_this_pyjwt_accepts_the_padded_spelling(monkeypatch) -> None:
    """Pins the premise: if this ever fails, the padded case above is vacuous."""
    jwt_guard, _ = guard(monkeypatch)
    token = jwt_guard.generate_access_token(User())

    assert token + "=" in other_spellings(jwt_guard, token)
