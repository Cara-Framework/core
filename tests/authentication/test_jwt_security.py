from __future__ import annotations

import importlib

import jwt
import pytest

from cara.authentication.contracts import Authenticatable
from cara.exceptions import (
    AuthenticationConfigurationException,
    ServiceUnavailableException,
)
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


@pytest.mark.parametrize(
    ("claim", "value"),
    [
        ("typ", "admin"),
        ("jti", "short"),
        ("fid", "short"),
        ("iat", "1"),
        ("ver", True),
    ],
)
def test_signed_but_malformed_claims_are_rejected(
    monkeypatch, claim: str, value: object
) -> None:
    jwt_guard, _ = guard(monkeypatch)
    token = jwt_guard.generate_access_token(User())
    payload = jwt.decode(
        token,
        jwt_guard.secret,
        algorithms=[jwt_guard.algorithm],
        issuer="test-api",
        audience="test-clients",
    )
    payload[claim] = value
    malformed = jwt.encode(payload, jwt_guard.secret, algorithm=jwt_guard.algorithm)

    assert jwt_guard.validate_token(malformed) is False


def test_token_minting_rejects_protected_extra_claims(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)

    with pytest.raises(ValueError, match="protected claims"):
        jwt_guard.generate_access_token(User(), extra_claims={"sub": "99"})


@pytest.mark.parametrize("ttl", [True, 0, "900", 259_201])
def test_custom_token_ttl_is_exact_and_bounded(monkeypatch, ttl: object) -> None:
    jwt_guard, _ = guard(monkeypatch)

    with pytest.raises(ValueError, match="TTL"):
        jwt_guard.generate_token_with_ttl(User(), ttl)  # type: ignore[arg-type]


def test_token_issuance_requires_explicit_persisted_auth_version(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)

    class MissingVersionUser(Authenticatable):
        id = 7

        def get_auth_id(self):
            return self.id

    with pytest.raises(AuthenticationConfigurationException, match="auth version"):
        jwt_guard.generate_access_token(MissingVersionUser())


@pytest.mark.parametrize("identifier", [None, "", 0, True, object()])
def test_token_issuance_requires_a_stable_scalar_subject(
    monkeypatch, identifier: object
) -> None:
    jwt_guard, _ = guard(monkeypatch)

    class InvalidIdentifierUser(Authenticatable):
        def get_auth_id(self):
            return identifier

        def get_auth_version(self):
            return 1

    with pytest.raises(AuthenticationConfigurationException, match="auth id"):
        jwt_guard.generate_access_token(InvalidIdentifierUser())


def test_token_rejects_user_whose_auth_version_has_advanced(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)
    token = jwt_guard.generate_access_token(User())
    monkeypatch.setattr(User, "auth_version", 4)

    assert jwt_guard.validate_token(token) is False


def test_revocation_authority_outage_is_not_reported_as_bad_credentials(
    monkeypatch,
) -> None:
    jwt_guard, cache = guard(monkeypatch)
    token = jwt_guard.generate_access_token(User())
    monkeypatch.setattr(
        cache,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ConnectionError("down")),
    )

    with pytest.raises(ServiceUnavailableException):
        jwt_guard.validate_token(token)


def test_identity_store_outage_is_retryable(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)
    token = jwt_guard.generate_access_token(User())
    monkeypatch.setattr(
        jwt_guard._user_class,
        "authenticate_jwt",
        lambda *_args: (_ for _ in ()).throw(ConnectionError("down")),
    )

    with pytest.raises(ServiceUnavailableException):
        jwt_guard.validate_token(token)


def test_refresh_reuse_revokes_entire_family(monkeypatch) -> None:
    jwt_guard, cache = guard(monkeypatch)
    pair = jwt_guard.generate_token_pair(User())
    claims = jwt.decode(
        pair["refresh_token"],
        jwt_guard.secret,
        algorithms=[jwt_guard.algorithm],
        issuer="test-api",
        audience="test-clients",
    )

    assert jwt_guard.consume_refresh_token(pair["refresh_token"]) is True
    assert jwt_guard.consume_refresh_token(pair["refresh_token"]) is False
    assert jwt_guard.validate_token(pair["access_token"]) is False
    assert cache.ttl_of(f"jwt_family_revoke:{claims['fid']}") >= (
        jwt_guard.refresh_ttl - 1
    )


def test_controller_refresh_path_detects_reuse(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)
    pair = jwt_guard.generate_token_pair(User())

    assert jwt_guard.consume_refresh_token_user(pair["refresh_token"]) is not None
    assert jwt_guard.consume_refresh_token_user(pair["refresh_token"]) is None
    assert jwt_guard.validate_token(pair["access_token"]) is False


def test_refresh_consumption_fails_closed_without_blacklist(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)
    pair = jwt_guard.generate_token_pair(User())
    jwt_guard.blacklist_enabled = False

    assert jwt_guard.consume_refresh_token(pair["refresh_token"]) is False


def test_guard_rejects_weak_secret(monkeypatch) -> None:
    module = importlib.import_module("cara.authentication.guards.JWTGuard")
    monkeypatch.setattr(module.JWTGuard, "_load_user_class", lambda *_: object)
    with pytest.raises(AuthenticationConfigurationException):
        module.JWTGuard(application=None, secret="short")


def test_guard_rejects_excessive_refresh_lifetime(monkeypatch) -> None:
    module = importlib.import_module("cara.authentication.guards.JWTGuard")
    monkeypatch.setattr(module.JWTGuard, "_load_user_class", lambda *_: object)
    with pytest.raises(AuthenticationConfigurationException, match="30 days"):
        module.JWTGuard(
            application=None,
            secret="x" * 48,
            ttl=900,
            refresh_ttl=31 * 24 * 60 * 60,
        )


def test_guard_rejects_user_model_without_canonical_resolver(monkeypatch) -> None:
    module = importlib.import_module("cara.authentication.guards.JWTGuard")
    monkeypatch.setattr(module.JWTGuard, "_load_user_class", lambda *_: object)

    with pytest.raises(AuthenticationConfigurationException, match="authenticate_jwt"):
        module.JWTGuard(
            application=None,
            secret="x" * 48,
            ttl=900,
            refresh_ttl=259_200,
        )


def test_websocket_ticket_is_opaque_and_single_use(monkeypatch) -> None:
    jwt_guard, _ = guard(monkeypatch)
    access = jwt_guard.generate_token_pair(User())["access_token"]

    ticket = jwt_guard.issue_websocket_ticket(access)

    assert access not in ticket
    assert jwt_guard.consume_websocket_ticket(ticket) is not None
    assert jwt_guard.consume_websocket_ticket(ticket) is None
