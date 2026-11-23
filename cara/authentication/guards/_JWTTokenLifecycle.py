"""JWT signature, claim, revocation and token-minting lifecycle."""

from __future__ import annotations

import hashlib
import logging
import math
import secrets
import time
from typing import Any

from cara.authentication.contracts import Authenticatable
from cara.exceptions import (
    ServiceUnavailableException,
    TokenBlacklistedException,
    TokenExpiredException,
    TokenInvalidException,
)
from cara.facades import Cache, Log

_logger = logging.getLogger("cara.auth.jwt")
_TOKEN_TYPE_ACCESS = "access"
_TOKEN_TYPE_REFRESH = "refresh"
_REQUIRED_CLAIMS = ("sub", "iat", "exp", "typ", "jti", "fid", "iss", "aud", "ver")
#: How long a family tombstone outlives the tokens it covers (see
#: ``_revoke_token_family``): longer than any request stays in flight.
_FAMILY_TOMBSTONE_MARGIN_SECONDS = 300


def _security_state_unavailable(operation: str, exc: Exception) -> Exception:
    Log.error(
        "JWT %s failed because the security-state authority is unavailable: %s: %s",
        operation,
        type(exc).__name__,
        exc,
        category="cara.auth.jwt",
        exc_info=True,
    )
    return ServiceUnavailableException(
        "Sign-in isn't working right now. Try again in a minute.",
        retry_after=5,
    )


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _blacklist_key(payload: dict[str, Any]) -> str:
    """Cache key of one credential's revocation entry.

    Keyed on the VERIFIED ``jti`` claim, never on the token text, because the
    text is malleable: a JWS signature segment has spare spellings (base64url
    padding — ``<token>=`` — and non-canonical trailing bits decode to the same
    bytes), and every one of them verifies. A key derived from the spelling
    let a logged-out token back in with one ``=`` appended, and let a burned
    refresh token win the one-time claim a second time without tripping reuse
    detection. The claim is covered by the signature, so every spelling of one
    credential lands on one key. Hashed so the key stays fixed-width whatever
    a signer put in the claim.
    """
    return f"jwt_blacklist:{_hash_token(str(payload['jti']))}"


def _decode_token(
    self,
    token: str,
    verify_exp: bool = True,
    *,
    check_token_blacklist: bool = True,
) -> dict[str, Any]:
    """
    Decode and validate JWT token.

    Args:
        token: JWT token string
        verify_exp: Whether to verify expiration

    Returns:
        Dict containing token payload

    Raises:
        TokenBlacklistedException: If token is blacklisted
        TokenExpiredException: If token is expired
        TokenInvalidException: If token is invalid
    """
    try:
        payload = self._decode_signed_token(token, verify_exp=verify_exp)
    except self._jwt.ExpiredSignatureError:
        raise TokenExpiredException("Token expired")
    except self._jwt.InvalidTokenError:
        raise TokenInvalidException("Invalid token")
    _validate_claims(payload)
    # Verify the signature and mandatory registered claims BEFORE touching
    # cache. Invalid attacker-controlled strings must not become Redis I/O.
    self._assert_not_revoked(payload, check_token_blacklist=check_token_blacklist)
    return payload


def _assert_not_revoked(
    self,
    payload: dict[str, Any],
    *,
    check_token_blacklist: bool = True,
) -> None:
    """Raise ``TokenBlacklistedException`` when these VERIFIED claims are revoked.

    The one revocation question, asked wherever a credential is honoured: a
    bearer token (``_decode_token``) and the WebSocket ticket minted from one
    (``consume_websocket_ticket``). The ticket path used to ask only about the
    family, so a ticket outlived a logout of its access token and a "log out
    everywhere" (password change, role change) for its whole 30 seconds, and
    the socket it opened stayed open after that.

    Every revocation entry of this credential is read in ONE round trip.
    """
    # (``Cache.many``, a Redis MGET): three sequential strict GETs were three
    # network waits on every authenticated request. The decisions below are
    # taken in the same order as before, on the same values: an absent key
    # reads as ``None``, which every check treats as "nothing recorded".
    #
    # Per-user revocation cutoff. After a security-sensitive change
    # (password reset, email change, "log out all sessions"), the
    # caller bumps ``jwt_user_revoke:{sub}`` to ``now``. Any token
    # with ``iat`` at or before that cutoff is treated as
    # revoked even though its signature is still valid. This is the
    # missing primitive that lets ``change_email`` actually expire
    # outstanding sessions instead of leaving stolen tokens live
    # for the full refresh-TTL window.
    #
    # Fail-closed contract on cache backend errors (round 30):
    #   * ``Cache.get`` returning ``None`` / ``0`` is the legitimate
    #     "no revocation event recorded for this user" branch — fall
    #     through normally and accept the token.
    #   * A cache backend EXCEPTION (Redis down, connection reset,
    #     serialization error) MUST NOT silently bypass the check.
    #     Pre-fix the bare ``except Exception: pass`` swallowed
    #     these and let a revoked JWT keep authenticating during a
    #     Redis outage — exactly the wrong direction for a
    #     security/availability trade-off. A user who is
    #     accidentally locked out can recover by re-logging in;
    #     a leaked token that keeps working has no recovery path
    #     because the legitimate owner doesn't know the token was
    #     ever stolen.
    sub = payload.get("sub")
    iat = payload.get("iat")
    check_blacklist = check_token_blacklist and self.blacklist_enabled
    blacklist_key = _blacklist_key(payload) if check_blacklist else None
    family_key = f"jwt_family_revoke:{payload['fid']}"
    cutoff_key = f"jwt_user_revoke:{sub}" if sub and iat is not None else None
    keys = [key for key in (blacklist_key, family_key, cutoff_key) if key]
    try:
        state = Cache.many(keys, None, strict=True)
    except ServiceUnavailableException:
        raise
    except Exception as exc:
        raise _security_state_unavailable("revocation lookup", exc) from exc

    if blacklist_key and state.get(blacklist_key):
        raise TokenBlacklistedException("Token has been blacklisted")
    if state.get(family_key):
        raise TokenBlacklistedException("Token family has been revoked")
    if cutoff_key:
        # ``None`` / ``0`` is the legitimate "no revocation event recorded"
        # branch — fall through and accept the token. Only a positive cutoff
        # whose value equals or exceeds the token's ``iat`` rejects.
        cutoff = state.get(cutoff_key)
        if cutoff and float(iat) <= float(cutoff):
            raise TokenBlacklistedException(
                "Token revoked: issued before user-level revocation cutoff"
            )


def _validate_claims(payload: dict[str, Any]) -> None:
    """Reject signed credentials whose claim types are ambiguous or corrupt."""
    for name in ("sub", "typ", "jti", "fid", "iss", "aud"):
        value = payload.get(name)
        if not isinstance(value, str) or not value.strip():
            raise TokenInvalidException(f"JWT claim {name!r} must be non-empty text")
    if payload["typ"] not in {_TOKEN_TYPE_ACCESS, _TOKEN_TYPE_REFRESH}:
        raise TokenInvalidException("JWT token type is invalid")
    if len(payload["jti"]) < 16 or len(payload["fid"]) < 16:
        raise TokenInvalidException("JWT credential identifiers are too short")
    for name in ("iat", "exp"):
        value = payload.get(name)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise TokenInvalidException(f"JWT claim {name!r} must be a finite number")
    if payload["exp"] <= payload["iat"]:
        raise TokenInvalidException("JWT expiry must follow issuance")
    version = payload.get("ver")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise TokenInvalidException("JWT auth version must be a positive integer")


def _revoke_user_sessions(self, user_id: Any, ttl: int | None = None) -> None:
    """Revoke every JWT issued before now for the given user.

    Sets a per-user ``iat`` cutoff in cache so any token (access or
    refresh) issued before this call is rejected by ``_decode_token``.
    TTL defaults to ``refresh_ttl`` because once the longest-lived
    token type expires naturally, the cutoff is no longer needed.
    """
    cache_ttl = ttl if ttl is not None else max(self.refresh_ttl, self.ttl)
    Cache.put(
        f"jwt_user_revoke:{user_id}",
        time.time(),
        cache_ttl,
        strict=True,
    )


def _revoke_token_family(self, family_id: str, ttl: int | None = None) -> None:
    """Revoke one login/refresh family without signing out other devices."""
    if not family_id:
        raise ValueError("family_id is required")
    # A rotation that claimed its refresh token just BEFORE this revocation
    # mints its pair just AFTER it, and that pair expires ``refresh_ttl`` from
    # the later mint. A tombstone of exactly ``refresh_ttl`` ended first, and
    # the pair of a revoked family was honoured again; the margin outlasts any
    # request that could still be in flight.
    cache_ttl = (
        ttl if ttl is not None else self.refresh_ttl
    ) + _FAMILY_TOMBSTONE_MARGIN_SECONDS
    Cache.put(
        f"jwt_family_revoke:{family_id}",
        True,
        cache_ttl,
        strict=True,
    )


def _is_family_revoked(self, family_id: str) -> bool:
    return bool(
        Cache.get(
            f"jwt_family_revoke:{family_id}",
            False,
            strict=True,
        )
    )


def _decode_signed_token(self, token: str, *, verify_exp: bool = True) -> dict[str, Any]:
    return self._jwt.decode(
        token,
        self.secret,
        algorithms=[self.algorithm],
        audience=self.audience,
        issuer=self.issuer,
        options={
            "verify_exp": verify_exp,
            "require": list(_REQUIRED_CLAIMS),
        },
    )


def _generate_token_with_ttl(
    self,
    user: Authenticatable,
    ttl: int,
    token_type: str = _TOKEN_TYPE_ACCESS,
    extra_claims: dict | None = None,
    *,
    family_id: str | None = None,
) -> str:
    """Generate JWT token for user with custom TTL and type.

    The `token_type` becomes the `typ` claim — used by refresh() and
    validate_refresh_token() to ensure access tokens can't be swapped
    in for refresh tokens or vice versa.

    Extra claims may add application context but may not restate credential
    identity, lifetime, issuer, audience, type, or revocation fields.
    """
    if (
        isinstance(ttl, bool)
        or not isinstance(ttl, int)
        or not 0 < ttl <= self.refresh_ttl
    ):
        raise ValueError("JWT TTL must be a positive integer within refresh_ttl")
    if token_type not in {_TOKEN_TYPE_ACCESS, _TOKEN_TYPE_REFRESH}:
        raise ValueError("JWT token_type must be access or refresh")
    if family_id is not None and (
        not isinstance(family_id, str) or len(family_id.strip()) < 16
    ):
        raise ValueError("JWT family_id must contain at least 16 characters")
    if extra_claims is not None and not isinstance(extra_claims, dict):
        raise TypeError("JWT extra_claims must be a dictionary")
    protected = set(_REQUIRED_CLAIMS)
    overlap = protected.intersection(extra_claims or {})
    if overlap:
        raise ValueError(
            f"JWT extra_claims cannot override protected claims: {sorted(overlap)}"
        )
    now = time.time()
    subject = str(self._require_auth_id(user))
    reserved = {
        "sub": subject,
        "iat": now,
        "exp": now + ttl,
        "typ": token_type,
        "jti": secrets.token_urlsafe(24),
        "fid": family_id or secrets.token_urlsafe(24),
        "iss": self.issuer,
        "aud": self.audience,
        "ver": self._require_auth_version(user),
    }

    payload = {**(extra_claims or {}), **reserved}

    return self._jwt.encode(payload, self.secret, algorithm=self.algorithm)


def _generate_access_token(
    self,
    user: Authenticatable,
    extra_claims: dict | None = None,
    *,
    family_id: str | None = None,
) -> str:
    """Generate access token with configured TTL."""
    return self.generate_token_with_ttl(
        user,
        self.ttl,
        _TOKEN_TYPE_ACCESS,
        extra_claims=extra_claims,
        family_id=family_id,
    )


def _generate_refresh_token(
    self,
    user: Authenticatable,
    extra_claims: dict | None = None,
    *,
    family_id: str | None = None,
) -> str:
    """Generate refresh token with configured refresh TTL."""
    return self.generate_token_with_ttl(
        user,
        self.refresh_ttl,
        _TOKEN_TYPE_REFRESH,
        extra_claims=extra_claims,
        family_id=family_id,
    )


def _generate_token_pair(
    self,
    user: Authenticatable,
    extra_claims: dict | None = None,
    *,
    family_id: str | None = None,
) -> dict[str, str]:
    """Mint an access/refresh pair bound to one rotation family."""
    family = family_id or secrets.token_urlsafe(24)
    return {
        "access_token": self.generate_access_token(
            user, extra_claims=extra_claims, family_id=family
        ),
        "refresh_token": self.generate_refresh_token(
            user, extra_claims=extra_claims, family_id=family
        ),
    }


def _blacklist_token(self, token: str) -> None:
    """Add token to blacklist.

    The entry is keyed on the credential's verified ``jti`` (see
    ``_blacklist_key``), so nothing replayable as a bearer token is stored
    and every spelling of the token is covered.
    """
    if not self.blacklist_enabled:
        return

    try:
        payload = self._decode_signed_token(token, verify_exp=False)
    except self._jwt.InvalidTokenError:
        _logger.warning("Refusing to blacklist an invalid JWT", exc_info=True)
        return

    exp = payload.get("exp", 0)
    # Rounded UP: ``int()`` cut the fraction off, so the entry could expire up
    # to a second before the token did and a logged-out token was accepted
    # again for that last moment of its life.
    ttl = max(0, math.ceil(exp - time.time()) + self.blacklist_grace_period)
    if ttl > 0:
        # Security state writes are fail-closed. Callers must know when a
        # logout/revocation did not reach the backing store; reporting
        # success while a bearer token remains live is unsafe.
        Cache.put(
            _blacklist_key(payload),
            True,
            ttl,
            strict=True,
        )


def _is_blacklisted(self, payload: dict[str, Any]) -> bool:
    """Whether the credential with these VERIFIED claims is blacklisted.

    Takes the decoded payload, not the token text: the lookup key is the
    ``jti`` claim (see ``_blacklist_key``), which only exists once the
    signature has been checked.

    Backend failure is a retryable service outage, never a false credential
    result. The caller therefore denies access while preserving the reason.
    """
    if not self.blacklist_enabled:
        return False

    try:
        return bool(
            Cache.get(
                _blacklist_key(payload),
                False,
                strict=True,
            )
        )
    except Exception as exc:
        raise _security_state_unavailable("token blacklist lookup", exc) from exc
