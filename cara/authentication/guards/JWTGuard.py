"""
JWT Authentication Guard.

Clean, focused JWT authentication with all functionality in a single class.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
import time
from contextvars import ContextVar
from typing import Any

from cara.authentication.contracts import Authenticatable, Guard
from cara.exceptions import (
    AuthenticationConfigurationException,
    TokenBlacklistedException,
    TokenExpiredException,
    TokenInvalidException,
    UserNotFoundException,
)
from cara.facades import Cache

# Per-request cache for the resolved user / consumed token. Lives in a
# ContextVar so each asyncio task (one per HTTP request / WS connection)
# gets its own slot. The guard itself is a process-singleton — without
# ContextVar isolation, ``self._user = userA`` from request A is still
# truthy when request B arrives mid-await, and request B's call to
# ``user()`` returns Alice instead of validating B's own Authorization
# header (cross-request identity leak under concurrency).
_REQUEST_USER: ContextVar[Any] = ContextVar("jwt_guard_user", default=None)
_REQUEST_TOKEN: ContextVar[Any] = ContextVar("jwt_guard_token", default=None)
# Verified claims of the most recently resolved token. Same leak class as
# _user/_token above — as a plain instance attribute on the singleton
# guard, request B could read request A's claims mid-await.
_REQUEST_PAYLOAD: ContextVar[Any] = ContextVar("jwt_guard_payload", default=None)

_logger = logging.getLogger("cara.auth.jwt")

_AUTH_FAILURES = (
    TokenInvalidException,
    TokenExpiredException,
    TokenBlacklistedException,
    UserNotFoundException,
)

# Token type claims — tokens carry `typ` so an access token can't be
# swapped in where a refresh token is required (and vice versa).
TOKEN_TYPE_ACCESS = "access"
TOKEN_TYPE_REFRESH = "refresh"
_REQUIRED_CLAIMS = (
    "sub",
    "iat",
    "exp",
    "typ",
    "jti",
    "fid",
    "iss",
    "aud",
    "ver",
)
_ALLOWED_ALGORITHMS = {"HS256", "HS384", "HS512"}
_WEBSOCKET_TICKET_TTL_SECONDS = 30


def _hash_token(token: str) -> str:
    """Hash a JWT for use as a cache key. Prevents raw tokens in Redis."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class JWTGuard(Guard):
    """
    JWT Authentication Guard.

    Handles JWT token extraction, validation, user resolution, and blacklisting.
    All JWT functionality in one clean, focused class.
    """

    def __init__(
        self,
        application,
        secret: str,
        algorithm: str = "HS256",
        ttl: int = 3600,
        refresh_ttl: int = 86400,
        blacklist_enabled: bool = True,
        blacklist_grace_period: int = 0,
        user_model: str = "app.models.User",
        header_name: str = "Authorization",
        header_prefix: str = "Bearer",
        issuer: str = "cara",
        audience: str = "cara-clients",
    ):
        # Validate PyJWT dependency
        try:
            global jwt
            import jwt
        except ImportError as e:
            raise AuthenticationConfigurationException(
                "PyJWT is required for JWT authentication. "
                "Please install it with: pip install PyJWT"
            ) from e

        if len(secret.encode("utf-8")) < 32:
            raise AuthenticationConfigurationException(
                "JWT signing secret must contain at least 32 bytes"
            )
        if algorithm not in _ALLOWED_ALGORITHMS:
            raise AuthenticationConfigurationException(
                f"JWT algorithm must be one of {sorted(_ALLOWED_ALGORITHMS)}"
            )
        if not 0 < int(ttl) <= 3600:
            raise AuthenticationConfigurationException(
                "JWT access-token TTL must be between 1 and 3600 seconds"
            )
        if int(refresh_ttl) <= int(ttl):
            raise AuthenticationConfigurationException(
                "JWT refresh-token TTL must be longer than the access-token TTL"
            )
        if int(refresh_ttl) > 30 * 24 * 60 * 60:
            raise AuthenticationConfigurationException(
                "JWT refresh-token TTL must not exceed 30 days"
            )
        if int(blacklist_grace_period) < 0:
            raise AuthenticationConfigurationException(
                "JWT blacklist grace period cannot be negative"
            )
        if not blacklist_enabled:
            raise AuthenticationConfigurationException(
                "JWT refresh-token rotation requires blacklist support"
            )
        if not issuer or not audience:
            raise AuthenticationConfigurationException(
                "JWT issuer and audience must be configured"
            )

        # Configuration
        self.application = application
        self.secret = secret
        self.algorithm = algorithm
        self.ttl = ttl
        self.refresh_ttl = refresh_ttl
        self.blacklist_enabled = blacklist_enabled
        self.blacklist_grace_period = blacklist_grace_period
        self.issuer = issuer
        self.audience = audience

        # Token extraction settings
        self.header_name = header_name
        self.header_prefix = header_prefix

        # User model
        self.user_model = user_model
        self._user_class = self._load_user_class(user_model)

        # Authentication state is stored in module-level ContextVars
        # (see top of file). ``self._user`` / ``self._token`` are
        # exposed as descriptors so existing call sites — and the
        # ``ResetAuth`` terminable middleware that clears them — keep
        # working unchanged, but the underlying storage is now scoped
        # per-asyncio-task. Concurrent requests no longer share a
        # cached identity through this singleton guard instance.

    @property
    def last_payload(self) -> dict:
        """Verified claims of the most recently resolved access token."""
        return dict(self._last_payload or {})

    @property
    def _last_payload(self) -> Any | None:
        return _REQUEST_PAYLOAD.get()

    @_last_payload.setter
    def _last_payload(self, value: Any | None) -> None:
        _REQUEST_PAYLOAD.set(value)

    @property
    def _user(self) -> Any | None:
        return _REQUEST_USER.get()

    @_user.setter
    def _user(self, value: Any | None) -> None:
        _REQUEST_USER.set(value)

    @property
    def _token(self) -> str | None:
        return _REQUEST_TOKEN.get()

    @_token.setter
    def _token(self, value: str | None) -> None:
        _REQUEST_TOKEN.set(value)

    def check(self) -> bool:
        """Check if the current request is authenticated."""
        try:
            return self.user() is not None
        except _AUTH_FAILURES:
            _logger.debug("JWT authentication check failed", exc_info=True)
            return False
        except Exception:
            _logger.warning(
                "JWT authentication check failed unexpectedly",
                exc_info=True,
            )
            return False

    def guest(self) -> bool:
        """Check if the current request is a guest."""
        return not self.check()

    def user(self) -> Any | None:
        """Get the currently authenticated user."""
        if self._user:
            return self._user

        # Extract and validate token
        token = self._extract_token()
        if not token:
            raise TokenInvalidException(
                "No Authorization header provided or invalid format (should be 'Bearer <token>')"
            )

        # Resolve user from token
        user = self._resolve_user_from_token(token)
        if user:
            self._user = user
            self._token = token
            return user

        # If we get here, token was provided but invalid
        raise TokenInvalidException("Invalid or expired JWT token")

    def id(self) -> Any | None:
        """Get the ID of the authenticated user."""
        user = self.user()
        if user and hasattr(user, "get_auth_id"):
            return user.get_auth_id()
        elif user and hasattr(user, "get_auth_identifier"):
            return user.get_auth_identifier()
        return None

    def attempt(self, credentials: dict[str, Any]) -> bool:
        """Attempt to authenticate using credentials."""
        username = credentials.get("email") or credentials.get("username")
        password = credentials.get("password")

        if not username or not password:
            return False

        try:
            # Find user by email/username
            user = self._user_class.where("email", username).first()
            if not user:
                return False

            # Validate password
            if self._validate_password(user, password):
                self._user = user
                return True

            return False
        except Exception:
            _logger.warning(
                "JWT credential authentication failed unexpectedly",
                exc_info=True,
            )
            return False

    def login(self, user: Authenticatable) -> str:
        """Log a user in and return JWT token."""
        if not isinstance(user, Authenticatable):
            raise TypeError("User must implement Authenticatable")

        self._user = user
        return self._generate_token(user)

    def logout(self) -> None:
        """Log the user out and blacklist current token."""
        # ``_token`` lives in a ContextVar — when authentication ran in a
        # run_in_thread copy of the context (AuthenticateUser middleware),
        # the write never reached this task. Fall back to re-extracting
        # from the request so logout still blacklists the token instead
        # of silently leaving it valid until expiry.
        token = self._token or self._extract_token()
        if self.blacklist_enabled and token:
            self._blacklist_token(token)

        self._user = None
        self._token = None
        self._last_payload = None

    def validate_token(self, token: str) -> bool:
        """Validate a JWT token without setting session state."""
        try:
            user = self._resolve_user_from_token(token)
            return user is not None
        except _AUTH_FAILURES:
            _logger.debug("JWT token validation failed", exc_info=True)
            return False
        except Exception:
            _logger.warning(
                "JWT token validation failed unexpectedly",
                exc_info=True,
            )
            return False

    def resolve_refresh_token_user(self, token: str) -> Any | None:
        """Decode a refresh token and return the associated user (or None)."""
        try:
            payload = self._decode_token(token)
            self._last_payload = dict(payload)
            user_id = payload.get("sub")
            if not user_id:
                return None
            if payload.get("typ") != TOKEN_TYPE_REFRESH:
                return None
            return self._resolve_user_by_id(user_id, payload)
        except _AUTH_FAILURES:
            _logger.debug("Refresh token user resolution failed", exc_info=True)
            return None
        except Exception:
            _logger.warning(
                "Refresh token user resolution failed unexpectedly",
                exc_info=True,
            )
            return None

    def consume_refresh_token_user(self, token: str) -> Any | None:
        """Atomically claim a refresh token and resolve its current user.

        The regular resolver checks the per-token blacklist first, which is
        correct for validation but hides a replay from rotation-reuse
        detection. This path deliberately skips only that one lookup, still
        verifies signature/expiry/family/user cutoff, then lets the atomic
        claim detect a second use and revoke the whole family.
        """
        try:
            payload = self._decode_token(token, check_token_blacklist=False)
            if payload.get("typ") != TOKEN_TYPE_REFRESH:
                return None
            user_id = payload.get("sub")
            if not user_id:
                return None
            user = self._resolve_user_by_id(str(user_id), payload)
            if user is None or not self.consume_refresh_token(token):
                return None
            self._last_payload = dict(payload)
            return user
        except _AUTH_FAILURES:
            _logger.debug("Refresh token claim failed", exc_info=True)
            return None
        except Exception:
            _logger.warning("Refresh token claim failed unexpectedly", exc_info=True)
            return None

    def blacklist_token(self, token: str) -> None:
        """Public wrapper around _blacklist_token for external callers."""
        self._blacklist_token(token)

    def issue_websocket_ticket(self, access_token: str) -> str:
        """Exchange a valid access JWT for a short-lived one-time WS ticket.

        Browser WebSocket APIs cannot set Authorization headers. Putting a JWT
        in the URL leaks it into proxy/access logs, so the URL carries only an
        opaque 30-second ticket whose cache record contains verified claims.
        """
        payload = self._decode_token(access_token)
        if payload.get("typ") != TOKEN_TYPE_ACCESS:
            raise TokenInvalidException("An access token is required")
        user = self._resolve_user_by_id(str(payload["sub"]), payload)
        if user is None:
            raise TokenInvalidException("Invalid access token")
        ticket = secrets.token_urlsafe(32)
        Cache.put(
            f"jwt_ws_ticket:{_hash_token(ticket)}",
            {"sub": str(payload["sub"]), "claims": dict(payload)},
            _WEBSOCKET_TICKET_TTL_SECONDS,
            strict=True,
        )
        return ticket

    def consume_websocket_ticket(self, ticket: str) -> Any | None:
        """Atomically consume a WS ticket and resolve its still-live user."""
        if not ticket:
            return None
        record = Cache.pull(f"jwt_ws_ticket:{_hash_token(ticket)}", None)
        if not isinstance(record, dict) or not isinstance(record.get("claims"), dict):
            return None
        claims = dict(record["claims"])
        if claims.get("typ") != TOKEN_TYPE_ACCESS or str(claims.get("sub")) != str(
            record.get("sub")
        ):
            return None
        family_id = claims.get("fid")
        if not isinstance(family_id, str) or not family_id:
            return None
        if self._is_family_revoked(family_id):
            return None
        user = self._resolve_user_by_id(str(record["sub"]), claims)
        if user is not None:
            self._last_payload = claims
        return user

    def consume_refresh_token(self, token: str) -> bool:
        """Atomically claim a refresh token for one-time use.

        Returns ``True`` if the caller wins the slot, ``False`` if the
        token has already been consumed (or is already blacklisted).

        The ``blacklist_token`` + later ``_is_blacklisted`` pair is a
        racy combo when used as one-time-use enforcement: two parallel
        ``/auth/refresh`` requests both pass ``validate_refresh_token``
        (blacklist hasn't been written yet), then both write — and
        both walk away with fresh token pairs. The fix is to make
        "is this the first use?" and "mark used" a single atomic op.

        ``Cache.add`` does exactly that: under Redis it's a ``SET ...
        NX EX <ttl>`` round-trip, so only one caller gets a True
        return for a given key. We use the blacklist key namespace so
        a token that was burned via ``logout`` or admin revocation
        still loses the race here.
        """
        try:
            # Decode signature + registered claims without consulting the
            # per-token blacklist: a replayed token is already blacklisted,
            # but we still need its verified family id to revoke the family.
            payload = self._decode_signed_token(token)
            if payload.get("typ") != TOKEN_TYPE_REFRESH:
                return False
            if self._is_family_revoked(str(payload["fid"])):
                return False
            exp = payload.get("exp", 0)
            ttl = max(0, int(exp - time.time()) + self.blacklist_grace_period)
            if ttl <= 0:
                # Token already past its natural lifetime; refuse rather
                # than write a zero-TTL key that vanishes immediately.
                return False
            won = bool(Cache.add(f"jwt_blacklist:{_hash_token(token)}", True, ttl))
            if not won:
                # REUSE DETECTION (OAuth 2.0 Security BCP §4.13.2): this
                # refresh token was ALREADY burned — rotated once and now
                # replayed, or killed by logout/admin. A rotated refresh
                # that shows up a SECOND time is the classic leaked-token
                # signal, so revoke this login family. Other devices keep
                # their independent families.
                self.revoke_token_family(str(payload["fid"]), ttl=ttl)
            return won
        except (jwt.InvalidTokenError, jwt.ExpiredSignatureError):
            _logger.debug("Refresh token consume failed", exc_info=True)
            return False

    def validate_refresh_token(self, token: str) -> bool:
        """Validate a refresh token specifically - ignores expiration for refresh window check."""
        try:
            payload = self._decode_token(token)
            user_id = payload.get("sub")

            if not user_id:
                return False

            # Enforce token-type claim: a leaked access token must not be
            # usable as a refresh token.
            if payload.get("typ") != TOKEN_TYPE_REFRESH:
                return False

            # Resolve user
            user = self._resolve_user_by_id(user_id, payload)
            return user is not None
        except _AUTH_FAILURES:
            _logger.debug("Refresh token validation failed", exc_info=True)
            return False

    # ========================================================================
    # INTERNAL HELPER METHODS
    # ========================================================================

    def _extract_token(self) -> str | None:
        """Extract JWT token from request headers.

        The Authorization scheme name is **case-insensitive** per
        RFC 7235 §2.1 ("auth-scheme ... case-insensitively"). Real
        clients send every casing — ``Bearer`` (canonical),
        ``bearer`` (curl / Postman exports / shell scripts that
        lowercase everything), ``BEARER`` (older OAuth integrations).
        Pre-fix the prefix check was a plain ``startswith("Bearer ")``,
        so any non-canonical casing surfaced as
        ``TokenInvalidException("No Authorization header provided
        or invalid format")`` — the same response as a missing
        header. The user holding a valid JWT couldn't tell their
        token had been rejected for casing alone.

        Only the SCHEME case is normalised; the token bytes that
        follow are preserved exactly (JWT base64url is
        case-sensitive — lowercasing the signature segment makes
        every token invalid).
        """
        try:
            from cara.http.request.Context import current_request

            request = current_request.get()
            header_value = request.header(self.header_name)

            if not header_value:
                return None

            prefix_len = len(self.header_prefix)
            # Need at least ``<prefix><space>`` before any token can
            # follow. Cheaper than building the lowercase head twice.
            if len(header_value) <= prefix_len:
                return None
            if header_value[prefix_len] != " ":
                return None
            if header_value[:prefix_len].lower() != self.header_prefix.lower():
                return None
            return header_value[prefix_len + 1 :]
        except (LookupError, RuntimeError):
            _logger.debug("No request context for JWT extraction", exc_info=True)
            return None
        except Exception:
            _logger.warning(
                "JWT token extraction failed unexpectedly",
                exc_info=True,
            )
            return None

    def _resolve_user_from_token(self, token: str) -> Any | None:
        """Resolve user from JWT token payload.

        Enforces the access-token type claim. ``refresh()`` already
        rejects access tokens passed to ``/auth/refresh`` via the
        symmetric ``typ == refresh`` check, but the inverse — a
        refresh token presented in the ``Authorization`` header on
        any auth-protected route — must not authenticate as an access
        token. Refresh tokens carry a much longer lifetime and are
        intended for the single ``/refresh`` endpoint only. Tokens
        without a ``typ`` claim are rejected.
        """
        try:
            payload = self._decode_token(token)
            user_id = payload.get("sub")

            if not user_id:
                return None

            if payload.get("typ") != TOKEN_TYPE_ACCESS:
                return None

            user = self._resolve_user_by_id(user_id, payload)
            # Expose the verified claims to the middleware layer
            # (request.jwt_claims) — e.g. the impersonation ``imp`` marker.
            self._last_payload = dict(payload)
            return user
        except _AUTH_FAILURES:
            _logger.debug("JWT user resolution from token failed", exc_info=True)
            return None
        except Exception:
            _logger.warning(
                "JWT user resolution from token failed unexpectedly",
                exc_info=True,
            )
            return None

    def _resolve_user_by_id(
        self, user_id: str, context: dict[str, Any] = None
    ) -> Any | None:
        """Resolve user by ID with optional context - Generic JWT authentication."""
        try:
            # Generic JWT authentication - call authenticate_jwt if available
            if hasattr(self._user_class, "authenticate_jwt"):
                user = self._user_class.authenticate_jwt(user_id, context or {})
                return user

            # Fallback to standard lookup by user_id or id
            # Try user_id field first (if exists)
            if hasattr(self._user_class, "where"):
                user = self._user_class.where("user_id", user_id).first()
                if user:
                    return user

            # Fallback to primary key lookup
            user = self._user_class.find(user_id)
            return user

        except Exception:
            _logger.warning(
                "JWT user resolution by ID failed unexpectedly",
                exc_info=True,
            )
            return None

    def _validate_password(self, user: Any, password: str) -> bool:
        """Validate user password."""
        try:
            if hasattr(user, "verify_password"):
                return user.verify_password(password)
            elif hasattr(user, "get_auth_password"):
                from cara.encryption import Hash

                return Hash.check(password, user.get_auth_password())
            return False
        except Exception:
            _logger.warning(
                "JWT password validation failed unexpectedly",
                exc_info=True,
            )
            return False

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
        except jwt.ExpiredSignatureError:
            raise TokenExpiredException("Token expired")
        except jwt.InvalidTokenError:
            raise TokenInvalidException("Invalid token")

        # Verify the signature and mandatory registered claims BEFORE touching
        # cache. Invalid attacker-controlled strings must not become Redis I/O.
        if (
            check_token_blacklist
            and self.blacklist_enabled
            and self._is_blacklisted(token)
        ):
            raise TokenBlacklistedException("Token has been blacklisted")
        if self._is_family_revoked(str(payload["fid"])):
            raise TokenBlacklistedException("Token family has been revoked")

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
        if sub and iat is not None:
            try:
                cutoff = Cache.get(f"jwt_user_revoke:{sub}", 0, strict=True)
            except Exception as exc:
                # Cache backend failure — fail CLOSED. We don't know
                # whether this user's tokens were revoked; treat them
                # as if they were. Log at ERROR so ops can spot the
                # Redis outage in the dashboards. A user who is
                # accidentally locked out can recover by re-logging in;
                # a leaked token that keeps working has no recovery
                # path because the legitimate owner doesn't know the
                # token was ever stolen.
                try:
                    from cara.facades import Log

                    Log.error(
                        "JWTGuard._decode_token: revocation-cutoff cache read failed for sub=%s; failing closed: %s: %s",
                        sub,
                        type(exc).__name__,
                        exc,
                        category="cara.auth.jwt",
                        exc_info=True,
                    )
                except ImportError:
                    pass
                raise TokenBlacklistedException(
                    "Token revocation status unavailable; failing closed."
                ) from exc

            # Cache.get(..., 0) returning ``None`` / ``0`` is the
            # legitimate "no revocation event recorded" branch — fall
            # through and accept the token. Only a positive cutoff
            # whose value strictly exceeds the token's ``iat`` rejects.
            if cutoff and float(iat) <= float(cutoff):
                raise TokenBlacklistedException(
                    "Token revoked: issued before user-level revocation cutoff"
                )

        return payload

    def revoke_user_sessions(self, user_id: Any, ttl: int | None = None) -> None:
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

    def revoke_token_family(self, family_id: str, ttl: int | None = None) -> None:
        """Revoke one login/refresh family without signing out other devices."""
        if not family_id:
            raise ValueError("family_id is required")
        cache_ttl = ttl if ttl is not None else self.refresh_ttl
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

    def _decode_signed_token(
        self, token: str, *, verify_exp: bool = True
    ) -> dict[str, Any]:
        return jwt.decode(
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

    def generate_token_with_ttl(
        self,
        user: Authenticatable,
        ttl: int,
        token_type: str = TOKEN_TYPE_ACCESS,
        extra_claims: dict | None = None,
        *,
        family_id: str | None = None,
    ) -> str:
        """Generate JWT token for user with custom TTL and type.

        The `token_type` becomes the `typ` claim — used by refresh() and
        validate_refresh_token() to ensure access tokens can't be swapped
        in for refresh tokens or vice versa.

        ``extra_claims`` are merged into the payload LAST-BUT-PROTECTED:
        reserved claims (sub/iat/exp/typ) always win, so a caller can add
        markers (e.g. an impersonation ``imp`` claim) but can never forge
        identity or lifetime.
        """
        now = time.time()
        subject = str(
            user.get_auth_id()
            if hasattr(user, "get_auth_id")
            else user.get_auth_identifier()
        )
        reserved = {
            "sub": subject,
            "iat": now,
            "exp": now + ttl,
            "typ": token_type,
            "jti": secrets.token_urlsafe(24),
            "fid": family_id or secrets.token_urlsafe(24),
            "iss": self.issuer,
            "aud": self.audience,
            "ver": int(
                user.get_auth_version()
                if hasattr(user, "get_auth_version")
                else getattr(user, "auth_version", 1)
            ),
        }

        # Use user's custom payload if available
        if hasattr(user, "to_jwt_payload") and callable(user.to_jwt_payload):
            payload = dict(user.to_jwt_payload() or {})
            if extra_claims:
                payload.update(extra_claims)
            payload.update(reserved)
        else:
            payload = {**(extra_claims or {}), **reserved}

        return jwt.encode(payload, self.secret, algorithm=self.algorithm)

    def generate_access_token(
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
            TOKEN_TYPE_ACCESS,
            extra_claims=extra_claims,
            family_id=family_id,
        )

    def generate_refresh_token(
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
            TOKEN_TYPE_REFRESH,
            extra_claims=extra_claims,
            family_id=family_id,
        )

    def generate_token_pair(
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

    def _generate_token(self, user: Authenticatable) -> str:
        """Generate JWT access token for user."""
        return self.generate_token_with_ttl(user, self.ttl, TOKEN_TYPE_ACCESS)

    def _blacklist_token(self, token: str) -> None:
        """Add token to blacklist.

        We store a SHA-256 hash rather than the raw token so a cache dump
        or log line can't be replayed as a bearer token. Collision risk is
        negligible for SHA-256 over the token space.
        """
        if not self.blacklist_enabled:
            return

        try:
            payload = self._decode_signed_token(token, verify_exp=False)
        except jwt.InvalidTokenError:
            _logger.warning("Refusing to blacklist an invalid JWT", exc_info=True)
            return

        exp = payload.get("exp", 0)
        ttl = max(0, int(exp - time.time()) + self.blacklist_grace_period)
        if ttl > 0:
            # Security state writes are fail-closed. Callers must know when a
            # logout/revocation did not reach the backing store; reporting
            # success while a bearer token remains live is unsafe.
            Cache.put(
                f"jwt_blacklist:{_hash_token(token)}",
                True,
                ttl,
                strict=True,
            )

    def _is_blacklisted(self, token: str) -> bool:
        """Check if token is blacklisted (by hash — see _blacklist_token).

        Fails CLOSED: if the cache is unavailable, we treat the token
        as blacklisted (reject). This aligns with the revocation cutoff
        fail-closed policy and prevents revoked tokens from authenticating
        during Redis outages.
        """
        if not self.blacklist_enabled:
            return False

        try:
            return bool(
                Cache.get(
                    f"jwt_blacklist:{_hash_token(token)}",
                    False,
                    strict=True,
                )
            )
        except Exception:
            _logger.warning(
                "JWT blacklist check failed — failing closed (treating as blacklisted)",
                exc_info=True,
            )
            return True

    def _load_user_class(self, user_model: str):
        """Load user model class safely."""
        try:
            parts = user_model.split(".")
            module_name = ".".join(parts[:-1])
            class_name = parts[-1]

            import importlib

            module = importlib.import_module(module_name)
            return getattr(module, class_name)
        except Exception as e:
            raise ImportError(f"Cannot import user model: {user_model}") from e
