"""
JWT Authentication Guard.

Clean, focused JWT authentication with all functionality in a single class.
"""

from __future__ import annotations

import hashlib
import importlib
import logging
import secrets
import time
from contextvars import ContextVar
from typing import Any

from cara.authentication.contracts import Guard
from cara.authentication.SessionPolicy import AUTH_SECURITY_MAX_WINDOW
from cara.exceptions import (
    AuthenticationConfigurationException,
    ServiceUnavailableException,
    TokenBlacklistedException,
    TokenExpiredException,
    TokenInvalidException,
    UserNotFoundException,
)
from cara.facades import Cache
from cara.http import current_request

from . import _JWTTokenLifecycle

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
            import jwt as jwt_module  # local: heavy optional dep
        except ImportError as e:
            raise AuthenticationConfigurationException(
                "PyJWT is required for JWT authentication. "
                "Please install it with: pip install PyJWT"
            ) from e
        self._jwt = jwt_module

        if not isinstance(secret, str) or len(secret.encode("utf-8")) < 32:
            raise AuthenticationConfigurationException(
                "JWT signing secret must contain at least 32 bytes"
            )
        if not isinstance(algorithm, str) or algorithm not in _ALLOWED_ALGORITHMS:
            raise AuthenticationConfigurationException(
                f"JWT algorithm must be one of {sorted(_ALLOWED_ALGORITHMS)}"
            )
        if isinstance(ttl, bool) or not isinstance(ttl, int) or not 0 < ttl <= 3600:
            raise AuthenticationConfigurationException(
                "JWT access-token TTL must be between 1 and 3600 seconds"
            )
        if (
            isinstance(refresh_ttl, bool)
            or not isinstance(refresh_ttl, int)
            or refresh_ttl <= ttl
        ):
            raise AuthenticationConfigurationException(
                "JWT refresh-token TTL must be longer than the access-token TTL"
            )
        if refresh_ttl > int(AUTH_SECURITY_MAX_WINDOW.total_seconds()):
            raise AuthenticationConfigurationException(
                "JWT refresh-token TTL must not exceed 30 days"
            )
        if (
            isinstance(blacklist_grace_period, bool)
            or not isinstance(blacklist_grace_period, int)
            or blacklist_grace_period < 0
        ):
            raise AuthenticationConfigurationException(
                "JWT blacklist grace period cannot be negative"
            )
        if blacklist_enabled is not True:
            raise AuthenticationConfigurationException(
                "JWT refresh-token rotation requires blacklist support"
            )
        for name, value in (
            ("issuer", issuer),
            ("audience", audience),
            ("user_model", user_model),
            ("header_name", header_name),
            ("header_prefix", header_prefix),
        ):
            if not isinstance(value, str) or not value.strip():
                raise AuthenticationConfigurationException(
                    f"JWT {name} must be a non-empty string"
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
        if not callable(getattr(self._user_class, "authenticate_jwt", None)):
            raise AuthenticationConfigurationException(
                "JWT user model must implement authenticate_jwt(user_id, claims)"
            )
        self._user = None
        self._token = None
        self._last_payload = None

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
        except ServiceUnavailableException:
            raise
        except Exception as exc:
            _logger.warning(
                "JWT authentication check failed unexpectedly",
                exc_info=True,
            )
            raise ServiceUnavailableException(
                "Authentication temporarily unavailable",
                retry_after=5,
            ) from exc

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
        return user.get_auth_id() if user is not None else None

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
        except ServiceUnavailableException:
            raise
        except Exception as exc:
            _logger.warning(
                "JWT token validation failed unexpectedly",
                exc_info=True,
            )
            raise ServiceUnavailableException(
                "Authentication temporarily unavailable",
                retry_after=5,
            ) from exc

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
        except ServiceUnavailableException:
            raise
        except Exception as exc:
            _logger.warning(
                "Refresh token user resolution failed unexpectedly",
                exc_info=True,
            )
            raise ServiceUnavailableException(
                "Authentication temporarily unavailable",
                retry_after=5,
            ) from exc

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
        except ServiceUnavailableException:
            raise
        except Exception as exc:
            _logger.warning("Refresh token claim failed unexpectedly", exc_info=True)
            raise ServiceUnavailableException(
                "Authentication temporarily unavailable",
                retry_after=5,
            ) from exc

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
        if not self.blacklist_enabled:
            # Refresh-token rotation is configured as fail-closed in the
            # constructor. Keep this boundary explicit in case a subclass
            # bypasses that invariant: never pretend one-time use is active
            # when there is no atomic claim store.
            return False
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
                # Descendant refresh tokens minted by the winning request can
                # live for a full refresh window even when this replayed token
                # was near expiry. Keep the family tombstone for that full
                # window or those descendants would become valid again.
                self.revoke_token_family(
                    str(payload["fid"]), ttl=max(ttl, self.refresh_ttl)
                )
            return won
        except self._jwt.InvalidTokenError, self._jwt.ExpiredSignatureError:
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
        except LookupError, RuntimeError:
            _logger.debug("No request context for JWT extraction", exc_info=True)
            return None
        except ServiceUnavailableException:
            raise
        except Exception as exc:
            _logger.warning(
                "JWT token extraction failed unexpectedly",
                exc_info=True,
            )
            raise ServiceUnavailableException(
                "Authentication request state temporarily unavailable",
                retry_after=5,
            ) from exc

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
        except ServiceUnavailableException:
            raise
        except Exception as exc:
            _logger.warning(
                "JWT user resolution from token failed unexpectedly",
                exc_info=True,
            )
            raise ServiceUnavailableException(
                "Authentication temporarily unavailable",
                retry_after=5,
            ) from exc

    def _resolve_user_by_id(
        self, user_id: str, context: dict[str, Any] = None
    ) -> Any | None:
        """Resolve user by ID with optional context - Generic JWT authentication."""
        try:
            user = self._user_class.authenticate_jwt(user_id, context or {})

            if user is None:
                return None
            token_version = (context or {}).get("ver")
            if (
                isinstance(token_version, bool)
                or not isinstance(token_version, int)
                or token_version < 1
                or self._require_auth_version(user) != token_version
            ):
                return None
            return user

        except AuthenticationConfigurationException, ServiceUnavailableException:
            raise
        except Exception as exc:
            _logger.warning(
                "JWT user resolution by ID failed unexpectedly",
                exc_info=True,
            )
            raise ServiceUnavailableException(
                "Identity store temporarily unavailable",
                retry_after=5,
            ) from exc

    @staticmethod
    def _require_auth_id(user: Any) -> str | int:
        """Return a stable scalar subject or reject the model contract."""
        try:
            identifier = user.get_auth_id()
        except (AttributeError, NotImplementedError, TypeError, ValueError) as exc:
            raise AuthenticationConfigurationException(
                "Authenticatable users must expose a stable auth id"
            ) from exc
        if isinstance(identifier, bool) or not isinstance(identifier, (str, int)):
            raise AuthenticationConfigurationException(
                "Authenticatable auth id must be a non-empty string or positive integer"
            )
        if (isinstance(identifier, int) and identifier < 1) or (
            isinstance(identifier, str) and not identifier.strip()
        ):
            raise AuthenticationConfigurationException(
                "Authenticatable auth id must be a non-empty string or positive integer"
            )
        return identifier

    @staticmethod
    def _require_auth_version(user: Any) -> int:
        """Return a valid persisted auth epoch or reject the model contract."""
        getter = getattr(user, "get_auth_version", None)
        if not callable(getter):
            raise AuthenticationConfigurationException(
                "Authenticatable users must implement get_auth_version()"
            )
        try:
            version = getter()
        except (AttributeError, NotImplementedError, TypeError, ValueError) as exc:
            raise AuthenticationConfigurationException(
                "Authenticatable users must expose a persisted auth version"
            ) from exc
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise AuthenticationConfigurationException(
                "Authenticatable auth version must be a positive integer"
            )
        return version

    _decode_token = _JWTTokenLifecycle._decode_token
    revoke_user_sessions = _JWTTokenLifecycle._revoke_user_sessions
    revoke_token_family = _JWTTokenLifecycle._revoke_token_family
    _is_family_revoked = _JWTTokenLifecycle._is_family_revoked
    _decode_signed_token = _JWTTokenLifecycle._decode_signed_token
    generate_token_with_ttl = _JWTTokenLifecycle._generate_token_with_ttl
    generate_access_token = _JWTTokenLifecycle._generate_access_token
    generate_refresh_token = _JWTTokenLifecycle._generate_refresh_token
    generate_token_pair = _JWTTokenLifecycle._generate_token_pair
    _blacklist_token = _JWTTokenLifecycle._blacklist_token
    _is_blacklisted = _JWTTokenLifecycle._is_blacklisted

    def _load_user_class(self, user_model: str):
        """Load user model class safely."""
        try:
            parts = user_model.split(".")
            module_name = ".".join(parts[:-1])
            class_name = parts[-1]

            module = importlib.import_module(module_name)
            return getattr(module, class_name)
        except (AttributeError, ImportError, TypeError, ValueError) as exc:
            raise AuthenticationConfigurationException(
                f"Cannot import auth user model {user_model!r}"
            ) from exc
