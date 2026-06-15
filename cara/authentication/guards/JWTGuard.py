"""
JWT Authentication Guard.

Clean, focused JWT authentication with all functionality in a single class.
"""

from __future__ import annotations

import hashlib
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

# Token type claims — tokens carry `typ` so an access token can't be
# swapped in where a refresh token is required (and vice versa).
TOKEN_TYPE_ACCESS = "access"
TOKEN_TYPE_REFRESH = "refresh"


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

        # Configuration
        self.application = application
        self.secret = secret
        self.algorithm = algorithm
        self.ttl = ttl
        self.refresh_ttl = refresh_ttl
        self.blacklist_enabled = blacklist_enabled
        self.blacklist_grace_period = blacklist_grace_period

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
        except Exception:
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
            return False

    def login(self, user: Authenticatable) -> str:
        """Log a user in and return JWT token."""
        if not isinstance(user, Authenticatable):
            raise TypeError("User must implement Authenticatable")

        self._user = user
        return self._generate_token(user)

    def logout(self) -> None:
        """Log the user out and blacklist current token."""
        if self.blacklist_enabled and self._token:
            self._blacklist_token(self._token)

        self._user = None
        self._token = None

    def validate_token(self, token: str) -> bool:
        """Validate a JWT token without setting session state."""
        try:
            user = self._resolve_user_from_token(token)
            return user is not None
        except Exception:
            return False

    def resolve_refresh_token_user(self, token: str) -> Any | None:
        """Decode a refresh token and return the associated user (or None)."""
        try:
            payload = self._decode_token(token, verify_exp=False)
            user_id = payload.get("sub")
            if not user_id:
                return None
            if payload.get("typ") != TOKEN_TYPE_REFRESH:
                return None
            return self._resolve_user_by_id(user_id, payload)
        except Exception:
            return None

    def blacklist_token(self, token: str) -> None:
        """Public wrapper around _blacklist_token for external callers."""
        self._blacklist_token(token)

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
            return True
        try:
            payload = jwt.decode(
                token,
                self.secret,
                algorithms=[self.algorithm],
                options={"verify_exp": False},
            )
            exp = payload.get("exp", 0)
            ttl = max(0, exp - int(time.time()) + self.blacklist_grace_period)
            if ttl <= 0:
                # Token already past its natural lifetime; refuse rather
                # than write a zero-TTL key that vanishes immediately.
                return False
            return bool(Cache.add(f"jwt_blacklist:{_hash_token(token)}", True, ttl))
        except Exception:
            return False

    def validate_refresh_token(self, token: str) -> bool:
        """Validate a refresh token specifically - ignores expiration for refresh window check."""
        try:
            # Decode token without expiration check first
            payload = self._decode_token(token, verify_exp=False)
            user_id = payload.get("sub")

            if not user_id:
                return False

            # Enforce token-type claim: a leaked access token must not be
            # usable as a refresh token. We treat legacy tokens without a
            # `typ` claim as invalid for refresh (they must re-auth).
            if payload.get("typ") != TOKEN_TYPE_REFRESH:
                return False

            # Check refresh window. The refresh token is minted with
            # ``exp = iat + refresh_ttl`` (see generate_refresh_token), so
            # ``exp`` already IS the end of the refresh window. Adding
            # ``refresh_ttl`` again doubled it — a 3-day refresh token was
            # accepted for 6 days, doubling the replay window of a stolen,
            # never-consumed refresh token.
            exp = payload.get("exp", 0)
            now = int(time.time())
            if now > exp:
                return False  # Beyond refresh window

            # Resolve user
            user = self._resolve_user_by_id(user_id, payload)
            return user is not None
        except Exception:
            return False

    def refresh(self) -> str:
        """
        Refresh the current JWT token.

        Returns:
            str: New JWT token

        Raises:
            TokenInvalidException: If no token or invalid token
            TokenExpiredException: If token is beyond refresh window
            UserNotFoundException: If user no longer exists
        """
        token = self._extract_token()
        if not token:
            raise TokenInvalidException("No token provided")

        try:
            # Decode token without expiration check
            payload = self._decode_token(token, verify_exp=False)
            user_id = payload.get("sub")

            if not user_id:
                raise TokenInvalidException("Invalid token payload")

            # Reject access tokens passed to /refresh — defence in depth
            # against access-token leaks (logs, dev tools, XSS).
            if payload.get("typ") != TOKEN_TYPE_REFRESH:
                raise TokenInvalidException("Provided token is not a refresh token")

            # Check refresh window. ``exp`` is already ``iat + refresh_ttl``
            # (see generate_refresh_token), so it IS the window end; adding
            # refresh_ttl again doubled the accepted lifetime.
            exp = payload.get("exp", 0)
            now = int(time.time())
            if now > exp:
                raise TokenExpiredException("Refresh token expired")

            # Resolve user
            user = self._resolve_user_by_id(user_id, payload)
            if not user:
                raise UserNotFoundException("User not found")

            if not isinstance(user, Authenticatable):
                raise TypeError("User must implement Authenticatable")

            # Blacklist old token and generate new one
            if self.blacklist_enabled:
                self._blacklist_token(token)

            self._user = user
            return self._generate_token(user)

        except jwt.ExpiredSignatureError:
            raise TokenExpiredException("Token expired")
        except jwt.InvalidTokenError:
            raise TokenInvalidException("Invalid token")

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
            from cara.http.request.context import current_request

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
        except Exception:
            return None

    def _resolve_user_from_token(self, token: str) -> Any | None:
        """Resolve user from JWT token payload.

        Enforces the access-token type claim. ``refresh()`` already
        rejects access tokens passed to ``/auth/refresh`` via the
        symmetric ``typ == refresh`` check, but the inverse — a
        refresh token presented in the ``Authorization`` header on
        any auth-protected route — was previously accepted as if it
        were an access token. Refresh tokens carry a much longer
        lifetime (3 days vs. 30 minutes for access) and are intended
        for the single ``/refresh`` endpoint only, so accepting one
        as an access token effectively extended every authenticated
        session by the refresh TTL. We treat legacy tokens without a
        ``typ`` claim as access tokens for backwards compatibility —
        the field was added recently and any in-flight token at
        rollout will still resolve.
        """
        try:
            payload = self._decode_token(token)
            user_id = payload.get("sub")

            if not user_id:
                return None

            typ = payload.get("typ")
            if typ is not None and typ != TOKEN_TYPE_ACCESS:
                return None

            user = self._resolve_user_by_id(user_id, payload)
            return user
        except Exception:
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
            return False

    def _decode_token(self, token: str, verify_exp: bool = True) -> dict[str, Any]:
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
        if self.blacklist_enabled and self._is_blacklisted(token):
            raise TokenBlacklistedException("Token has been blacklisted")

        try:
            options = {"verify_exp": verify_exp}
            payload = jwt.decode(
                token, self.secret, algorithms=[self.algorithm], options=options
            )
        except jwt.ExpiredSignatureError:
            raise TokenExpiredException("Token expired")
        except jwt.InvalidTokenError:
            raise TokenInvalidException("Invalid token")

        # Per-user revocation cutoff. After a security-sensitive change
        # (password reset, email change, "log out all sessions"), the
        # caller bumps ``jwt_user_revoke:{sub}`` to ``now``. Any token
        # with ``iat`` strictly older than that cutoff is treated as
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
            # Collect the set of exception types that mean "cache
            # backend is degraded — we cannot trust an absent
            # revocation cutoff". Built-in network / timeout types
            # always; redis-py types when the library is importable
            # so we don't accidentally widen the catch to unrelated
            # exceptions. Anything outside this set bubbles up to the
            # caller (legitimate programming errors stay loud).
            cache_failure_types: tuple[type[BaseException], ...] = (
                ConnectionError,
                TimeoutError,
                OSError,
            )
            try:
                from cara.exceptions import (
                    CacheConfigurationException,
                    DriverNotRegisteredException,
                )

                cache_failure_types = cache_failure_types + (
                    CacheConfigurationException,
                    DriverNotRegisteredException,
                )
            except ImportError:
                pass
            try:
                import redis as _redis  # type: ignore

                cache_failure_types = cache_failure_types + (
                    _redis.exceptions.RedisError,  # type: ignore[attr-defined]
                )
            except (ImportError, AttributeError):
                pass

            try:
                cutoff = Cache.get(f"jwt_user_revoke:{sub}", 0)
            except cache_failure_types as exc:
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
                        f"JWTGuard._decode_token: revocation-cutoff cache "
                        f"read failed for sub={sub!r}; failing closed: "
                        f"{type(exc).__name__}: {exc}",
                        category="cara.auth.jwt",
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
            if cutoff and int(iat) < int(cutoff):
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
        try:
            Cache.put(f"jwt_user_revoke:{user_id}", int(time.time()), cache_ttl)
        except Exception:
            # Logging only — we never want a cache hiccup to silently
            # skip revocation. Caller should treat this as best-effort.
            try:
                from cara.facades import Log

                Log.error(
                    f"JWTGuard.revoke_user_sessions failed for user_id={user_id}",
                    category="cara.auth.jwt",
                )
            except ImportError:
                pass

    def generate_token_with_ttl(
        self,
        user: Authenticatable,
        ttl: int,
        token_type: str = TOKEN_TYPE_ACCESS,
    ) -> str:
        """Generate JWT token for user with custom TTL and type.

        The `token_type` becomes the `typ` claim — used by refresh() and
        validate_refresh_token() to ensure access tokens can't be swapped
        in for refresh tokens or vice versa.
        """
        now = int(time.time())

        # Use user's custom payload if available
        if hasattr(user, "to_jwt_payload") and callable(user.to_jwt_payload):
            payload = user.to_jwt_payload()
            payload.update({"iat": now, "exp": now + ttl, "typ": token_type})
        else:
            # Default payload
            payload = {
                "sub": str(
                    user.get_auth_id()
                    if hasattr(user, "get_auth_id")
                    else user.get_auth_identifier()
                ),
                "iat": now,
                "exp": now + ttl,
                "typ": token_type,
            }

        return jwt.encode(payload, self.secret, algorithm=self.algorithm)

    def generate_access_token(self, user: Authenticatable) -> str:
        """Generate access token with configured TTL."""
        return self.generate_token_with_ttl(user, self.ttl, TOKEN_TYPE_ACCESS)

    def generate_refresh_token(self, user: Authenticatable) -> str:
        """Generate refresh token with configured refresh TTL."""
        return self.generate_token_with_ttl(user, self.refresh_ttl, TOKEN_TYPE_REFRESH)

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
            payload = jwt.decode(
                token,
                self.secret,
                algorithms=[self.algorithm],
                options={"verify_exp": False},
            )
            exp = payload.get("exp", 0)

            # Calculate TTL for blacklist
            ttl = max(0, exp - int(time.time()) + self.blacklist_grace_period)
            if ttl > 0:
                Cache.put(f"jwt_blacklist:{_hash_token(token)}", True, ttl)
        except Exception as exc:
            # A malformed or already-expired token reaching blacklist is
            # notable but recoverable — log rather than silently swallow.
            try:
                from cara.facades import Log

                Log.warning(
                    f"JWT blacklist add failed (token ignored): {exc}",
                    category="cara.auth.jwt",
                )
            except ImportError:
                pass

    def _is_blacklisted(self, token: str) -> bool:
        """Check if token is blacklisted (by hash — see _blacklist_token)."""
        if not self.blacklist_enabled:
            return False

        try:
            return Cache.get(f"jwt_blacklist:{_hash_token(token)}", False)
        except Exception:
            return False

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
