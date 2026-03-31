"""
JWT Authentication Guard.

Clean, focused JWT authentication with all functionality in a single class.
"""

import time
from typing import Any, Dict, Optional

from cara.authentication.contracts import Authenticatable, Guard
from cara.exceptions import (
    AuthenticationConfigurationException,
    TokenBlacklistedException,
    TokenExpiredException,
    TokenInvalidException,
    UserNotFoundException,
)
from cara.facades import Cache


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
        except ImportError:
            raise AuthenticationConfigurationException(
                "PyJWT is required for JWT authentication. "
                "Please install it with: pip install PyJWT"
            )

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

        # Authentication state
        self._user: Optional[Authenticatable] = None
        self._token: Optional[str] = None

    def check(self) -> bool:
        """Check if the current request is authenticated."""
        try:
            return self.user() is not None
        except:
            return False

    def guest(self) -> bool:
        """Check if the current request is a guest."""
        return not self.check()

    def user(self) -> Optional[Any]:
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

    def id(self) -> Optional[Any]:
        """Get the ID of the authenticated user."""
        user = self.user()
        if user and hasattr(user, "get_auth_id"):
            return user.get_auth_id()
        elif user and hasattr(user, "get_auth_identifier"):
            return user.get_auth_identifier()
        return None

    def attempt(self, credentials: Dict[str, Any]) -> bool:
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
        except:
            return False

    def validate_refresh_token(self, token: str) -> bool:
        """Validate a refresh token specifically - ignores expiration for refresh window check."""
        try:
            # Decode token without expiration check first
            payload = self._decode_token(token, verify_exp=False)
            user_id = payload.get("sub")

            if not user_id:
                return False

            # Check refresh window manually
            exp = payload.get("exp", 0)
            now = int(time.time())
            if now > exp + self.refresh_ttl:
                return False  # Beyond refresh window

            # Resolve user
            user = self._resolve_user_by_id(user_id, payload)
            return user is not None
        except:
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

            # Check refresh window
            exp = payload.get("exp", 0)
            now = int(time.time())
            if now > exp + self.refresh_ttl:
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

    def _extract_token(self) -> Optional[str]:
        """Extract JWT token from request headers."""
        try:
            from cara.http.request.context import current_request

            request = current_request.get()
            header_value = request.header(self.header_name)

            if not header_value:
                return None

            if header_value.startswith(f"{self.header_prefix} "):
                return header_value[len(self.header_prefix) + 1 :]

            return None
        except Exception:
            return None

    def _resolve_user_from_token(self, token: str) -> Optional[Any]:
        """Resolve user from JWT token payload."""
        try:
            payload = self._decode_token(token)
            user_id = payload.get("sub")

            if not user_id:
                return None

            user = self._resolve_user_by_id(user_id, payload)
            return user
        except Exception:
            return None

    def _resolve_user_by_id(
        self, user_id: str, context: Dict[str, Any] = None
    ) -> Optional[Any]:
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
                return user.get_auth_password() == password
            return False
        except Exception:
            return False

    def _decode_token(self, token: str, verify_exp: bool = True) -> Dict[str, Any]:
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
            return payload
        except jwt.ExpiredSignatureError:
            raise TokenExpiredException("Token expired")
        except jwt.InvalidTokenError:
            raise TokenInvalidException("Invalid token")

    def generate_token_with_ttl(self, user: Authenticatable, ttl: int) -> str:
        """Generate JWT token for user with custom TTL."""
        now = int(time.time())

        # Use user's custom payload if available
        if hasattr(user, "to_jwt_payload") and callable(getattr(user, "to_jwt_payload")):
            payload = user.to_jwt_payload()
            payload.update({"iat": now, "exp": now + ttl})
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
            }

        return jwt.encode(payload, self.secret, algorithm=self.algorithm)

    def generate_access_token(self, user: Authenticatable) -> str:
        """Generate access token with configured TTL."""
        return self.generate_token_with_ttl(user, self.ttl)

    def generate_refresh_token(self, user: Authenticatable) -> str:
        """Generate refresh token with configured refresh TTL."""
        return self.generate_token_with_ttl(user, self.refresh_ttl)

    def _generate_token(self, user: Authenticatable) -> str:
        """Generate JWT token for user."""
        return self.generate_token_with_ttl(user, self.ttl)

    def _blacklist_token(self, token: str) -> None:
        """Add token to blacklist."""
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
                Cache.put(f"jwt_blacklist:{token}", True, ttl)
        except Exception:
            pass

    def _is_blacklisted(self, token: str) -> bool:
        """Check if token is blacklisted."""
        if not self.blacklist_enabled:
            return False

        try:
            return Cache.get(f"jwt_blacklist:{token}", False)
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
