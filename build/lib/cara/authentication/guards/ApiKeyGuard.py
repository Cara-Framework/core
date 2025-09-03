"""
API Key Authentication Guard.

Clean, focused API Key authentication with all functionality in a single class.
"""

from typing import Any, Dict, List, Optional, Union

from cara.authentication.contracts import Authenticatable, Guard
from cara.exceptions import TokenInvalidException
from cara.facades import Cache


class ApiKeyGuard(Guard):
    """
    API Key Authentication Guard.

    Handles API key extraction, validation, user resolution, and rate limiting.
    All API Key functionality in one clean, focused class.
    """

    def __init__(
        self,
        application,
        header_name: str = "X-API-Key",
        header_prefix: str = "",
        # Static API key configuration
        api_keys: Optional[Union[List[str], Dict[str, Any]]] = None,
        # Database user resolution
        user_model: Optional[str] = None,
        api_key_field: str = "api_key",
        # Rate limiting
        rate_limit_enabled: bool = False,
        rate_limit_max_attempts: int = 100,
        rate_limit_window: int = 3600,
        # Caching
        cache_enabled: bool = True,
        cache_ttl: int = 3600,
    ):
        # Configuration
        self.application = application
        self.api_key_field = api_key_field
        self.rate_limit_enabled = rate_limit_enabled
        self.rate_limit_max_attempts = rate_limit_max_attempts
        self.rate_limit_window = rate_limit_window
        self.cache_enabled = cache_enabled
        self.cache_ttl = cache_ttl

        # Token extraction settings
        self.header_name = header_name
        self.header_prefix = header_prefix

        # API key configuration
        self.api_keys = api_keys or []

        # User model (optional for database-backed keys)
        self.user_model = user_model
        if user_model:
            self._user_class = self._load_user_class(user_model)
        else:
            self._user_class = None

        # Authentication state
        self._user: Optional[Any] = None
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

        # Extract and validate API key
        api_key = self._extract_api_key()
        if not api_key:
            raise TokenInvalidException(f"No {self.header_name} header provided")

        # Check rate limiting
        if self.rate_limit_enabled and not self._check_rate_limit(api_key):
            raise TokenInvalidException("Rate limit exceeded for this API key")

        # Resolve user from API key
        user = self._resolve_user_from_api_key(api_key)
        if user:
            self._user = user
            self._token = api_key
            self._record_usage(api_key)
            return user

        # If we get here, API key was provided but invalid
        raise TokenInvalidException("Invalid API key")

    def id(self) -> Optional[Any]:
        """Get the ID of the authenticated user."""
        user = self.user()
        if user and hasattr(user, "get_auth_id"):
            return user.get_auth_id()
        elif user and hasattr(user, "get_auth_identifier"):
            return user.get_auth_identifier()
        elif isinstance(user, dict):
            return user.get("api_key")
        return None

    def attempt(self, credentials: Dict[str, Any]) -> bool:
        """API keys don't use credential-based authentication."""
        return False

    def login(self, user: Authenticatable) -> str:
        """API keys don't support login - they're static."""
        raise NotImplementedError("API Keys don't support login functionality")

    def logout(self) -> None:
        """Clear authentication state."""
        self._user = None
        self._token = None

    def validate_token(self, token: str) -> bool:
        """
        Validate an API key without setting session state.

        Args:
            token: API key to validate

        Returns:
            bool: True if API key is valid
        """
        try:
            # Check rate limiting
            if self.rate_limit_enabled and not self._check_rate_limit(token):
                return False

            user = self._resolve_user_from_api_key(token)
            if user:
                self._record_usage(token)
                return True

            return False
        except Exception:
            return False

    def validate_api_key(self, api_key: str) -> bool:
        """Validate an API key (alias for validate_token)."""
        return self.validate_token(api_key)

    def get_api_key_info(self, api_key: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about an API key.

        Args:
            api_key: API key to get info for

        Returns:
            Dict containing API key information or None if invalid
        """
        try:
            user = self._resolve_user_from_api_key(api_key)
            if user:
                return {
                    "type": "api_key",
                    "api_key": api_key,
                    **(user if isinstance(user, dict) else {"user": user}),
                }
            return None
        except Exception:
            return None

    # ========================================================================
    # INTERNAL HELPER METHODS
    # ========================================================================

    def _extract_api_key(self) -> Optional[str]:
        """Extract API key from request headers."""
        try:
            from cara.http.request.context import current_request

            request = current_request.get()
            header_value = request.header(self.header_name)

            if not header_value:
                return None

            # Handle optional prefix
            if self.header_prefix and header_value.startswith(f"{self.header_prefix} "):
                return header_value[len(self.header_prefix) + 1 :]
            elif not self.header_prefix:
                return header_value

            return None
        except Exception:
            return None

    def _resolve_user_from_api_key(self, api_key: str) -> Optional[Any]:
        """
        Resolve user/info from API key - Generic API Key authentication.

        Supports both database-backed and static API keys.
        """
        if not api_key:
            return None

        # Database-backed API keys
        if self._user_class:
            try:
                # Generic API key authentication - call authenticate_api_key if available
                if hasattr(self._user_class, "authenticate_api_key"):
                    return self._user_class.authenticate_api_key(
                        api_key, {"type": "api_key"}
                    )

                # Fallback to field lookup
                return self._user_class.where(self.api_key_field, api_key).first()
            except Exception:
                return None

        # Static API keys
        return self._resolve_static_api_key(api_key)

    def _resolve_static_api_key(self, api_key: str) -> Optional[Any]:
        """Resolve API key from static configuration."""
        # Handle list of API keys
        if isinstance(self.api_keys, list):
            if api_key in self.api_keys:
                return {
                    "type": "api_key",
                    "api_key": api_key,
                    "permissions": ["read", "write"],
                }
            return None

        # Handle dictionary of API keys with metadata
        if isinstance(self.api_keys, dict):
            if api_key in self.api_keys:
                api_key_info = self.api_keys[api_key]
                if isinstance(api_key_info, dict):
                    return {
                        "type": "api_key",
                        "api_key": api_key,
                        **api_key_info,
                    }
                else:
                    return {
                        "type": "api_key",
                        "api_key": api_key,
                        "name": str(api_key_info),
                    }
            return None

        return None

    def _check_rate_limit(self, api_key: str) -> bool:
        """Check if API key is within rate limits."""
        if not self.rate_limit_enabled:
            return True

        try:
            cache_key = f"api_key_rate_limit:{api_key}"
            current_count = Cache.get(cache_key, 0)
            return current_count < self.rate_limit_max_attempts
        except Exception:
            return True

    def _record_usage(self, api_key: str) -> None:
        """Record API key usage for rate limiting and analytics."""
        if not self.rate_limit_enabled:
            return

        try:
            cache_key = f"api_key_rate_limit:{api_key}"
            current_count = Cache.get(cache_key, 0)
            Cache.put(cache_key, current_count + 1, self.rate_limit_window)
        except Exception:
            pass

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
