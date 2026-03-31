"""
Authentication Provider for Cara Framework.
Service provider that registers authentication guards.
"""

from cara.authentication.Authentication import Authentication
from cara.authentication.guards import ApiKeyGuard, JWTGuard
from cara.configuration import config
from cara.exceptions import AuthenticationConfigurationException
from cara.foundation import DeferredProvider


class AuthenticationProvider(DeferredProvider):
    """
    Authentication service provider.
    Registers guards and configures authentication system.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return ["auth"]

    def register(self) -> None:
        """Register authentication services."""
        settings = config("auth", {})
        default_guard = settings.get("default", "jwt")

        # Create authentication manager
        auth_manager = Authentication(self.application, default_guard)

        # Register authentication guards
        self._register_jwt_guard(auth_manager, settings)
        self._register_api_key_guard(auth_manager, settings)

        # Bind to container
        self.application.bind("auth", auth_manager)

    def _register_jwt_guard(self, auth_manager: Authentication, settings: dict) -> None:
        """Register JWT guard."""
        jwt_config = settings.get("guards", {}).get("jwt")
        if not jwt_config:
            raise AuthenticationConfigurationException("Missing JWT configuration")

        secret = jwt_config.get("secret")
        if not secret:
            raise AuthenticationConfigurationException("JWT secret required")

        # Create JWT guard with ALL config parameters
        jwt_guard = JWTGuard(
            application=self.application,
            secret=secret,
            algorithm=jwt_config.get("algorithm", "HS256"),
            ttl=jwt_config.get("ttl", 3600),
            refresh_ttl=jwt_config.get("refresh_ttl", 86400),
            blacklist_enabled=jwt_config.get("blacklist_enabled", True),
            blacklist_grace_period=jwt_config.get("blacklist_grace_period", 0),
            user_model=jwt_config.get("user_model", "app.models.User"),
            header_name=jwt_config.get("header_name", "Authorization"),
            header_prefix=jwt_config.get("header_prefix", "Bearer"),
        )

        auth_manager.add_guard("jwt", jwt_guard)

    def _register_api_key_guard(
        self, auth_manager: Authentication, settings: dict
    ) -> None:
        """Register API Key guard."""
        api_config = settings.get("guards", {}).get("api_key")
        if not api_config:
            return  # API Key is optional

        # Create API Key guard with ALL config parameters
        api_key_guard = ApiKeyGuard(
            application=self.application,
            user_model=api_config.get("user_model", "app.models.User"),
            api_key_field=api_config.get("api_key_field", "api_key"),
            header_name=api_config.get("header_name", "X-API-Key"),
            header_prefix=api_config.get("header_prefix", ""),
            rate_limit_enabled=api_config.get("rate_limit_enabled", False),
            rate_limit_max_attempts=api_config.get("rate_limit_max_attempts", 100),
            rate_limit_window=api_config.get("rate_limit_window", 3600),
            cache_enabled=api_config.get("cache_enabled", True),
            cache_ttl=api_config.get("cache_ttl", 3600),
        )

        auth_manager.add_guard("api_key", api_key_guard)
