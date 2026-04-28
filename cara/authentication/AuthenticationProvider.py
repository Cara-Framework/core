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
        default_guard = config("auth.default", "jwt")
        auth_manager = Authentication(self.application, default_guard)

        self._register_jwt_guard(auth_manager)
        self._register_api_key_guard(auth_manager)

        self.application.bind("auth", auth_manager)

    def _register_jwt_guard(self, auth_manager: Authentication) -> None:
        """Register JWT guard."""
        secret = config("auth.guards.jwt.secret")
        if not secret:
            raise AuthenticationConfigurationException("JWT secret required")

        jwt_guard = JWTGuard(
            application=self.application,
            secret=secret,
            algorithm=config("auth.guards.jwt.algorithm", "HS256"),
            ttl=config("auth.guards.jwt.ttl", 3600),
            refresh_ttl=config("auth.guards.jwt.refresh_ttl", 86400),
            blacklist_enabled=config("auth.guards.jwt.blacklist_enabled", True),
            blacklist_grace_period=config("auth.guards.jwt.blacklist_grace_period", 0),
            user_model=config("auth.guards.jwt.user_model", "app.models.User"),
            header_name=config("auth.guards.jwt.header_name", "Authorization"),
            header_prefix=config("auth.guards.jwt.header_prefix", "Bearer"),
        )

        auth_manager.add_guard("jwt", jwt_guard)

    def _register_api_key_guard(self, auth_manager: Authentication) -> None:
        """Register API Key guard."""
        if not config("auth.guards.api_key"):
            return

        api_key_guard = ApiKeyGuard(
            application=self.application,
            user_model=config("auth.guards.api_key.user_model", "app.models.User"),
            api_key_field=config("auth.guards.api_key.api_key_field", "api_key"),
            header_name=config("auth.guards.api_key.header_name", "X-API-Key"),
            header_prefix=config("auth.guards.api_key.header_prefix", ""),
            rate_limit_enabled=config("auth.guards.api_key.rate_limit_enabled", False),
            rate_limit_max_attempts=config("auth.guards.api_key.rate_limit_max_attempts", 100),
            rate_limit_window=config("auth.guards.api_key.rate_limit_window", 3600),
            cache_enabled=config("auth.guards.api_key.cache_enabled", True),
            cache_ttl=config("auth.guards.api_key.cache_ttl", 3600),
        )

        auth_manager.add_guard("api_key", api_key_guard)
