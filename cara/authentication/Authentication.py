"""
Authentication Manager for Cara Framework.

Manages authentication guards and user sessions.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Any

from cara.authentication.contracts import Authenticatable
from cara.exceptions import ConfigurationException

# The Authentication manager is bound to the IoC container as a
# process-wide singleton (see ``AuthenticationProvider.register``).
# Storing the resolved user on ``self._user`` therefore leaked the
# first-resolved identity to every subsequent request that hit any
# ``auth_manager.user()`` / ``auth_manager.login()`` call before its
# own guard could re-authenticate — the exact cross-request identity
# leak that ``JWTGuard`` was already fixed for via a ContextVar.
# This mirrors that fix one layer up so the wrapper API is safe even
# if a future caller routes through ``auth_manager.user()`` instead
# of going to ``auth_manager.guard("jwt").user()`` directly.
_REQUEST_USER: ContextVar[Any] = ContextVar("auth_manager_user", default=None)

_logger = logging.getLogger("cara.auth")


class Authentication:
    """
    Authentication manager that handles multiple guards.
    """

    def __init__(self, application, default_guard: str = "jwt"):
        self.application = application
        self.default_guard = default_guard
        self.guards: dict[str, Any] = {}

    @property
    def _user(self) -> Any | None:
        return _REQUEST_USER.get()

    @_user.setter
    def _user(self, value: Any | None) -> None:
        _REQUEST_USER.set(value)

    def add_guard(self, name: str, guard) -> None:
        """Add a guard to the authentication manager."""
        self.guards[name] = guard

    def guard(self, name: str | None = None):
        """Get a guard by name or detect from request."""
        if name:
            # Explicit guard name provided
            guard_name = name
        else:
            # Auto-detect guard from request headers
            guard_name = self._detect_guard_from_request() or self.default_guard

        if guard_name not in self.guards:
            raise ConfigurationException(f"Guard '{guard_name}' not found")
        return self.guards[guard_name]

    def _detect_guard_from_request(self) -> str | None:
        """Detect which guard should be used from route middleware, not headers."""
        try:
            from cara.http.request.Context import current_request

            request = current_request.get()

            # Get guard from route middleware (secure way)
            route_guard = getattr(request, "_route_auth_guard", None)
            if route_guard:
                return route_guard

            return None
        except Exception:
            _logger.debug("guard detection failed", exc_info=True)
            return None

    def get_default_guard(self) -> str:
        """Get the default guard name."""
        return self.default_guard

    def check(self) -> bool:
        """Check if the current request is authenticated."""
        return self.guard().check()

    def guest(self) -> bool:
        """Check if the current request is a guest."""
        return not self.check()

    def user(self) -> Any | None:
        """Get the currently authenticated user."""
        if self._user is not None:
            return self._user

        # Use auto-detected guard from request headers
        user = self.guard().user()
        if user:
            self._user = user

        return user

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
        return self.guard().attempt(credentials)

    def logout(self) -> None:
        """Log the user out."""
        self.guard().logout()
        self._user = None

    def login(self, user: Authenticatable) -> str:
        """Log a user in."""
        if not isinstance(user, Authenticatable):
            raise TypeError("User must implement Authenticatable")

        token = self.guard().login(user)
        self._user = user
        return token

    def validate_token(self, token: str) -> bool:
        """Validate a token."""
        return self.guard().validate_token(token)
