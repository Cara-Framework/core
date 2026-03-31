"""
Authentication Manager for Cara Framework.

Manages authentication guards and user sessions.
"""

from typing import Any, Dict, Optional

from cara.authentication.contracts import Authenticatable


class Authentication:
    """
    Authentication manager that handles multiple guards.
    """

    def __init__(self, application, default_guard: str = "jwt"):
        self.application = application
        self.default_guard = default_guard
        self.guards: Dict[str, Any] = {}
        self._user: Optional[Authenticatable] = None

    def add_guard(self, name: str, guard) -> None:
        """Add a guard to the authentication manager."""
        self.guards[name] = guard

    def guard(self, name: str = None):
        """Get a guard by name or detect from request."""
        if name:
            # Explicit guard name provided
            guard_name = name
        else:
            # Auto-detect guard from request headers
            guard_name = self._detect_guard_from_request() or self.default_guard

        if guard_name not in self.guards:
            raise ValueError(f"Guard '{guard_name}' not found")
        return self.guards[guard_name]

    def _detect_guard_from_request(self) -> Optional[str]:
        """Detect which guard should be used from route middleware, not headers."""
        try:
            from cara.http.request.context import current_request

            request = current_request.get()

            # Get guard from route middleware (secure way)
            route_guard = getattr(request, "_route_auth_guard", None)
            if route_guard:
                return route_guard

            return None
        except Exception:
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

    def user(self) -> Optional[Any]:
        """Get the currently authenticated user."""
        if self._user is not None:
            return self._user

        # Use auto-detected guard from request headers
        user = self.guard().user()
        if user:
            self._user = user

        return user

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

    def using(self, guard_name: str):
        """Use a specific guard for the next authentication action."""
        return self.guard(guard_name)
