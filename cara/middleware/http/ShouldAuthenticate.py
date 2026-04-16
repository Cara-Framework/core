"""
Base Authentication Middleware for Cara Framework

Core authentication logic with easy customization points.
Users can extend this in their app for custom authentication needs.
"""

from typing import Callable, List, Optional

from cara.http import Request, Response
from cara.middleware import Middleware


class ShouldAuthenticate(Middleware):
    """Base authentication middleware with automatic parameter parsing."""

    def __init__(self, application, guards: Optional[List[str]] = None):
        super().__init__(application)

        if guards:
            self.guards = list(guards)
            return

        # No guards specified — resolve the configured default guard from the
        # auth manager. Fall back to ["jwt"] only if auth is not wired up.
        try:
            auth_manager = application.make("auth")
            self.guards = [auth_manager.get_default_guard()]
        except Exception:
            self.guards = ["jwt"]

    async def handle(self, request: Request, next_fn: Callable) -> Response:
        """Handle authentication check."""
        # Check if authentication should be skipped
        if self.should_skip_authentication(request):
            return await next_fn(request)

        # Try to authenticate with each guard until one succeeds
        user = None
        successful_guard = None
        last_error = None

        for guard_name in self.guards:
            try:
                auth_manager = self.application.make("auth")
                guard = auth_manager.guard(guard_name)

                # Let guard handle its own authentication and error messages
                user = guard.user()
                if user:
                    successful_guard = guard_name
                    break

            except Exception as e:
                last_error = e
                continue

        if not user:
            return self.authentication_failed(request, last_error)

        # Set authenticated user and guard info for Auth facade
        request.user = user
        request._route_auth_guard = successful_guard

        response = await next_fn(request)
        return response

    def authentication_failed(
        self, request: Request, last_error: Optional[Exception] = None
    ) -> Response:
        """Handle authentication failure."""
        response = Response(self.application)

        if (
            last_error
            and hasattr(last_error, "message")
            and hasattr(last_error, "status_code")
        ):
            # Guard threw a custom authentication exception
            return response.json(
                {"error": "Unauthorized", "message": last_error.message},
                last_error.status_code,
            )
        elif last_error:
            # Generic exception from guard
            return response.json(
                {"error": "Unauthorized", "message": str(last_error)}, 401
            )
        else:
            # No specific error
            return response.json(
                {"error": "Unauthorized", "message": "Authentication required"}, 401
            )

    def should_skip_authentication(self, request: Request) -> bool:
        """Determine if authentication should be skipped for this request."""
        return False
