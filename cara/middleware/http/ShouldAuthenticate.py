"""
Base Authentication Middleware for Cara Framework

Core authentication logic with easy customization points.
Users can extend this in their app for custom authentication needs.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from cara.http import Request, Response
from cara.middleware import Middleware


class ShouldAuthenticate(Middleware):
    """Base authentication middleware with automatic parameter parsing."""

    def __init__(self, application, guards: list[str] | None = None):
        super().__init__(application)

        if guards:
            self.guards = list(guards)
            return

        # Missing auth wiring is a boot/configuration failure. Inventing a
        # ``jwt`` default here can authenticate with a guard the application
        # did not select.
        auth_manager = application.make("auth")
        self.guards = [auth_manager.get_default_guard()]

    async def handle(
        self, request: Request, next_fn: Callable[[Any], Awaitable[Any]]
    ) -> Response:
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

        # ``Request.user`` is a method (returns ``self._user``).
        # Assigning ``request.user = user`` shadows the method on the
        # instance — every subsequent ``request.user()`` then raises
        # ``TypeError: 'User' object is not callable``. Use the
        # documented setter so ``request.user()`` keeps working and
        # downstream code (controllers, facades, ResetAuth) sees one
        # canonical place where the per-request user lives.
        request.set_user(user)
        request._route_auth_guard = successful_guard

        response = await next_fn(request)
        return response

    def authentication_failed(
        self, request: Request, last_error: Exception | None = None
    ) -> Response:
        """Handle authentication failure."""
        response = Response(self.application)

        # Canonical error shape: ``{error, type, ...}`` (see
        # ``HttpException.to_dict``). Pre-fix this middleware used
        # ``{error: "Unauthorized", message: "..."}`` which broke the
        if (
            last_error
            and hasattr(last_error, "message")
            and hasattr(last_error, "status_code")
        ):
            # Guard threw a custom authentication exception
            return response.json(
                {
                    "error": last_error.message,
                    "type": "authentication_error",
                },
                last_error.status_code,
            )
        elif last_error:
            # Generic exception from guard
            return response.json(
                {
                    "error": str(last_error) or "Unauthorized",
                    "type": "authentication_error",
                },
                401,
            )
        else:
            # No specific error
            return response.json(
                {
                    "error": "Authentication required",
                    "type": "authentication_error",
                },
                401,
            )

    def should_skip_authentication(self, request: Request) -> bool:
        """Determine if authentication should be skipped for this request."""
        return False
