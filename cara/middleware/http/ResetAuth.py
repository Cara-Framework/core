"""
Authentication Cache Cleanup Middleware.

CRITICAL: This terminable middleware ensures all authentication-related caches
are cleared after each request to prevent user data leakage between requests.
Guards are singleton instances that persist across requests.

This middleware runs automatically after every HTTP response is sent.
"""

from cara.facades import Log
from cara.http import Request, Response
from cara.middleware import Middleware


class ResetAuth(Middleware):
    """
    CRITICAL: Terminable middleware for authentication cache cleanup.

    This middleware ensures all authentication-related caches are cleared after
    each request to prevent user data leakage between requests. Guards are
    singleton instances that persist across requests.

    Security Impact: HIGH - Prevents user authentication data from leaking
    between different requests and users.
    """

    async def handle(self, request: Request, next_fn):
        """This middleware only works as terminable, no pre-processing needed."""
        return await next_fn(request)

    async def terminate(self, request: Request, response: Response):
        """CRITICAL: Clear all authentication caches after response is sent."""
        try:
            # Clear Authentication facade cache
            auth_manager = self.application.make("auth")
            if hasattr(auth_manager, "_user"):
                auth_manager._user = None

            # Clear Request object user cache
            if hasattr(request, "_user"):
                request._user = None
            if hasattr(request, "user"):
                request.user = None

            # Clear all registered guard caches
            guard_names = getattr(auth_manager, "registered_guards", None) or [
                "api_key",
                "jwt",
            ]
            for guard_name in guard_names:
                try:
                    guard = auth_manager.guard(guard_name)
                except Exception:
                    # Unknown/unconfigured guard — nothing to reset.
                    continue

                if hasattr(guard, "_user"):
                    guard._user = None
                if hasattr(guard, "_token"):
                    guard._token = None

        except Exception as exc:
            # CRITICAL: Never let cache cleanup break the application,
            # but do log so operators can diagnose cache-leak risks.
            Log.warning(
                f"ResetAuth cleanup failed: {exc}",
                category="cara.middleware.reset_auth",
                exc_info=True,
            )
