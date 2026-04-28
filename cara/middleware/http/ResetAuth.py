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

            # Clear Request object user cache. ``request.user`` is a
            # METHOD (returns ``self._user``); never assign to it —
            # doing so used to shadow the method with ``None`` and
            # break every subsequent ``request.user()`` call within the
            # same Request instance lifetime. We clear ``_user`` and
            # use the canonical ``set_user`` setter where available.
            setter = getattr(request, "set_user", None)
            if callable(setter):
                try:
                    setter(None)
                except Exception:
                    pass
            elif hasattr(request, "_user"):
                request._user = None

            # Clear all registered guard caches — query the actual guard
            # dict so new guards are always covered automatically.
            guard_names = list(
                getattr(auth_manager, "guards", {}).keys()
            ) or ["jwt"]
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
