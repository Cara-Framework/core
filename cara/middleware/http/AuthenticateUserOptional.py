"""Optional JWT authentication middleware.

Sits in front of routes that personalize their response when a user
*happens* to be signed in but stay reachable for guests too. The
controller reads ``request.user()`` (e.g. via an ``optional_user_id``
helper); without a middleware populating that it always returns
``None`` and the personalized branch never fires for logged-in
callers. The ``auth`` middleware 401s on absence; optional surfaces
need a softer variant — decode the JWT if present, skip silently if
not.

This middleware does exactly that: try each configured guard, set
``request.user`` on success, and call ``next_fn`` either way. No
401, no log noise on missing tokens, no cache headers (the optional
surfaces ship their own cache directives via
``apply_public_swr_cache`` or stay un-cached).

Typically wired as the ``auth.optional`` middleware alias.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cara.facades import Log
from cara.http import Request, Response

from .AuthenticateUser import AuthenticateUser


class AuthenticateUserOptional(AuthenticateUser):
    """JWT auth that NEVER 401s — populates request.user when possible."""

    async def handle(self, request: Request, next_fn: Callable) -> Response:
        # Container resolution failure should not block a public-with-
        # personalization route; degrade to the guest path silently
        # (the auth manager being absent is rare enough that the
        # AuthenticateUser equivalent already logs it once, so we
        # don't re-log here).
        auth_manager = self._resolve_auth_manager()
        if auth_manager is None:
            return await next_fn(request)

        user: Any = None
        successful_guard: str | None = None

        for guard_name in self._resolve_guards(auth_manager):
            try:
                guard = auth_manager.guard(guard_name)
                user = guard.user()
                if user:
                    successful_guard = guard_name
                    break
            except Exception as e:
                # Optional auth swallows guard errors — the most
                # common case is "no Authorization header" which is
                # the whole point. Debug-log so a real misconfig
                # still surfaces under verbose logging without
                # spamming production.
                Log.debug(
                    f"AuthenticateUserOptional: guard '{guard_name}' "
                    f"skipped ({e.__class__.__name__}: {e})",
                    category="auth",
                )
                continue

        if user is not None:
            request.set_user(user)
            request._route_auth_guard = successful_guard

        return await next_fn(request)
