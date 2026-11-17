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

from cara.context import ExecutionContext
from cara.facades import Log
from cara.http import Request, Response

from .AuthenticateUser import AuthenticateUser


class AuthenticateUserOptional(AuthenticateUser):
    """JWT auth that NEVER 401s — populates request.user when possible.

    A resolved identity is still offered to :meth:`admit` before it is
    attached. A product whose tokens carry a server-side session (``sid``)
    overrides it to require that session to be live: a token whose session
    was logged out or revoked then degrades to the guest path instead of
    keeping the personalized response until the JWT expires.
    """

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
        claims: Any = None
        successful_guard: str | None = None

        for guard_name in self._resolve_guards():
            try:
                guard = auth_manager.guard(guard_name)

                # Offload the sync ``guard.user()`` (a ``User.find()`` SELECT)
                # off the event loop — same rationale as AuthenticateUser.
                # ``run_in_thread`` runs in a COPY of the context, so the
                # guard's verified claims are captured inside the thread;
                # read afterwards they would be empty.
                def _resolve(g=guard):
                    return g.user(), getattr(g, "_last_payload", None)

                user, claims = await ExecutionContext.run_in_thread(_resolve)
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

        if user:
            verified_claims = dict(claims or {})
            if await self.admit(request, user, verified_claims):
                request.set_user(user)
                request._route_auth_guard = successful_guard
                request.jwt_claims = verified_claims

        return await next_fn(request)

    async def admit(self, request: Request, user: Any, claims: dict[str, Any]) -> bool:
        """Decide whether a token-resolved ``user`` may personalize this request.

        ``claims`` are the token's verified claims. Refusing never fails the
        request — it proceeds as a guest. The default admits every identity
        the guard resolved.
        """
        return True
