"""JWT authentication middleware.

Extends Cara's ``ShouldAuthenticate`` for routes that opt in via the
``auth`` middleware alias.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cara.authentication import Authentication
from cara.context import ExecutionContext
from cara.facades import Log
from cara.http import Request, Response
from .ShouldAuthenticate import ShouldAuthenticate
from cara.observability import set_request_tag, set_request_user


class AuthenticateUser(ShouldAuthenticate):
    """JWT authentication middleware — opt-in per-route via the 'auth' alias.

    Authenticates the user against configured guards and attaches
    ``request.set_user(...)`` + ``request._route_auth_guard`` before the
    controller runs.

    Notes
    -----
    The base class signature is fixed by Cara's middleware contract
    (``__init__(application, guards)``), so the auth manager is
    resolved lazily through the container instead of being injected
    by type. This is the one approved exception to the
    "no ``application.make`` in app code" rule, because middlewares
    *are* framework infrastructure: the container is what hands
    middleware its collaborators.

    What changed vs. the previous version: the eager ``application.make``
    in ``__init__`` was wrapped in a bare ``except Exception`` that
    silently set ``_auth_manager = None`` — which masked real binding
    failures at boot. Resolution is now lazy and any failure is
    surfaced through ``Log.error`` + a clean 401 instead of a silent
    nullptr the next handler sees as "no auth manager".
    """

    def _resolve_auth_manager(self) -> Authentication | None:
        """Resolve the ``auth`` binding from the container, lazily.

        Cached on the instance so we don't re-resolve on every request.
        Returns ``None`` when the binding is missing — the caller must
        treat that as an authentication failure (a missing auth
        manager is NOT a "skip auth" scenario).
        """
        cached = getattr(self, "_auth_manager", None)
        if cached is not None:
            return cached
        try:
            auth_manager = self.application.make("auth")
        except Exception as e:
            Log.error(
                f"AuthenticateUser: failed to resolve 'auth' binding from container: {e}",
                category="auth",
                exc_info=True,
            )
            return None
        self._auth_manager = auth_manager
        return auth_manager

    def _resolve_guards(self, auth_manager: Authentication) -> list[str]:
        """Translate the configured ``self.guards`` into concrete guard names.

        ``self.guards`` may be ``None`` (use default), explicitly
        ``["jwt"]`` (which we treat as "use whatever the auth manager
        considers the default"), or a list of explicit guard aliases.
        """
        configured: list[str] = self.guards or ["jwt"]
        if configured != ["jwt"]:
            return configured
        try:
            return [auth_manager.get_default_guard()]
        except Exception as e:
            Log.warning(
                f"AuthenticateUser: get_default_guard() raised, falling back to 'jwt': {e}",
                category="auth",
            )
            return ["jwt"]

    async def handle(self, request: Request, next_fn: Callable) -> Response:
        auth_manager = self._resolve_auth_manager()
        if auth_manager is None:
            return self.authentication_failed(
                request,
                RuntimeError("Auth manager binding is not registered"),
            )

        user: Any = None
        successful_guard: str | None = None
        last_error: Exception | None = None

        for guard_name in self._resolve_guards(auth_manager):
            try:
                guard = auth_manager.guard(guard_name)
                # ``guard.user()`` resolves the JWT subject to a row via a
                # SYNC ``User.find()`` (psycopg2). This middleware's ``handle``
                # is async + runs on the event loop, so calling it inline would
                # block the loop for the duration of that SELECT on every
                # authenticated request. Offload to the connection-safe thread
                # pool so the loop stays free (a genuine await-yield, unlike a
                # sync-caller offload which can't help).
                user = await ExecutionContext.run_in_thread(guard.user)
                if user:
                    successful_guard = guard_name
                    break
            except Exception as e:
                last_error = e
                Log.debug(
                    f"AuthenticateUser: guard '{guard_name}' failed: "
                    f"{e.__class__.__name__}: {e}",
                    category="auth",
                )
                continue

        if not user:
            return self.authentication_failed(request, last_error)

        request.set_user(user)
        request._route_auth_guard = successful_guard
        # Verified token claims for app-layer markers (impersonation etc.).
        # ``guard`` still holds the winning guard INSTANCE from the loop
        # (``successful_guard`` is only its name).
        request.jwt_claims = getattr(guard, "last_payload", None) or {}

        # Attach the resolved identity to Sentry's request-scoped scope
        # so any downstream error arrives with user_id + masked email.
        # Wrapping in try is paranoid — set_request_user already
        # swallows missing-sdk + setter failures internally — but the
        # auth middleware must never fail a request because of
        # observability.
        try:
            set_request_user(
                getattr(user, "id", None) or getattr(user, "public_id", None),
                getattr(user, "email", None),
            )
            if successful_guard:
                set_request_tag("auth_guard", successful_guard)
        except Exception as e:
            # Observability must never fail an authenticated request.
            Log.debug("AuthenticateUser: Sentry tagging skipped: %s", e)

        response = await next_fn(request)
        self._apply_default_cache_headers(response)
        return response

    @staticmethod
    def _apply_default_cache_headers(response: Any) -> None:
        """Default ``Cache-Control`` + ``Vary`` on authenticated responses.

        Controllers that need cacheability call one of the
        ``apply_*_cache`` helpers and set Cache-Control explicitly;
        when they don't, the response shipped with NO Cache-Control,
        and RFC 7234 §4.2.2 lets intermediate caches use heuristic
        freshness (up to ~24h) on stable URLs. A corporate proxy
        could then cache user A's ``/wishlist`` payload and serve
        it to user B on the same URL.

        Default behaviour for authenticated requests:
          * Cache-Control: ``private, no-store, must-revalidate`` —
            forbid intermediate storage. ``private`` is belt-and-
            braces for caches that don't honour ``no-store`` alone.
          * Vary: append ``Authorization`` (preserve existing
            tokens, never duplicate) so any cache that DOES ignore
            the directive at least partitions by bearer token
            instead of serving across users.

        Best-effort — wrapped in try/except so a response object
        without the expected ``header`` / ``headers`` interface
        doesn't break the authenticated request path. Errors are
        debug-logged and swallowed.
        """
        try:
            headers = getattr(response, "headers", None)
            if headers is None or not hasattr(response, "header"):
                return

            # Only default Cache-Control when the controller didn't
            # explicitly set one. ``apply_*_cache`` helpers already
            # set Cache-Control with the right per-endpoint TTL.
            existing_cc = (
                headers.get("Cache-Control") if hasattr(headers, "get") else None
            )
            if not existing_cc:
                response.header(
                    "Cache-Control",
                    "private, no-store, must-revalidate",
                )

            # Vary: append Authorization without dropping existing
            # tokens (e.g. ``Accept-Encoding`` from CompressResponses
            # which may have run earlier in the pipeline) and without
            # duplicating an Authorization that's already there.
            existing_vary = headers.get("Vary") if hasattr(headers, "get") else None
            tokens = [t.strip() for t in (existing_vary or "").split(",") if t.strip()]
            if not any(t.lower() == "authorization" for t in tokens):
                tokens.append("Authorization")
                response.header("Vary", ", ".join(tokens))
        except Exception as e:
            Log.debug(
                f"AuthenticateUser: default cache header apply skipped: {e}",
                category="auth",
            )
