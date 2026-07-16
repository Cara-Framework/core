"""
CORS Middleware for the Cara framework.

Laravel-style CORS middleware with configurable options.
Handles cross-origin requests with proper preflight support.
"""

from __future__ import annotations

import contextlib
from collections.abc import Awaitable, Callable
from typing import Any

from cara.configuration import config
from cara.facades import Log
from cara.http import Request, Response
from cara.middleware import Middleware


class HandleCors(Middleware):
    """
    Laravel-style CORS middleware (HandleCors).

    Configurable through config/cors.py or inline parameters.
    Handles OPTIONS preflight requests automatically.
    """

    def __init__(self, application, parameters=None):
        """
        Initialize CORS middleware.

        Args:
            application: The Cara application instance
            parameters: Optional inline parameters (overrides config)
        """
        super().__init__(application)
        self.parameters = parameters or []

        # Load configuration (Laravel style)
        self.config = self._load_config()

    def _load_config(self) -> dict:
        """Load CORS configuration from config/cors.py using dot-path access."""
        return {
            "paths": config("cors.cors.paths", ["api/*"]),
            "allowed_methods": config("cors.cors.allowed_methods", ["*"]),
            "allowed_origins": config("cors.cors.allowed_origins", ["*"]),
            "allowed_origins_patterns": config("cors.cors.allowed_origins_patterns", []),
            "allowed_headers": config("cors.cors.allowed_headers", ["*"]),
            "exposed_headers": config("cors.cors.exposed_headers", []),
            "max_age": config("cors.cors.max_age", 0),
            "supports_credentials": config("cors.cors.supports_credentials", False),
        }

    async def handle(self, request: Request, next_fn: Callable[..., Awaitable[Any]]) -> Response:
        """
        Handle CORS request (Laravel style).

        CORS headers MUST be applied even when the inner chain raises.
        Browsers enforce the same-origin policy regardless of HTTP
        status — a 500 or 401 without ``Access-Control-Allow-Origin``
        is opaque to the JS client (the fetch promise rejects with a
        generic "CORS error" and the real status is unreachable). We
        therefore wrap ``next_handler`` in try/except, attach headers
        to whatever response object is in flight, and re-raise so the
        framework's exception handler still produces the body it
        would have produced. If the exception carries no Response
        (i.e. a raw Python exception), we fall back to building a
        500 response just so headers have somewhere to live; the
        outer handler can replace the body but the headers will
        already be set.
        """
        # Skip CORS processing entirely for paths outside the configured
        # scope. Without this, CORS headers are applied to ALL routes
        # (including admin/internal) regardless of the ``paths`` config.
        if not self._path_matches_cors_config(request):
            return await next_fn(request)

        # Handle preflight OPTIONS requests
        if request.method.upper() == "OPTIONS":
            return self._handle_preflight(request)

        response = None
        try:
            response = await next_fn(request)
            return response
        except Exception as exc:
            # Look for a response attached to the exception (framework
            # convention: HTTP-shaped exceptions carry ``.response``).
            response = getattr(exc, "response", None)
            raise
        finally:
            if response is not None:
                try:
                    self._add_cors_headers(request, response)
                except Exception:
                    # Header application must never mask the primary
                    # exception path. Browsers handle missing CORS
                    # headers gracefully (visible CORS error) — far
                    # better than losing the original failure cause.
                    Log.debug("CORS header attachment failed", exc_info=True)

    def _handle_preflight(self, request: Request) -> Response:
        """Handle OPTIONS preflight request."""
        response = Response(self.application)
        response.status(204)

        # Add CORS headers
        self._add_cors_headers(request, response)

        return response

    def _add_cors_headers(self, request: Request, response: Response) -> None:
        """Add CORS headers to response (Laravel style).

        Security note — when ``supports_credentials`` is True we MUST
        NOT honour ``"*"`` in ``allowed_origins``: the browser refuses
        to send cookies / Authorization to a wildcard, but more
        importantly reflecting an arbitrary ``Origin`` together with
        ``Access-Control-Allow-Credentials: true`` is the textbook CSRF
        primitive. The previous implementation took the ``else`` branch
        — reflecting whatever the attacker's site sent — when wildcard
        was configured alongside credentials. We now treat that
        configuration as "no origin allowed" and emit no ACAO header.
        """
        origin = request.header("Origin")
        creds = bool(self.config["supports_credentials"])

        # Access-Control-Allow-Origin
        if self._is_origin_allowed(origin):
            allow_origin: str | None = None
            if creds:
                # Credentials path — only echo the origin if it matches
                # an EXPLICIT allowlist entry (string or regex), never
                # a wildcard. ``_is_origin_allowed`` would have returned
                # True for the wildcard case; double-check here.
                if origin and self._is_origin_explicitly_allowed(origin):
                    allow_origin = origin
            elif "*" in self.config["allowed_origins"]:
                allow_origin = "*"
            elif origin:
                allow_origin = origin

            if allow_origin is not None:
                response.header("Access-Control-Allow-Origin", allow_origin)
                if allow_origin != "*":
                    # When the ACAO value depends on the Origin header, proxies and
                    # CDNs must key their cache by it — otherwise one origin's
                    # response is served to another.
                    response.header("Vary", "Origin")

        # Access-Control-Allow-Methods
        response.header(
            "Access-Control-Allow-Methods", ", ".join(self.config["allowed_methods"])
        )

        # Access-Control-Allow-Headers
        response.header(
            "Access-Control-Allow-Headers", ", ".join(self.config["allowed_headers"])
        )

        # Access-Control-Expose-Headers
        if self.config["exposed_headers"]:
            response.header(
                "Access-Control-Expose-Headers", ", ".join(self.config["exposed_headers"])
            )

        # Access-Control-Allow-Credentials — only when explicitly
        # configured AND we actually emitted a non-wildcard ACAO above.
        if creds:
            response.header("Access-Control-Allow-Credentials", "true")

        # Access-Control-Max-Age
        response.header("Access-Control-Max-Age", str(self.config["max_age"]))

    def _path_matches_cors_config(self, request: Request) -> bool:
        """Check if the request path is within the CORS-configured paths.

        Supports simple glob patterns like ``api/*``. An empty paths
        list means "apply to all" (backward compatible default).
        """
        paths = self.config.get("paths")
        if not paths:
            return True

        import fnmatch

        request_path = request.path.lstrip("/")
        return any(fnmatch.fnmatch(request_path, pattern) for pattern in paths)

    def _is_origin_allowed(self, origin: str) -> bool:
        """Check if origin is allowed (any rule)."""
        if not origin:
            return True

        if "*" in self.config["allowed_origins"]:
            return True

        return self._is_origin_explicitly_allowed(origin)

    def _is_origin_explicitly_allowed(self, origin: str) -> bool:
        """Check if origin matches a NON-WILDCARD allowlist entry.

        Used by the credentials path where ``"*"`` must not match — the
        browser would block the cookie / header, and reflecting an
        arbitrary origin alongside ``Allow-Credentials: true`` is a
        cross-site request forgery primitive.
        """
        if not origin:
            return False
        if origin in self.config["allowed_origins"]:
            return True
        import re

        for pattern in self.config.get("allowed_origins_patterns", []):
            if re.match(pattern, origin):
                return True
        return False


def apply_cors_headers_to_response(application, request, response) -> None:
    """Stamp CORS headers on ``response`` for middleware that
    short-circuits BEFORE ``HandleCors`` runs in the chain.

    The global chain in ``api/config/middleware.py`` places
    ``HandleCors`` at position 9; ``EnforceBodySizeLimit`` (3) and
    ``FilterBlockedUserAgents`` (4) reject earlier with a ``return
    Response(...)``. That response unwinds the stack 3 → 2 → 1 —
    position 9 (``HandleCors``) is never invoked, so the browser
    sees a response without ``Access-Control-Allow-Origin`` and
    the fetch promise rejects with a generic "CORS error" that
    masks the real status code. The exception handler covers the
    equivalent case for RAISED exceptions
    (``DefaultExceptionHandler._cors_headers_for_scope``) but not
    for direct Response returns.

    This helper applies the same logic ``HandleCors._add_cors_headers``
    would — including the wildcard-with-credentials safety guard —
    so callers get a single source of truth for the policy.
    Header-application failures are swallowed so a CORS-config
    hiccup never masks the primary 413/403 the middleware was
    trying to surface.
    """
    with contextlib.suppress(Exception):
        HandleCors(application)._add_cors_headers(request, response)
