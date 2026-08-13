"""
CORS Middleware for the Cara framework.

Laravel-style CORS middleware with configurable options.
Handles cross-origin requests with proper preflight support.

The POLICY itself is not here — it lives in :mod:`cara.middleware.http.Cors`
and this module reads it (§5: read the SSOT, never restate it). Three sites
stamp CORS headers: this middleware on the success path,
:func:`apply_cors_headers_to_response` for middleware that short-circuits
before this one runs, and ``DefaultExceptionHandler._cors_headers_for_scope``
for raised exceptions. Each one that restated the policy drifted OPEN — the
exception path granted ``Access-Control-Allow-Origin: *`` on routes the
operator had deliberately excluded from ``cors.cors.paths``, turning any
provokable error into a cross-origin read of a non-CORS endpoint.
"""

from __future__ import annotations

import contextlib
from collections.abc import Awaitable, Callable
from typing import Any

from cara.facades import Log
from cara.http import Request, Response
from cara.middleware.http.Cors import (
    load_cors_policy,
    path_in_cors_scope,
    resolve_allow_origin,
)
from cara.middleware.Middleware import Middleware


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
        """Read the policy from the shared module — one key list, one default set.

        This method used to carry its own ``config("cors.cors.<key>", <default>)``
        block, which meant the framework had TWO declarations of what the CORS
        keys are and what they default to. A guard test AST-parsed this body and
        compared the literals against ``Cors.CORS_DEFAULTS`` to catch the drift,
        which is the tell that a copy exists: you do not need a test to prove two
        things are equal when there is only one of them.
        """
        return load_cors_policy()

    async def handle(
        self, request: Request, next_fn: Callable[..., Awaitable[Any]]
    ) -> Response:
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
        was configured alongside credentials. That rule is now
        :func:`~cara.middleware.http.Cors.resolve_allow_origin`, shared
        with the exception handler, because a CSRF guard that exists in
        two copies is a CSRF guard that will be fixed in one of them.
        """
        origin = request.header("Origin")
        creds = bool(self.config["supports_credentials"])

        # Access-Control-Allow-Origin
        allow_origin = resolve_allow_origin(origin, self.config)
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
        #
        # The second half of that sentence was a comment, not a condition:
        # the header went out on ``if creds`` alone. When
        # ``resolve_allow_origin`` fails closed — credentials enabled, this
        # origin not on the allowlist — the response then carried
        # ``Allow-Credentials: true`` with no ``Allow-Origin`` at all. No
        # browser grants anything on that pair, so nothing was exploitable;
        # what it did was advertise to any origin that this endpoint takes
        # credentials, and leave the only written statement of the rule
        # disagreeing with the code enforcing it. A security comment that
        # over-describes its code is the kind that gets trusted in the next
        # review instead of re-read.
        if creds and allow_origin is not None and allow_origin != "*":
            response.header("Access-Control-Allow-Credentials", "true")

        # Access-Control-Max-Age
        response.header("Access-Control-Max-Age", str(self.config["max_age"]))

    def _path_matches_cors_config(self, request: Request) -> bool:
        """Whether this request's path is inside ``cors.cors.paths``.

        Delegates to :func:`~cara.middleware.http.Cors.path_in_cors_scope`,
        which takes a plain string so every site that has to answer this
        question — this middleware, the early-reject helper below, and the
        exception handler — asks the SAME predicate. The copy that used to
        live here was reachable only through a ``Request``, which is exactly
        why the other two sites never consulted it and the error path ended
        up unscoped.
        """
        return path_in_cors_scope(request.path, self.config.get("paths"))


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

    It applies the same logic ``HandleCors._add_cors_headers`` would —
    INCLUDING the ``cors.cors.paths`` gate. That gate was missing, which
    made this path strictly more permissive than the success path it
    stands in for: with the shipped default ``["api/*"]``, a request to
    ``/internal/metrics`` got no ``Access-Control-Allow-Origin`` when it
    succeeded, and ``Access-Control-Allow-Origin: *`` when
    ``EnforceBodySizeLimit`` or ``FilterBlockedUserAgents`` rejected it
    early. An attacker's page could therefore read the status and body of
    a deliberately-non-CORS endpoint cross-origin simply by making the
    request oversized or by sending a blocked user agent. The same
    inversion, on the raised-exception path, is what
    ``DefaultExceptionHandler._cors_headers_for_scope`` fixed.

    Failures are swallowed so a CORS-config hiccup never masks the
    primary 413/403 the middleware was trying to surface — and because a
    policy (or a path) we could not read is not a policy that allows
    everyone, swallowing here means emitting NOTHING (§9, fail closed).
    Note the consequence for callers: a request object without a ``path``
    attribute now yields no CORS headers rather than wildcard ones.
    """
    with contextlib.suppress(Exception):
        middleware = HandleCors(application)
        if not middleware._path_matches_cors_config(request):
            return
        middleware._add_cors_headers(request, response)
