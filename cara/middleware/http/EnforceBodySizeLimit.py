"""Reject oversized request bodies before they reach the handler.

Defence-in-depth against DoS / accidental large-payload bugs. The
ASGI runner buffers ``Content-Length`` bytes into worker memory
before the route handler sees them; without an upstream cap a single
``curl --data-binary @100mb.bin`` per worker drains RAM and stalls
every concurrent request on the same process.

Policy:
  * Read ``config("server.max_body_size")`` at call time so an ops
    bump takes effect on the next request (no redeploy required).
  * Inspect the ``Content-Length`` header up-front. If absent (chunked
    transfer encoding) we let the request through — the streaming
    body has its own backpressure path and a hard cap there belongs
    at the ASGI server level (uvicorn / hypercorn), not here.
  * 413 ``Payload Too Large`` is the canonical status for body-size
    rejection. The body follows the canonical middleware envelope —
    ``{"error", "type", "max_bytes", "content_length"}`` — same shape
    ``ThrottleRequests`` / ``CheckMaintenanceMode`` / ``CanPerform``
    / ``ShouldAuthenticate`` emit. Pre-fix this middleware was the
    outlier: it used a non-canonical validation-error shape
    (``{"errors": {"__all__": [...]}}``) which forced client /
    SDK consumers to special-case the 413 path because the canonical
    ``type`` discriminator wasn't present.

Mounted globally (see ``config/middleware.py``). Per-endpoint
overrides aren't supported yet — when a new endpoint legitimately
needs a larger ceiling, ops bumps ``SERVER_MAX_BODY_SIZE`` site-wide.
A future enhancement can read a per-route override from the
controller's docstring if the asymmetry gets painful.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from cara.configuration import config
from cara.facades import Log
from cara.http import Request, Response
from cara.middleware import Middleware
from cara.middleware.http.HandleCors import apply_cors_headers_to_response

# 10 MiB — matches ``config("server.max_body_size")``'s default.
# Stays in sync via the ``config(...)`` call at request time; this
# constant is only the fallback when the config key is missing or
# coerces to an invalid value (operator typo).
_DEFAULT_MAX_BODY_SIZE = 10 * 1024 * 1024


class EnforceBodySizeLimit(Middleware):
    """413 when ``Content-Length`` exceeds the configured ceiling."""

    async def handle(
        self,
        request: Request,
        next_fn: Callable[..., Awaitable[Response]],
    ) -> Response:
        limit = self._max_body_size()
        if limit <= 0:
            # ``<= 0`` is a documented "disable the check" knob —
            # operators set ``SERVER_MAX_BODY_SIZE=0`` to bypass
            # this middleware while keeping it in the chain. Same
            # idea as the empty-list disabler used by RequireAdminIp
            # / FilterBlockedUserAgents.
            return await next_fn(request)

        length = self._content_length(request)
        if length is None:
            # Chunked transfer (no ``Content-Length``) — let it
            # through; the ASGI server's own buffer limit is the
            # final defence for those.
            return await next_fn(request)

        if length > limit:
            Log.warning(
                f"EnforceBodySizeLimit: rejecting payload of {length} bytes "
                f"(limit={limit})",
                category="security.body_size",
            )
            response = Response(self.application).json(
                {
                    "error": f"Request body too large (max {limit} bytes)",
                    "type": "payload_too_large",
                    "max_bytes": int(limit),
                    "content_length": int(length),
                },
                413,
            )
            # This middleware sits at position 3 in the global chain;
            # ``HandleCors`` is at position 9 and never runs when we
            # return here. Stamp CORS headers explicitly so the browser
            # can read the 413 status instead of seeing an opaque
            # "CORS error" with no body. The helper applies the same
            # wildcard-with-credentials guard ``HandleCors`` uses.
            apply_cors_headers_to_response(self.application, request, response)
            return response
        return await next_fn(request)

    @staticmethod
    def _max_body_size() -> int:
        try:
            return int(config("server.max_body_size", _DEFAULT_MAX_BODY_SIZE))
        except (TypeError, ValueError):
            return _DEFAULT_MAX_BODY_SIZE

    @staticmethod
    def _content_length(request: Request) -> int | None:
        """Read ``Content-Length`` across Cara request adapters.

        Returns ``None`` if the header is missing, blank, or non-numeric
        (the request will continue uncapped — the ASGI server handles
        the streaming case separately). Returns the parsed integer
        otherwise; the comparison against the limit happens in the
        caller.
        """
        raw: str | None = None
        try:
            getter = getattr(request, "header", None)
            if callable(getter):
                raw = getter("Content-Length") or getter("content-length")
        except Exception as e:
            # Header accessor missing or crashed on this transport →
            # treat as "no Content-Length", let the request through
            # (the streaming-body / chunked-encoding fallback below
            # owns that case). Convention check requires the swallow
            # be logged; ``debug`` keeps prod logs quiet.
            Log.debug(
                f"EnforceBodySizeLimit: header(...) accessor failed: {e}",
                category="security.body_size",
            )
            raw = None
        if raw is None:
            try:
                headers = getattr(request, "headers", None)
                if headers is not None and hasattr(headers, "get"):
                    raw = headers.get("Content-Length") or headers.get("content-length")
            except Exception as e:
                Log.debug(
                    f"EnforceBodySizeLimit: headers.get(...) accessor failed: {e}",
                    category="security.body_size",
                )
                raw = None
        if not isinstance(raw, str) or not raw.strip():
            return None
        try:
            length = int(raw.strip())
        except (TypeError, ValueError):
            return None
        return length if length >= 0 else None
