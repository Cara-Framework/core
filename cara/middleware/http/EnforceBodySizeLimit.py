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

from cara.exceptions import BadRequestException
from cara.facades import Log
from cara.http import BodyLimits, Request, Response
from cara.middleware.http.HandleCors import apply_cors_headers_to_response

from ..Middleware import Middleware


class EnforceBodySizeLimit(Middleware):
    """413 when ``Content-Length`` exceeds the configured ceiling."""

    async def handle(
        self,
        request: Request,
        next_fn: Callable[..., Awaitable[Response]],
    ) -> Response:
        limit = self._max_body_size()
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
        return BodyLimits.configured().body_bytes

    @staticmethod
    def _content_length(request: Request) -> int | None:
        """Read ``Content-Length`` across Cara request adapters.

        Returns ``None`` if the header is missing, blank, or non-numeric
        (the request will continue uncapped — the ASGI server handles
        the streaming case separately). Returns the parsed integer
        otherwise; the comparison against the limit happens in the
        caller.
        """
        raw: object = None
        getter = getattr(request, "header", None)
        if callable(getter):
            raw = getter("Content-Length")
        if raw is None:
            headers = getattr(request, "headers", None)
            if headers is not None and hasattr(headers, "get"):
                raw = headers.get("Content-Length")
        if raw is None:
            return None
        if not isinstance(raw, str) or not raw.strip():
            raise BadRequestException("Invalid Content-Length header")
        try:
            length = int(raw.strip())
        except ValueError as exc:
            raise BadRequestException("Invalid Content-Length header") from exc
        if length < 0:
            raise BadRequestException("Invalid Content-Length header")
        return length
