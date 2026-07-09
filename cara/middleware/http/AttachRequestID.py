"""
Attach Request ID Middleware for the Cara framework.

This module provides middleware that attaches a unique request ID to each HTTP response for
traceability.
"""

from __future__ import annotations

import contextlib
from collections.abc import Awaitable, Callable
from typing import Any

from cara.context import ExecutionContext
from cara.http import Request, Response
from cara.middleware import Middleware


class AttachRequestID(Middleware):
    async def handle(self, request: Request, next_fn: Callable[..., Awaitable[Any]]) -> Response:
        # Honour a caller-supplied ``X-Request-ID`` so multi-hop traces
        # (load balancer → API → background job) share one ID instead
        # of starting a fresh UUID at each tier. Falls through to the
        # auto-generated request_id when no header is present.
        try:
            headers = getattr(request, "headers", {}) or {}
            incoming = headers.get("X-Request-ID") or headers.get("x-request-id")
            if incoming:
                request.request_id = str(incoming)[:64]
        except (AttributeError, TypeError):
            pass

        with contextlib.suppress(AttributeError, TypeError):
            ExecutionContext.set_correlation_id(request.request_id)

        try:
            from cara.observability.Sentry import set_request_tag

            set_request_tag("request_id", request.request_id)
        except ImportError:
            pass
        except (AttributeError, TypeError):
            pass

        response = await next_fn(request)
        response.header("X-Request-ID", request.request_id)
        return response
