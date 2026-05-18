"""
Attach Request ID Middleware for the Cara framework.

This module provides middleware that attaches a unique request ID to each HTTP response for
traceability.
"""

from collections.abc import Callable

from cara.context import ExecutionContext
from cara.http import Request
from cara.middleware import Middleware


class AttachRequestID(Middleware):
    async def handle(self, request: Request, next: Callable):
        # Honour a caller-supplied ``X-Request-ID`` so multi-hop traces
        # (load balancer → API → background job) share one ID instead
        # of starting a fresh UUID at each tier. Falls through to the
        # auto-generated request_id when no header is present.
        try:
            headers = getattr(request, "headers", {}) or {}
            incoming = headers.get("X-Request-ID") or headers.get("x-request-id")
            if incoming:
                request.request_id = str(incoming)[:64]
        except Exception:
            pass

        # Bridge into ExecutionContext so downstream logs, jobs queued
        # mid-request, and any code that consults ``get_correlation_id()``
        # see the same value as the X-Request-ID header. Without this,
        # anything dispatched during the request lost the trace once it
        # hopped onto a worker.
        try:
            ExecutionContext.set_correlation_id(request.request_id)
        except Exception:
            pass

        # Tag the Sentry scope as early as possible so any error fired
        # from downstream handlers carries the request ID. The header
        # echo on the response is for the client; the tag is for ops
        # to correlate a user-reported incident to the Sentry event.
        try:
            from cara.observability.Sentry import set_request_tag

            set_request_tag("request_id", request.request_id)
        except Exception:
            pass

        response = await next(request)
        response.header("X-Request-ID", request.request_id)
        return response
