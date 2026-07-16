"""OpenTelemetry server-span middleware with W3C context propagation."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import ExitStack
from typing import Any

from cara.http import Request, Response
from cara.middleware import Middleware
from cara.observability import Trace, normalize_metric_path


class TraceRequests(Middleware):
    """Create one bounded-cardinality server span around each HTTP request."""

    async def handle(
        self,
        request: Request,
        next_fn: Callable[..., Awaitable[Any]],
    ) -> Response:
        headers = getattr(request, "headers", {}) or {}
        carrier = {
            key: value
            for key, value in {
                "traceparent": headers.get("traceparent")
                or headers.get("Traceparent"),
                "tracestate": headers.get("tracestate")
                or headers.get("Tracestate"),
            }.items()
            if value
        }
        method = str(getattr(request, "method", "GET") or "GET").upper()
        route = normalize_metric_path(str(getattr(request, "path", "/") or "/"))
        attributes = {
            "http.request.method": method,
            "http.route": route,
        }

        with ExitStack() as stack:
            stack.enter_context(Trace.extracted_context(carrier))
            stack.enter_context(
                Trace.span(
                    f"{method} {route}",
                    attributes=attributes,
                    kind="SERVER",
                )
            )
            try:
                response: Response = await next_fn(request)
            except Exception as exc:
                Trace.record_exception(exc)
                raise

            status_code = int(response.get_status_code())
            Trace.set_attributes(**{"http.response.status_code": status_code})
            trace_id = Trace.current_trace_id()
            if trace_id:
                response.header("X-Trace-ID", trace_id)
            return response
