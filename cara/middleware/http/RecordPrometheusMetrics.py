"""Prometheus HTTP request metrics middleware.

Instruments every request with the framework-level HTTP metrics defined on
:class:`cara.observability.MetricsBase` (``http_requests_total``,
``http_request_duration_seconds``, ``http_requests_in_flight``) — the same
registry singletons every app-level ``Metrics`` subclass shares, so apps
get request instrumentation by simply adding this middleware to their
global chain. The ``/metrics`` exposition endpoint stays app-owned; this
middleware only instruments request lifecycle.

Label cardinality is bounded by the codebase, never by the client. The
``route`` label is the matched route's template (``/api/products/@id``),
read after the downstream chain has routed the request; a request that
matched no route (404/405, or rejected before routing) counts under the
single ``unmatched`` series. Labelling from the raw path minted one
permanent Counter + Histogram child per distinct path, so random
unauthenticated 404s grew every worker without limit. ``method`` is
likewise folded to the standard verbs plus ``OTHER``.

Exception semantics: downstream exceptions are already logged by
``LogHttpRequests`` and the framework exception handler — this middleware
counts the request under the exception's own ``status_code`` (so a raised
404/422 lands in its true class, a status-less crash in 5xx) and re-raises,
never logging a third copy. Metric-emission failures degrade to a WARNING;
they never fail the request.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from cara.facades import Log
from cara.http import Request, Response
from cara.middleware.Middleware import Middleware
from cara.observability import MetricsBase, status_class

_METHODS = frozenset(("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"))
_UNMATCHED_ROUTE = "unmatched"


class RecordPrometheusMetrics(Middleware):
    """Instruments every non-``/metrics`` HTTP request with counters + duration."""

    # Paths we explicitly don't instrument — self-observation on the
    # scrape endpoint would skew counts and duration percentiles.
    _SKIP_PATHS = frozenset(("/metrics", "/api/metrics"))

    async def handle(self, request: Request, get_response: Callable) -> Response:
        if request.path in self._SKIP_PATHS:
            return await get_response(request)

        start = time.time()
        in_flight_incremented = False
        # Static log messages + ``exc_info=True`` so error trackers group
        # by exception type and stack frame, not by stringified ``{e}``.
        try:
            MetricsBase.http_requests_in_flight.inc()
            in_flight_incremented = True
        except Exception:
            Log.warning(
                "RecordPrometheusMetrics: failed to increment in-flight counter",
                exc_info=True,
            )

        status_code: int = 500
        crashed: str | None = None
        try:
            response: Response = await get_response(request)
            try:
                status_code = int(response.get_status_code())
            except Exception:
                Log.warning(
                    "RecordPrometheusMetrics: failed to read response status code",
                    exc_info=True,
                )
            return response
        except Exception as exc:
            # The conductor renders this exception into an HTTP response
            # outside the pipeline using ``exc.status_code`` (EntityNotFound
            # →404, validation→422, …). Mirror that here so a 404 counts as
            # 4xx instead of being smeared into the 5xx series; a bare
            # exception with no ``status_code`` still degrades to 500, so
            # genuine crashes keep counting as 5xx.
            status_code = int(getattr(exc, "status_code", 500) or 500)
            if status_code >= 500:
                crashed = type(exc).__name__
            raise
        finally:
            duration = time.time() - start
            try:
                # Routing ran inside ``get_response``; only now is the
                # template known (see the module docstring).
                matched = request.get_route()
                route = matched.url if matched is not None else _UNMATCHED_ROUTE
                method = request.method if request.method in _METHODS else "OTHER"
                MetricsBase.http_requests_total.labels(
                    method=method,
                    route=route,
                    status_class=status_class(status_code),
                ).inc()
                MetricsBase.http_request_duration_seconds.labels(
                    method=method,
                    route=route,
                ).observe(duration)
                if crashed is not None:
                    MetricsBase.exceptions_total.labels(
                        source="http",
                        route=route,
                        exception=crashed,
                    ).inc()
                if in_flight_incremented:
                    MetricsBase.http_requests_in_flight.dec()
            except Exception:
                Log.warning(
                    "RecordPrometheusMetrics: failed to emit request metrics",
                    exc_info=True,
                )
