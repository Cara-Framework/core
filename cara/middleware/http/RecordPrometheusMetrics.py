"""Prometheus HTTP request metrics middleware.

Instruments every request with the framework-level HTTP metrics defined on
:class:`cara.observability.MetricsBase` (``http_requests_total``,
``http_request_duration_seconds``, ``http_requests_in_flight``) — the same
registry singletons every app-level ``Metrics`` subclass shares, so apps
get request instrumentation by simply adding this middleware to their
global chain. The ``/metrics`` exposition endpoint stays app-owned; this
middleware only instruments request lifecycle.

Label cardinality is kept low by :func:`cara.observability.normalize_metric_path`,
which collapses numeric/UUID/ULID segments into placeholders so
``/api/products/123`` and ``/api/products/456`` share a single time series.

Exception semantics: downstream exceptions are already logged by
``LogHttpRequests`` and the framework exception handler — this middleware
only counts the 500 and re-raises, never logs a third copy. Metric-emission
failures degrade to a WARNING; they never fail the request.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from cara.facades import Log
from cara.http import Request, Response
from cara.middleware import Middleware
from cara.observability import MetricsBase, normalize_metric_path, status_class


class RecordPrometheusMetrics(Middleware):
    """Instruments every non-``/metrics`` HTTP request with counters + duration."""

    # Paths we explicitly don't instrument — self-observation on the
    # scrape endpoint would skew counts and duration percentiles.
    _SKIP_PATHS = frozenset(("/metrics", "/api/metrics"))

    async def handle(self, request: Request, get_response: Callable) -> Response:
        if request.path in self._SKIP_PATHS:
            return await get_response(request)

        route = normalize_metric_path(request.path)
        method = request.method

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
        except Exception:
            status_code = 500
            raise
        finally:
            duration = time.time() - start
            try:
                MetricsBase.http_requests_total.labels(
                    method=method,
                    route=route,
                    status_class=status_class(status_code),
                ).inc()
                MetricsBase.http_request_duration_seconds.labels(
                    method=method,
                    route=route,
                ).observe(duration)
                if in_flight_incremented:
                    MetricsBase.http_requests_in_flight.dec()
            except Exception:
                Log.warning(
                    "RecordPrometheusMetrics: failed to emit request metrics",
                    exc_info=True,
                )
