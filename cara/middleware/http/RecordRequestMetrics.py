"""Request Metrics Middleware — records duration, status, query count; logs slow requests."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from cara.eloquent import DatabaseManager
from cara.facades import Log
from cara.http import Request, Response
from cara.middleware import Middleware


class RecordRequestMetrics(Middleware):
    """Middleware to collect and log request performance metrics."""

    SLOW_REQUEST_THRESHOLD_MS = 500

    async def handle(
        self, request: Request, get_response: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if request.path and request.path.startswith("/health"):
            return await get_response(request)

        start_time = time.time()
        request.start_time = start_time

        response: Response = await get_response(request)

        elapsed_ms = int((time.time() - start_time) * 1000)

        if elapsed_ms > self.SLOW_REQUEST_THRESHOLD_MS:
            Log.warning(
                f"Slow request: {request.method} {request.path} took {elapsed_ms}ms",
                category="app.http.requests",
            )

        return response

    def _get_query_count(self) -> int:
        try:
            manager = DatabaseManager.get_instance()
            if manager and hasattr(manager, "_query_log"):
                return len(getattr(manager, "_query_log", []) or [])
        except Exception as e:
            Log.debug(
                f"RequestMetrics query-count probe failed: {e.__class__.__name__}: {e}",
                category="app.metrics",
            )
        return 0
