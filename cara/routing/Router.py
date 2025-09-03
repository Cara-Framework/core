"""
Core Router class for HTTP and WebSocket traffic.

Implements Laravel-style route lookup, including OPTIONS preflight for HTTP and WS dispatch.
"""

from typing import Any, Dict, List, Optional

from cara.exceptions import (
    MethodNotAllowedException,
    RouteNotFoundException,
)
from cara.support.Collection import flatten

from .Route import Route

# Include "WS" so WebSocket routes are bucketed
HTTP_METHODS = [
    "GET",
    "HEAD",
    "POST",
    "PUT",
    "PATCH",
    "DELETE",
    "OPTIONS",
    "WS",
]


class Router:
    """Router for mapping incoming ASGI scopes to Route instances."""

    def __init__(
        self,
        application: Any,
        *routes: Route,
        module_location: Optional[str] = None,
    ):
        self.application = application
        self.routes: List[Route] = flatten(routes)
        self.controller_locations = module_location

        # Bucket routes by method for efficient lookup
        self.routes_by_method: Dict[str, List[Route]] = {m: [] for m in HTTP_METHODS}
        for route in self.routes:
            for m in route.request_method:
                key = m.upper()
                if key in self.routes_by_method:
                    self.routes_by_method[key].append(route)

    def add(self, *routes: Route) -> "Router":
        for route in routes:
            for r in route if isinstance(route, list) else [route]:
                self.routes.append(r)
                for m in r.request_method:
                    key = m.upper()
                    if key in self.routes_by_method:
                        self.routes_by_method[key].append(r)
        return self

    def find(self, path: str, request_method: str) -> Route:
        """
        Find a matching route by path and method.

        For HTTP OPTIONS, automatically generates preflight if needed.
        """
        method = request_method.upper()
        candidates = self.routes_by_method.get(method, [])

        for route in candidates:
            if route.matches(path, request_method):
                return route

        # If no direct match, check for method-not-allowed
        allowed = self.get_allowed_methods(path)
        if allowed:
            if method == "OPTIONS":
                return self._create_preflight_route(path, allowed)
            raise MethodNotAllowedException(
                f"Method {request_method} not allowed. Allowed methods: {allowed}"
            )

        raise RouteNotFoundException(f"No route matches path '{path}'")

    def get_allowed_methods(self, path: str) -> List[str]:
        """Return all methods allowed for given path."""
        allowed: List[str] = []
        for m, bucket in self.routes_by_method.items():
            for route in bucket:
                if route.matches(path, m.lower()):
                    allowed.append(m)
                    break
        return allowed

    def _create_preflight_route(self, path: str, allowed_methods: List[str]) -> Route:
        """Generate an OPTIONS route for CORS preflight."""

        class PreflightController:
            def handle(self, request, response):
                response.with_headers(
                    {
                        "Allow": ", ".join(allowed_methods),
                        "Access-Control-Allow-Methods": ", ".join(allowed_methods),
                    }
                ).status(204)
                return response

        return Route(path, PreflightController().handle, ["options"])
