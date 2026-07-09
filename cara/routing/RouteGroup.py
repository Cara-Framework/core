"""
Route Grouping Module.

This module provides functionality for organizing and managing groups of routes in the Cara
framework, allowing shared attributes like URL prefixes and middleware to be applied collectively to
multiple routes, making route management more efficient and maintainable.
"""

from __future__ import annotations

from cara.support.Collection import flatten


class RouteGroup:
    """Helper class for grouping routes with shared attributes."""

    def __init__(
        self,
        prefix: str = "",
        middleware: str | list[str] | None = None,
    ):
        self._prefix = prefix
        self._middleware = (
            middleware
            if isinstance(middleware, list)
            else [middleware]
            if middleware
            else []
        )

    def routes(self, *routes):
        """Add routes to the group, applying prefix and middleware.

        Idempotent per (route, group-config): grouping mutates the Route
        in place, so re-running the same group over the same objects
        (repeated discovery, hot reload, tests) used to stack the prefix
        and duplicate the group middleware. Uses the same per-route
        ledger as ``Route.group``.
        """
        from cara.routing.Route import Route

        marker = Route._group_marker(self._prefix, None, self._middleware)
        flattened = flatten(routes)
        output: list[Route] = []
        for route in flattened:
            if isinstance(route, Route):
                applied = route.__dict__.setdefault("_applied_groups", set())
                if marker in applied:
                    output.append(route)
                    continue
                applied.add(marker)

                if self._prefix:
                    route.url = Route._join_paths(self._prefix, route.url)
                    route.compiler.compile_route(route.url)
                if self._middleware:
                    # Group middleware must run BEFORE route-specific middleware
                    # (Laravel parity: group `auth` establishes `request.user`
                    # before per-route `verified` reads it).
                    existing = list(route.get_middleware())
                    route._middleware = list(self._middleware) + existing
                output.append(route)
        return output

    def middleware(self, middleware: str | list[str]) -> RouteGroup:
        """Set middleware for the group."""
        self._middleware = middleware if isinstance(middleware, list) else [middleware]
        return self
