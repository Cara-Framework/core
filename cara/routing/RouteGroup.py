"""
Route Grouping Module.

This module provides functionality for organizing and managing groups of routes in the Cara
framework, allowing shared attributes like URL prefixes and middleware to be applied collectively to
multiple routes, making route management more efficient and maintainable.
"""

from typing import List, Union

from cara.support.Collection import flatten


class RouteGroup:
    """Helper class for grouping routes with shared attributes."""

    def __init__(
        self,
        prefix: str = "",
        middleware: Union[str, List[str]] = None,
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
        """Add routes to the group, applying prefix and middleware."""
        from cara.routing import Route

        flattened = flatten(routes)
        output: List[Route] = []
        for route in flattened:
            if isinstance(route, Route):
                if self._prefix:
                    route.url = Route._join_paths(self._prefix, route.url)
                    route.compiler.compile_route(route.url)
                if self._middleware:
                    route.middleware(self._middleware)
                output.append(route)
        return output

    def middleware(self, middleware: Union[str, List[str]]) -> "RouteGroup":
        """Set middleware for the group."""
        self._middleware = middleware if isinstance(middleware, list) else [middleware]
        return self
