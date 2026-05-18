"""
Route decorator for defining HTTP routes.

This module provides decorators for registering routes with support for
HTTP methods, middleware, and route grouping in the Cara framework.

Supported extras (passed as kwargs):
  - prefix: str                # route group prefix
  - namespace: str             # controller namespace
  - middleware: List[Any]      # list of middleware classes or callables
"""

from collections.abc import Callable
from functools import wraps
from typing import Any, TypedDict


# Strongly‐typed metadata shape for clarity
class RouteMeta(TypedDict, total=False):
    methods: list[str]
    path: str
    name: str | None
    prefix: str | None
    namespace: str | None
    middleware: list[Any] | None
    handler: Callable[..., Any]


class RouteDecorator:
    """Route Decorator for HTTP methods."""

    def get(
        self,
        path: str,
        name: str | None = None,
        middleware: str | list | None = None,
    ):
        """GET route decorator"""
        return self._route(path, ["GET"], name, middleware)

    def post(
        self,
        path: str,
        name: str | None = None,
        middleware: str | list | None = None,
    ):
        """POST route decorator"""
        return self._route(path, ["POST"], name, middleware)

    def put(
        self,
        path: str,
        name: str | None = None,
        middleware: str | list | None = None,
    ):
        """PUT route decorator"""
        return self._route(path, ["PUT"], name, middleware)

    def patch(
        self,
        path: str,
        name: str | None = None,
        middleware: str | list | None = None,
    ):
        """PATCH route decorator"""
        return self._route(path, ["PATCH"], name, middleware)

    def delete(
        self,
        path: str,
        name: str | None = None,
        middleware: str | list | None = None,
    ):
        """DELETE route decorator"""
        return self._route(path, ["DELETE"], name, middleware)

    def options(
        self,
        path: str,
        name: str | None = None,
        middleware: str | list | None = None,
    ):
        """OPTIONS route decorator"""
        return self._route(path, ["OPTIONS"], name, middleware)

    def any(
        self,
        path: str,
        name: str | None = None,
        middleware: str | list | None = None,
    ):
        """ANY route decorator (all HTTP methods)"""
        return self._route(
            path, ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"], name, middleware
        )

    def match(
        self,
        methods: list[str],
        path: str,
        name: str | None = None,
        middleware: str | list | None = None,
    ):
        """MATCH route decorator for specific methods"""
        return self._route(path, methods, name, middleware)

    def _route(
        self,
        path: str,
        methods: list[str],
        name: str | None = None,
        middleware: str | list | None = None,
    ):
        """Internal route method"""

        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    from cara.facades import Log

                    Log.error(
                        f"Exception in route handler: {e}",
                        category="cara.routing",
                        exc_info=True,
                    )
                    raise

            # Store route metadata for registration
            wrapper.__route__ = {
                "methods": methods,
                "path": path,
                "name": name,
                "middleware": middleware,
            }

            return wrapper

        return decorator


# Global decorator instance
route = RouteDecorator()


_pending_routes: list[dict] = []


def all_pending() -> list[dict]:
    """Return pending routes collected from @route decorators."""
    return _pending_routes.copy()


def clear() -> None:
    """Clear pending routes after they have been loaded."""
    _pending_routes.clear()
