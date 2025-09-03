"""
Route decorator for defining HTTP routes.

This module provides decorators for registering routes with support for
HTTP methods, middleware, and route grouping in the Cara framework.

Supported extras (passed as kwargs):
  - prefix: str                # route group prefix
  - namespace: str             # controller namespace
  - middleware: List[Any]      # list of middleware classes or callables
"""

import traceback
from functools import wraps
from typing import Any, Callable, List, Optional, TypedDict, Union


# Strongly‐typed metadata shape for clarity
class RouteMeta(TypedDict, total=False):
    methods: List[str]
    path: str
    name: Optional[str]
    prefix: Optional[str]
    namespace: Optional[str]
    middleware: Optional[List[Any]]
    handler: Callable[..., Any]


class RouteDecorator:
    """Route Decorator for HTTP methods."""

    def get(
        self,
        path: str,
        name: Optional[str] = None,
        middleware: Optional[Union[str, list]] = None,
    ):
        """GET route decorator"""
        return self._route(path, ["GET"], name, middleware)

    def post(
        self,
        path: str,
        name: Optional[str] = None,
        middleware: Optional[Union[str, list]] = None,
    ):
        """POST route decorator"""
        return self._route(path, ["POST"], name, middleware)

    def put(
        self,
        path: str,
        name: Optional[str] = None,
        middleware: Optional[Union[str, list]] = None,
    ):
        """PUT route decorator"""
        return self._route(path, ["PUT"], name, middleware)

    def patch(
        self,
        path: str,
        name: Optional[str] = None,
        middleware: Optional[Union[str, list]] = None,
    ):
        """PATCH route decorator"""
        return self._route(path, ["PATCH"], name, middleware)

    def delete(
        self,
        path: str,
        name: Optional[str] = None,
        middleware: Optional[Union[str, list]] = None,
    ):
        """DELETE route decorator"""
        return self._route(path, ["DELETE"], name, middleware)

    def options(
        self,
        path: str,
        name: Optional[str] = None,
        middleware: Optional[Union[str, list]] = None,
    ):
        """OPTIONS route decorator"""
        return self._route(path, ["OPTIONS"], name, middleware)

    def any(
        self,
        path: str,
        name: Optional[str] = None,
        middleware: Optional[Union[str, list]] = None,
    ):
        """ANY route decorator (all HTTP methods)"""
        return self._route(
            path, ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"], name, middleware
        )

    def match(
        self,
        methods: List[str],
        path: str,
        name: Optional[str] = None,
        middleware: Optional[Union[str, list]] = None,
    ):
        """MATCH route decorator for specific methods"""
        return self._route(path, methods, name, middleware)

    def _route(
        self,
        path: str,
        methods: List[str],
        name: Optional[str] = None,
        middleware: Optional[Union[str, list]] = None,
    ):
        """Internal route method"""

        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    print(f"Exception in route handler: {e}")
                    print(traceback.format_exc())
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


# Legacy RouteProvider compatibility - kept for backward compatibility
_pending_routes: List[dict] = []


def all_pending() -> List[dict]:
    """Return pending routes for RouteProvider (legacy)"""
    return _pending_routes.copy()


def clear() -> None:
    """Clear pending routes for RouteProvider (legacy)"""
    _pending_routes.clear()
