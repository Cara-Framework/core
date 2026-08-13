"""Canonical route decorator implementation."""

from __future__ import annotations

from functools import wraps

from cara.facades import Log


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
                    Log.error(
                        "Exception in route handler: %s",
                        e,
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
