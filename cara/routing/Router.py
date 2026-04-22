"""
Core Router class for HTTP and WebSocket traffic.

Implements Laravel-style route lookup, including OPTIONS preflight for HTTP and WS dispatch.
Supports route model binding for automatic model resolution.
"""

from typing import Any, Callable, Dict, List, Optional, Type

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
    ) -> None:
        self.application = application
        self.routes: List[Route] = flatten(routes)
        self.controller_locations = module_location
        self._model_bindings: Dict[str, Callable[[Any], Any]] = {}

        # Bucket routes by method for efficient lookup
        self.routes_by_method: Dict[str, List[Route]] = {m: [] for m in HTTP_METHODS}
        for route in self.routes:
            for m in route.request_method:
                key = m.upper()
                if key in self.routes_by_method:
                    self.routes_by_method[key].append(route)

    def add(self, *routes: Route) -> "Router":
        """Add routes to the router."""
        for route in routes:
            for r in route if isinstance(route, list) else [route]:
                self.routes.append(r)
                for m in r.request_method:
                    key = m.upper()
                    if key in self.routes_by_method:
                        self.routes_by_method[key].append(r)
        return self

    # ------------------------------------------------------------------
    # Named-route URL generation (Laravel: ``route('users.show', {id:1})``)
    # ------------------------------------------------------------------
    def find_by_name(self, name: str) -> Optional[Route]:
        """Return the Route registered under ``name`` or None."""
        for route in self.routes:
            if route.get_name() == name:
                return route
        return None

    def url(self, name: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Generate the URL for a named route.

        Substitutes ``@param`` placeholders (with optional ``:type`` suffix)
        in the route URL using ``params``. Unknown placeholders are left
        in place so the caller sees the mismatch.
        """
        import re as _re

        route = self.find_by_name(name)
        if route is None:
            raise RouteNotFoundException(
                f"Route named '{name}' is not registered."
            )
        params = params or {}
        url = route.url

        def _replace(match):
            key = match.group(1)
            if key in params:
                return str(params[key])
            return match.group(0)

        # Match @name or @name:type
        url = _re.sub(r"@(\w+)(?::\w+)?", _replace, url)
        # Append extra params as a query string.
        used = set(
            m.group(1) for m in _re.finditer(r"@(\w+)(?::\w+)?", route.url)
        )
        extras = {k: v for k, v in params.items() if k not in used}
        if extras:
            import urllib.parse as _up

            url = url + "?" + _up.urlencode(extras, doseq=True)
        return url

    def model(
        self,
        name: str,
        model_class: Type[Any],
        key: str = "id",
    ) -> "Router":
        """Register implicit route model binding.

        Args:
            name: The parameter name to bind
            model_class: The model class to resolve to
            key: The key to query by (default: 'id')
        """
        def resolver(value: Any) -> Any:
            # Use the model's find method if available
            if hasattr(model_class, 'find'):
                return model_class.find(value)
            # Fallback to querying by the specified key
            return model_class.where(key, value).first()

        self._model_bindings[name] = resolver
        return self

    def resolve_model_bindings(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve model bindings for extracted route parameters.

        Args:
            params: Extracted route parameters

        Returns:
            Parameters with models resolved
        """
        resolved = {}
        for key, value in params.items():
            if key in self._model_bindings:
                resolver = self._model_bindings[key]
                try:
                    resolved[key] = resolver(value)
                except Exception:
                    # If resolution fails, keep original value
                    resolved[key] = value
            else:
                resolved[key] = value
        return resolved

    def find(self, path: str, request_method: str) -> Route:
        """Find a matching route by path and method.

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
        """Return all methods allowed for given path.

        Args:
            path: The request path

        Returns:
            List of allowed HTTP methods
        """
        allowed: List[str] = []
        for m, bucket in self.routes_by_method.items():
            for route in bucket:
                if route.matches(path, m.lower()):
                    allowed.append(m)
                    break
        return allowed

    def _create_preflight_route(
        self, path: str, allowed_methods: List[str]
    ) -> Route:
        """Generate an OPTIONS route for CORS preflight.

        Args:
            path: The request path
            allowed_methods: List of allowed HTTP methods

        Returns:
            A Route instance configured for OPTIONS requests
        """

        class PreflightController:
            def handle(self, request: Any, response: Any) -> Any:
                response.with_headers(
                    {
                        "Allow": ", ".join(allowed_methods),
                        "Access-Control-Allow-Methods": ", ".join(allowed_methods),
                    }
                ).status(204)
                return response

        return Route(path, PreflightController().handle, ["options"])
