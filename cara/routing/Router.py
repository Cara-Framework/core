"""
Core Router class for HTTP and WebSocket traffic.

Implements Laravel-style route lookup, including OPTIONS preflight for HTTP and WS dispatch.
"""

from __future__ import annotations

import re as _re
import urllib.parse as _up
from typing import Any

from cara.exceptions import (
    MethodNotAllowedException,
    RouteNotFoundException,
)
from cara.support import flatten

from ._RouteIndex import _RouteIndex
from .Route import Route

#: A bucket smaller than this is scanned directly: walking the index costs
#: more than a handful of regex matches.
_INDEX_MIN_ROUTES = 8

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
        module_location: str | None = None,
    ) -> None:
        self.application = application
        self.routes: list[Route] = flatten(routes)
        self.controller_locations = module_location

        # Bucket routes by method for efficient lookup. ``setdefault``
        # (vs the previous ``if key in self.routes_by_method`` guard)
        # so a route declaring a non-standard HTTP verb (PROPFIND,
        # MKCOL, REPORT — WebDAV; LINK / UNLINK — RFC 5988; custom
        # extension verbs) gets its own bucket and is reachable via
        # ``find``. Pre-fix the route landed in ``self.routes`` but
        # never in any bucket, so ``find`` (which iterates the
        # bucket for the request method) silently treated it as
        # 404 — both the route registration AND the inspection list
        # said it was there. The HTTP_METHODS bucket is still
        # pre-seeded so a vanilla GET-only app pays no overhead.
        self.routes_by_method: dict[str, list[Route]] = {m: [] for m in HTTP_METHODS}
        for route in self.routes:
            for m in route.request_method:
                key = m.upper()
                self.routes_by_method.setdefault(key, []).append(route)

    def add(self, *routes: Route) -> Router:
        """Add routes to the router."""
        for r in flatten(routes):
            self.routes.append(r)
            for m in r.request_method:
                key = m.upper()
                # See ``__init__``: ``setdefault`` instead of an
                # allow-list ``in`` check so non-standard verbs
                # registered after construction are also reachable.
                self.routes_by_method.setdefault(key, []).append(r)
        return self

    # ------------------------------------------------------------------
    # Named-route URL generation (Laravel: ``route('users.show', {id:1})``)
    # ------------------------------------------------------------------
    def find_by_name(self, name: str) -> Route | None:
        """Return the Route registered under ``name`` or None."""
        for route in self.routes:
            if route.get_name() == name:
                return route
        return None

    def url(self, name: str, params: dict[str, Any] | None = None) -> str:
        """Generate the URL for a named route.

        Substitutes ``@param`` placeholders (with optional ``:type`` suffix)
        in the route URL using ``params``. Unknown placeholders are left
        in place so the caller sees the mismatch.
        """

        route = self.find_by_name(name)
        if route is None:
            raise RouteNotFoundException(f"Route named '{name}' is not registered.")
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
        used = set(m.group(1) for m in _re.finditer(r"@(\w+)(?::\w+)?", route.url))
        extras = {k: v for k, v in params.items() if k not in used}
        if extras:
            url = f"{url}?{_up.urlencode(extras, doseq=True)}"
        return url

    def find(self, path: str, request_method: str) -> Route:
        """Find a matching route by path and method.

        For HTTP OPTIONS, automatically generates preflight if needed.
        """
        return self.find_with_params(path, request_method)[0]

    def find_with_params(
        self, path: str, request_method: str
    ) -> tuple[Route, dict[str, Any]]:
        """``find``, plus the parameters the path binds on the found route.

        The match that selects the route already captured them; returning
        them spares the caller a second regex match of the same path, and
        keeps them per request instead of on the shared ``Route``.
        """
        method = request_method.upper()
        found = self._first_match_with_params(method, path, request_method)
        if found is not None:
            return found

        # If no direct match, check for method-not-allowed
        allowed = self.get_allowed_methods(path)
        if allowed:
            if method == "OPTIONS":
                preflight = self._create_preflight_route(path, allowed)
                return preflight, preflight.extract_parameters(path)
            # Pass the allow-list through to the exception so the
            # default handler can emit the RFC-9110 ``Allow`` header
            # (see ``MethodNotAllowedException`` docstring). The kwarg
            # rides through ``HttpException.__init__``'s **kwargs
            # setattr loop, so no exception-class plumbing is needed.
            raise MethodNotAllowedException(
                f"Method {request_method} not allowed. Allowed methods: {allowed}",
                allowed=allowed,
            )

        raise RouteNotFoundException(f"No route matches path '{path}'")

    def get_allowed_methods(self, path: str) -> list[str]:
        """Return all methods allowed for given path.

        Args:
            path: The request path

        Returns:
            List of allowed HTTP methods
        """
        return [
            m
            for m in self.routes_by_method
            if self._first_match(m, path, m.lower()) is not None
        ]

    def _first_match(self, bucket_method: str, path: str, method: str) -> Route | None:
        """The first-registered route of one method bucket that matches ``path``."""
        found = self._first_match_with_params(bucket_method, path, method)
        return found[0] if found is not None else None

    def _first_match_with_params(
        self, bucket_method: str, path: str, method: str
    ) -> tuple[Route, dict[str, Any]] | None:
        """The first-registered route of one method bucket that matches ``path``,
        with the parameters that match bound.

        Registration order decides between two routes that both match (a
        literal ``/orders/export`` declared after ``/orders/@id`` loses to
        it). The index only narrows which routes are tried, in that same
        order — see ``_RouteIndex``.
        """
        bucket = self.routes_by_method.get(bucket_method)
        if not bucket:
            return None
        # ``$`` in a route pattern also matches before a trailing newline, so
        # ``/health\n`` matches ``/health`` — but its last SEGMENT is
        # ``health\n``, which the index would not find. Keep the full scan
        # for that path shape rather than change what it resolves to.
        if len(bucket) < _INDEX_MIN_ROUTES or path.endswith("\n"):
            for route in bucket:
                params = route.match(path, method)
                if params is not None:
                    return route, params
            return None

        # ``__dict__`` rather than an attribute set in ``__init__``: a router
        # built without it (``__new__`` in tooling) must still resolve.
        indexes = self.__dict__.setdefault("_route_indexes", {})
        index = indexes.get(bucket_method)
        if index is None or not index.is_current_for(bucket):
            index = indexes[bucket_method] = _RouteIndex(bucket)
        for _, route in index.candidates(path):
            params = route.match(path, method)
            if params is not None:
                return route, params
        return None

    def _create_preflight_route(self, path: str, allowed_methods: list[str]) -> Route:
        """Generate an OPTIONS route for CORS preflight.

        The advertised method list ALWAYS includes ``OPTIONS`` itself,
        regardless of whether an OPTIONS route was explicitly
        registered. Browser preflight logic expects the requesting
        method (``Access-Control-Request-Method``) to appear in the
        response, and CORS clients spec the preflight to include the
        preflight verb in the advertised set — pre-fix
        ``get_allowed_methods`` only returned what was explicitly
        registered, so a path with just ``GET`` + ``POST`` returned
        ``Allow: GET, POST`` on a preflight (no OPTIONS), which some
        strict HTTP clients flag as inconsistent (`Allow` is supposed
        to describe the methods the resource supports, and the
        resource clearly supports OPTIONS — the response itself is
        proof). Dedup the merge so an explicit OPTIONS registration
        doesn't double-list.

        Args:
            path: The request path
            allowed_methods: List of allowed HTTP methods

        Returns:
            A Route instance configured for OPTIONS requests
        """
        # Preserve registration order, append OPTIONS if absent.
        # ``dict.fromkeys`` is the idiomatic ordered-dedup pattern.
        advertised = list(dict.fromkeys([*allowed_methods, "OPTIONS"]))

        class PreflightController:
            def handle(self, request: Any, response: Any) -> Any:
                response.with_headers(
                    {
                        "Allow": ", ".join(advertised),
                        "Access-Control-Allow-Methods": ", ".join(advertised),
                    }
                ).status(204)
                return response

        return Route(path, PreflightController().handle, ["options"])
