"""Route helper class for creating and managing route instances.

Mirrors Laravel-style syntax for HTTP and WebSocket routes with support for:
- Parameter validation and type conversion
- Route grouping with prefix/middleware
- Named routes
"""

from __future__ import annotations

from typing import Any

from cara.routing.RouteCompiler import RouteCompiler
from cara.routing.RouteGroup import RouteGroup
from cara.routing.RouteParameterValidator import RouteParameterValidator
from cara.routing.RouteResolver import RouteResolver
from cara.support.Collection import flatten


class Route:
    """Route helper class for creating different types of routes.

    Provides a fluent interface for defining HTTP and WebSocket routes
    with support for parameter validation and middleware.
    """

    # Default parameter compiler patterns - Laravel-compatible regex patterns
    compilers: dict[str, str] = {
        "int": r"(\d+)",
        "integer": r"(\d+)",
        "string": r"([a-zA-Z]+)",
        "alpha": r"([a-zA-Z]+)",
        "alphanum": r"([a-zA-Z0-9]+)",
        "slug": r"([\w-]+)",
        "uuid": r"([0-9a-fA-F-]{36})",
        "bool": r"(true|false|1|0)",
        "any": r"(.*)",
        "default": r"([^/]+)",
    }

    controllers_locations: list[str] = []

    def __init__(
        self,
        url: str,
        controller: Any,
        request_method: list[str],
        name: str | None = None,
        compilers: dict[str, str] | None = None,
        controllers_locations: list[str] | None = None,
        **options: Any,
    ) -> None:
        """Initialize a new Route instance.

        Args:
            url: URL pattern (e.g. "/users/@id:int")
            controller: Handler, controller method or function
            request_method: List of HTTP methods (e.g. ["get"], ["post"])
            name: Optional route name for URL generation
            compilers: Optional custom parameter compilers
            controllers_locations: Optional controller module locations
            **options: Additional route options
        """
        self.url = self._normalize_url(url)
        self.request_method = [m.lower() for m in request_method]
        self._name = name
        self._middleware: list[str] = []
        self.compiler = RouteCompiler(self.url, compilers or Route.compilers)
        self.controller = RouteResolver(
            controller,
            controllers_locations or Route.controllers_locations,
        )

    def __str__(self) -> str:
        """String representation of the route."""
        return f"<Route [{self._name}]: {self.url}>"

    def get_name(self) -> str | None:
        """Get the route name.

        Returns:
            The route name or None
        """
        return self._name

    def name(self, name: str) -> Route:
        """Set the route name.

        Args:
            name: The route name

        Returns:
            Self for method chaining
        """
        self._name = name
        return self

    def _normalize_url(self, url: str) -> str:
        """Normalize URL by ensuring leading slash and removing duplicates.

        Args:
            url: The URL to normalize

        Returns:
            The normalized URL
        """
        return "/" + "/".join(filter(None, url.split("/")))

    def extract_parameters(self, path: str) -> dict[str, Any]:
        return self.compiler.extract_parameters(path)

    def set_params(self, params: dict[str, Any]):
        self._params = params

    def get_params(self) -> dict[str, Any]:
        return getattr(self, "_params", {})

    def set_params_from_path(self, path: str) -> dict:
        params = self.extract_parameters(path)
        self.set_params(params)
        return params

    def middleware(self, middleware: str | list[str]) -> Route:
        if isinstance(middleware, list):
            self._middleware.extend(middleware)
        else:
            self._middleware.append(middleware)
        return self

    def prepend_middleware(self, middleware: str | list[str]) -> Route:
        """Prepend middleware so it runs BEFORE the route's existing
        chain. Used by ``Route.group`` to apply group middleware
        outer-to-inner — Laravel runs group middleware first so a
        route declaring ``middleware("auth")`` inside a
        ``Route.group(middleware="throttle:api")`` gets evaluated as
        ``[throttle:api, auth]`` not ``[auth, throttle:api]``.
        """
        if isinstance(middleware, list):
            self._middleware = list(middleware) + self._middleware
        else:
            self._middleware = [middleware] + self._middleware
        return self

    def get_middleware(self) -> list[str]:
        return self._middleware

    def matches(self, path: str, method: str) -> bool:
        return self.compiler.matches(path) and method.lower() in self.request_method

    @classmethod
    def factory(
        cls,
        url: str,
        controller: Any,
        request_method: list[str],
        **options,
    ) -> Route:
        name = options.pop("name", None)
        prefix = options.pop("prefix", None)
        # ``middleware=[...]`` is a first-class factory kwarg (parity with
        # ``name=`` / ``prefix=``). Without popping + applying it here it
        # would fall through to ``**options`` and be silently dropped by
        # ``__init__`` — which is exactly the form the route generator
        # emits (``Route.get(path, handler, middleware=[...], name=...)``).
        # That bug meant every generator-produced per-route middleware
        # (``throttle:ai`` on the AI endpoints, ``auth,admin`` on
        # ``products.marketplace_data``, ``throttle:health`` on the probes)
        # vanished at registration, so the routes ran unthrottled /
        # unguarded. Applying it via ``route.middleware()`` keeps the
        # group-prepends-before-route ordering intact (RouteGroup.routes).
        middleware = options.pop("middleware", None)
        if prefix:
            url = cls._join_paths(prefix, url)
        route = cls(
            url=url,
            controller=controller,
            request_method=request_method,
            name=name,
            compilers=cls.compilers,
            controllers_locations=cls.controllers_locations,
            **options,
        )
        if middleware:
            route.middleware(middleware)
        return route

    @classmethod
    def get(cls, url: str, controller: Any, **options) -> Route:
        """Create a GET route."""
        return cls.factory(url, controller, ["get", "head"], **options)

    @classmethod
    def post(cls, url: str, controller: Any, **options) -> Route:
        """Create a POST route."""
        return cls.factory(url, controller, ["post"], **options)

    @classmethod
    def put(cls, url: str, controller: Any, **options) -> Route:
        """Create a PUT route."""
        return cls.factory(url, controller, ["put"], **options)

    @classmethod
    def patch(cls, url: str, controller: Any, **options) -> Route:
        """Create a PATCH route."""
        return cls.factory(url, controller, ["patch"], **options)

    @classmethod
    def delete(cls, url: str, controller: Any, **options) -> Route:
        """Create a DELETE route."""
        return cls.factory(url, controller, ["delete"], **options)

    @classmethod
    def ws(cls, url: str, controller: Any, **options) -> Route:
        """
        Create a WebSocket route.

        Usage: Route.ws("/chat/@room_id", ChatController)
        """
        return cls.factory(url, controller, ["ws"], **options)

    @classmethod
    def options(cls, url: str, controller: Any, **options) -> Route:
        """Create an OPTIONS route."""
        return cls.factory(url, controller, ["options"], **options)

    @classmethod
    def any(cls, url: str, controller: Any, **options) -> Route:
        """Create a route matching any HTTP verb."""
        return cls.factory(
            url,
            controller,
            ["get", "post", "put", "patch", "delete", "options", "head"],
            **options,
        )

    @classmethod
    def api_resource(
        cls,
        base: str,
        controller: Any,
        param: str = "id",
        param_type: str = "int",
        only: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> list[Route]:
        """Laravel-style ``apiResource``.

        Auto-registers the standard REST actions on ``controller``:
          - ``index``   GET    ``/{base}``
          - ``store``   POST   ``/{base}``
          - ``show``    GET    ``/{base}/@{param}:{param_type}``
          - ``update``  PUT    ``/{base}/@{param}:{param_type}``
          - ``update``  PATCH  ``/{base}/@{param}:{param_type}``
          - ``destroy`` DELETE ``/{base}/@{param}:{param_type}``

        Pass ``only=[...]`` or ``exclude=[...]`` to trim the action set.

        Example::

            Route.api_resource("/posts", "PostController")
        """
        actions = ["index", "store", "show", "update", "destroy"]
        if only is not None:
            actions = [a for a in actions if a in only]
        if exclude is not None:
            actions = [a for a in actions if a not in exclude]

        base = cls._join_paths(base)
        param_segment = f"/@{param}:{param_type}" if param_type else f"/@{param}"
        routes: list[Route] = []

        if "index" in actions:
            routes.append(cls.get(base, f"{controller}@index"))
        if "store" in actions:
            routes.append(cls.post(base, f"{controller}@store"))
        if "show" in actions:
            routes.append(cls.get(base + param_segment, f"{controller}@show"))
        if "update" in actions:
            routes.append(cls.put(base + param_segment, f"{controller}@update"))
            routes.append(cls.patch(base + param_segment, f"{controller}@update"))
        if "destroy" in actions:
            routes.append(cls.delete(base + param_segment, f"{controller}@destroy"))
        return routes

    @classmethod
    def resource(
        cls,
        base: str,
        controller: Any,
        **kwargs,
    ) -> list[Route]:
        """Alias for :meth:`api_resource` (Laravel uses ``resource`` for
        web-side routes; for API-only apps the semantics are identical)."""
        return cls.api_resource(base, controller, **kwargs)

    @classmethod
    def _join_paths(cls, *paths: str) -> str:
        segments = []
        for path in paths:
            segments.extend(filter(None, path.split("/")))
        return "/" + "/".join(segments)

    @staticmethod
    def _group_marker(
        prefix: str | None,
        name_prefix: str | None,
        middleware: str | list[str] | None,
    ) -> tuple:
        """Canonical identity of one group application, recorded per route.

        Grouping mutates the Route object in place, so re-running the same
        group over the same objects (hot reload, repeated discovery, tests
        re-importing module-level routes) used to stack the prefix, the
        name prefix AND the middleware a second time. Each application is
        recorded on the route; an identical re-application is a no-op.
        Nested groups with different prefixes/middleware still stack —
        their markers differ. (Deliberate trade-off: two *identical*
        nested groups collapse to one; ``/a/a`` via two same-config
        groups needs an explicit combined prefix.)
        """
        if isinstance(middleware, list):
            middleware_key: tuple = tuple(middleware)
        elif middleware:
            middleware_key = (middleware,)
        else:
            middleware_key = ()
        return (prefix or "", name_prefix or "", middleware_key)

    @classmethod
    def group(cls, *routes: Route, **options) -> list[Route]:
        """Group multiple routes under a shared prefix and/or middleware.

        Correctness properties:

        1. Group middleware is **prepended**, not appended. Laravel
           runs group middleware first (outer-to-inner). The previous
           code appended, so a route with ``middleware("auth")``
           inside ``Route.group(middleware="throttle:api")`` resolved
           to ``[auth, throttle:api]`` — auth ran before throttle,
           defeating ``throttle:auth``-style per-user keying and
           letting unauthenticated traffic hit the auth guard before
           getting throttled.

        2. Idempotent re-application via a per-route ledger (see
           ``_group_marker``): re-running the same group over the same
           Route objects no longer double-prefixes the URL/name or
           duplicates group middleware.
        """
        inner: list[Route] = []
        prefix = options.get("prefix")
        name_prefix = options.get("name")
        group_middleware = options.get("middleware")
        marker = cls._group_marker(prefix, name_prefix, group_middleware)

        for route in flatten(routes):
            applied = route.__dict__.setdefault("_applied_groups", set())
            if marker in applied:
                inner.append(route)
                continue
            applied.add(marker)

            if prefix:
                route.url = cls._join_paths(prefix, route.url)
                route.compiler.compile_route(route.url)
            if name_prefix:
                route._name = name_prefix + (route._name or "")
            if group_middleware:
                # Prepend so group middleware runs first.
                route.prepend_middleware(group_middleware)
            inner.append(route)
        return inner

    @classmethod
    def set_controller_locations(cls, *controllers_locations: str) -> Route:
        cls.controllers_locations = list(controllers_locations)
        return cls

    @classmethod
    def compile(cls, key: str, to: str = "") -> Route:
        cls.compilers.update({key: to})
        # Get the pattern for this compiler type
        pattern = cls.compilers.get(to, to)
        # Notify RouteParameterValidator about compile rules
        RouteParameterValidator.set_compile_rule(key, to, pattern)
        return cls

    @classmethod
    def validate(cls, parameter: str, rules: str) -> Route:
        """Set validation rules for a route parameter."""
        RouteParameterValidator.set_validation_rules(parameter, rules)
        return cls

    @classmethod
    def prefix(cls, prefix: str) -> RouteGroup:
        return RouteGroup(prefix=prefix)

    @classmethod
    def routes(cls, *routes: Route) -> list[Route]:
        return list(flatten(routes))

    def is_ws(self) -> bool:
        """Return True if this route is a WebSocket route."""
        return "ws" in self.request_method

    def is_http(self) -> bool:
        """Return True if this route is an HTTP route (get, post, etc)."""
        http_methods = {
            "get",
            "post",
            "put",
            "patch",
            "delete",
            "head",
            "options",
        }
        return any(m in http_methods for m in self.request_method)
