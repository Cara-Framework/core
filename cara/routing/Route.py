"""Route helper class for creating and managing route instances.

Mirrors Laravel-style syntax for HTTP and WebSocket routes with support for:
- Parameter validation and type conversion
- Route grouping with prefix/middleware
- Route model binding
- Named routes
"""

from typing import Any, Dict, List, Optional, Type, Union

from cara.http import Response
from cara.routing import (
    RouteCompiler,
    RouteGroup,
    RouteResolver,
)
from cara.routing.RouteParameterValidator import RouteParameterValidator
from cara.support.Collection import flatten


class Route:
    """Route helper class for creating different types of routes.

    Provides a fluent interface for defining HTTP and WebSocket routes
    with support for parameter validation, middleware, and model binding.
    """

    # Default parameter compiler patterns - Laravel-compatible regex patterns
    compilers: Dict[str, str] = {
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

    controllers_locations: List[str] = []

    def __init__(
        self,
        url: str,
        controller: Any,
        request_method: List[str],
        name: Optional[str] = None,
        compilers: Optional[Dict[str, str]] = None,
        controllers_locations: Optional[List[str]] = None,
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
        self._middleware: List[str] = []
        self._model_bindings: Dict[str, Type[Any]] = {}
        self.compiler = RouteCompiler(self.url, compilers or Route.compilers)
        self.controller = RouteResolver(
            controller,
            controllers_locations or Route.controllers_locations,
        )

    def __str__(self) -> str:
        """String representation of the route."""
        return f"<Route [{self._name}]: {self.url}>"

    def get_name(self) -> Optional[str]:
        """Get the route name.

        Returns:
            The route name or None
        """
        return self._name

    def name(self, name: str) -> "Route":
        """Set the route name.

        Args:
            name: The route name

        Returns:
            Self for method chaining
        """
        self._name = name
        return self

    def model(self, param: str, model_class: Type[Any]) -> "Route":
        """Register implicit route model binding.

        Args:
            param: The route parameter name
            model_class: The model class to bind to

        Returns:
            Self for method chaining
        """
        self._model_bindings[param] = model_class
        return self

    def get_model_bindings(self) -> Dict[str, Type[Any]]:
        """Get all model bindings for this route.

        Returns:
            Dictionary of parameter names to model classes
        """
        return self._model_bindings

    def _normalize_url(self, url: str) -> str:
        """Normalize URL by ensuring leading slash and removing duplicates.

        Args:
            url: The URL to normalize

        Returns:
            The normalized URL
        """
        return "/" + "/".join(filter(None, url.split("/")))

    def extract_parameters(self, path: str) -> Dict[str, Any]:
        return self.compiler.extract_parameters(path)

    def set_params(self, params: Dict[str, Any]):
        self._params = params

    def get_params(self) -> Dict[str, Any]:
        return getattr(self, "_params", {})

    def set_params_from_path(self, path: str) -> dict:
        params = self.extract_parameters(path)
        self.set_params(params)
        return params

    def middleware(self, middleware: str | List[str]) -> "Route":
        if isinstance(middleware, list):
            self._middleware.extend(middleware)
        else:
            self._middleware.append(middleware)
        return self

    def get_middleware(self) -> List[str]:
        return self._middleware

    def matches(self, path: str, method: str) -> bool:
        return self.compiler.matches(path) and method.lower() in self.request_method

    async def dispatch(self, request: Any, response: Any) -> Response:
        """Dispatch a request to this route's controller.

        Extracts route parameters, applies model bindings, and delegates to controller.

        Args:
            request: The HTTP request
            response: The HTTP response object

        Returns:
            The response object
        """
        # Extract route parameters
        params = self.extract_parameters(request.path)

        # Apply model bindings if registered
        for param_name, model_class in self._model_bindings.items():
            if param_name in params:
                try:
                    # Resolve model using find() method if available
                    if hasattr(model_class, 'find'):
                        params[param_name] = model_class.find(params[param_name])
                except Exception:
                    # Keep original value if model resolution fails
                    pass

        # Set validated parameters on request for easy access
        self.set_params(params)
        for key, value in params.items():
            # Convert to appropriate type based on route compiler
            converted_value = self._convert_parameter_type(key, value)
            setattr(request, f"param_{key}", converted_value)

        result = await self.controller.handle(request, response)
        if isinstance(result, Response):
            response.clone_from(result)
            return response
        elif result is not None:
            response.json(result)
            return response
        return response

    def _convert_parameter_type(
        self, param_name: str, param_value: str
    ) -> Union[int, bool, str]:
        """Convert route parameter to appropriate type based on compiler pattern.

        Args:
            param_name: The parameter name
            param_value: The parameter value as string

        Returns:
            The converted parameter value
        """
        # Get the compiler pattern for this parameter
        compiler_pattern = self.compiler.compilers.get(param_name)

        if not compiler_pattern:
            return param_value

        # Convert based on known patterns
        if compiler_pattern == r"(\d+)" or param_name in ["int", "integer"]:
            try:
                return int(param_value)
            except (ValueError, TypeError):
                return param_value
        elif compiler_pattern == r"(true|false|1|0)":
            return param_value.lower() in ["true", "1"]

        return param_value

    @classmethod
    def factory(
        cls,
        url: str,
        controller: Any,
        request_method: List[str],
        **options,
    ) -> "Route":
        name = options.pop("name", None)
        prefix = options.pop("prefix", None)
        if prefix:
            url = cls._join_paths(prefix, url)
        return cls(
            url=url,
            controller=controller,
            request_method=request_method,
            name=name,
            compilers=cls.compilers,
            controllers_locations=cls.controllers_locations,
            **options,
        )

    @classmethod
    def get(cls, url: str, controller: Any, **options) -> "Route":
        """Create a GET route."""
        return cls.factory(url, controller, ["get", "head"], **options)

    @classmethod
    def post(cls, url: str, controller: Any, **options) -> "Route":
        """Create a POST route."""
        return cls.factory(url, controller, ["post"], **options)

    @classmethod
    def put(cls, url: str, controller: Any, **options) -> "Route":
        """Create a PUT route."""
        return cls.factory(url, controller, ["put"], **options)

    @classmethod
    def patch(cls, url: str, controller: Any, **options) -> "Route":
        """Create a PATCH route."""
        return cls.factory(url, controller, ["patch"], **options)

    @classmethod
    def delete(cls, url: str, controller: Any, **options) -> "Route":
        """Create a DELETE route."""
        return cls.factory(url, controller, ["delete"], **options)

    @classmethod
    def ws(cls, url: str, controller: Any, **options) -> "Route":
        """
        Create a WebSocket route.

        Usage: Route.ws("/chat/@room_id", ChatController)
        """
        return cls.factory(url, controller, ["ws"], **options)

    @classmethod
    def options(cls, url: str, controller: Any, **options) -> "Route":
        """Create an OPTIONS route."""
        return cls.factory(url, controller, ["options"], **options)

    @classmethod
    def any(cls, url: str, controller: Any, **options) -> "Route":
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
        only: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> List["Route"]:
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

            Route.api_resource("/products", "ProductController")
        """
        actions = ["index", "store", "show", "update", "destroy"]
        if only is not None:
            actions = [a for a in actions if a in only]
        if exclude is not None:
            actions = [a for a in actions if a not in exclude]

        base = cls._join_paths(base)
        param_segment = f"/@{param}:{param_type}" if param_type else f"/@{param}"
        routes: List[Route] = []

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
    ) -> List["Route"]:
        """Alias for :meth:`api_resource` (Laravel uses ``resource`` for
        web-side routes; for API-only apps the semantics are identical)."""
        return cls.api_resource(base, controller, **kwargs)

    @classmethod
    def _join_paths(cls, *paths: str) -> str:
        segments = []
        for path in paths:
            segments.extend(filter(None, path.split("/")))
        return "/" + "/".join(segments)

    @classmethod
    def group(cls, *routes: "Route", **options) -> List["Route"]:
        """Group multiple routes under a shared prefix or middleware."""
        inner: List[Route] = []
        for route in flatten(routes):
            if prefix := options.get("prefix"):
                route.url = cls._join_paths(prefix, route.url)
                route.compiler.compile_route(route.url)
            if name_prefix := options.get("name"):
                route._name = name_prefix + (route._name or "")
            if middleware := options.get("middleware"):
                route.middleware(middleware)
            inner.append(route)
        return inner

    @classmethod
    def set_controller_locations(cls, *controllers_locations: str) -> "Route":
        cls.controllers_locations = list(controllers_locations)
        return cls

    @classmethod
    def compile(cls, key: str, to: str = "") -> "Route":
        cls.compilers.update({key: to})
        # Get the pattern for this compiler type
        pattern = cls.compilers.get(to, to)
        # Notify RouteParameterValidator about compile rules
        RouteParameterValidator.set_compile_rule(key, to, pattern)
        return cls

    @classmethod
    def validate(cls, parameter: str, rules: str) -> "Route":
        """Set validation rules for a route parameter."""
        RouteParameterValidator.set_validation_rules(parameter, rules)
        return cls

    @classmethod
    def prefix(cls, prefix: str) -> RouteGroup:
        return RouteGroup(prefix=prefix)

    @classmethod
    def routes(cls, *routes: "Route") -> List["Route"]:
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
