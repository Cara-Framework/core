"""
Route helper class for creating and managing route instances in the Cara framework.

Mirrors Laravel-style syntax for HTTP and WebSocket routes.
"""

from typing import Any, Dict, List, Optional

from cara.http import Response
from cara.routing import (
    RouteCompiler,
    RouteGroup,
    RouteResolver,
)
from cara.routing.RouteParameterValidator import RouteParameterValidator
from cara.support.Collection import flatten


class Route:
    """Route helper class for creating different types of routes."""

    # Default parameter compiler patterns
    compilers = {
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
        **options,
    ):
        """
        Initialize a new Route instance.

        :param url: URL pattern (e.g. "/users/@id:int")
        :param controller: handler, controller method or function
        :param request_method: list of methods, e.g. ["get"], ["ws"]
        :param name: optional route name
        """
        self.url = self._normalize_url(url)
        self.request_method = [m.lower() for m in request_method]
        self._name = name
        self._middleware: List[str] = []
        self.compiler = RouteCompiler(self.url, compilers or Route.compilers)
        self.controller = RouteResolver(
            controller,
            controllers_locations or Route.controllers_locations,
        )

    def __str__(self):
        return f"<Route [{self._name}]: {self.url}>"

    def get_name(self) -> Optional[str]:
        return self._name

    def name(self, name: str) -> "Route":
        self._name = name
        return self

    def _normalize_url(self, url: str) -> str:
        # Ensure leading slash and remove duplicate separators
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

    async def dispatch(self, request: Any, response: Any) -> Any:
        # Extract route parameters
        params = self.extract_parameters(request.path)

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

    def _convert_parameter_type(self, param_name: str, param_value: str):
        """Convert route parameter to appropriate type based on compiler pattern."""
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
