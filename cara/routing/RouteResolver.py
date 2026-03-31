"""
Resolves route handlers and injects dependencies for HTTP, WebSocket, and other contexts.
Provides Laravel-style dependency injection by analyzing method signatures and automatically resolving dependencies from the container.
"""

import inspect
from typing import Any, Callable, List, Optional

from cara.exceptions import (
    ControllerMethodNotFoundException,
    MissingContainerBindingException,
    RouteRegistrationException,
    ValidationException,
)
from cara.facades import Loader
from cara.http import Request, Response
from cara.routing import RouteParameterValidator
from cara.support.Str import modularize
from cara.websocket import Socket


class RouteResolver:
    def __init__(
        self,
        handler: Any,
        controller_paths: List[str] = None,
        container: Any = None,  # container may be None at route‐registration time
    ) -> None:
        """
        Initialize the resolver.

        Args:
            handler: The route handler (function, method, or string).
            controller_paths: List of paths to search for controllers.
            container: Dependency injection container (typically the Application).
                       May be None if called during route registration.
        """
        self._controller_paths = controller_paths
        self._container = container  # store container (may be None for now)
        self._route_handler: Optional[Callable] = None
        self._handler_signature: Optional[inspect.Signature] = None
        self._controller_class = None
        self._controller_method_name = None

        self.resolve(handler)

    def _safe_signature(self, callable_obj):
        """
        Create a safe signature that handles builtin type annotations properly.
        Fixes the Python inspect module bug with builtin types like int, str, etc.
        """
        try:
            # Try normal signature first
            return inspect.signature(callable_obj)
        except (ValueError, TypeError):
            # Handle builtin type annotation issues
            from typing import get_type_hints

            # Get function code and create safe parameters
            if hasattr(callable_obj, "__code__"):
                code = callable_obj.__code__
                param_names = code.co_varnames[: code.co_argcount]

                # Get defaults if any
                defaults = getattr(callable_obj, "__defaults__", None) or ()
                defaults_offset = len(param_names) - len(defaults)

                # Try to get type hints safely
                type_hints = {}
                try:
                    type_hints = get_type_hints(callable_obj)
                except (NameError, AttributeError, TypeError):
                    # If type hints fail, try to get them from annotations
                    if hasattr(callable_obj, "__annotations__"):
                        type_hints = callable_obj.__annotations__

                # Create parameters
                parameters = []
                for i, name in enumerate(param_names):
                    if name == "self":
                        continue

                    # Determine default value
                    default = inspect.Parameter.empty
                    if i >= defaults_offset:
                        default = defaults[i - defaults_offset]

                    # Get annotation safely
                    annotation = type_hints.get(name, inspect.Parameter.empty)

                    # Create parameter with preserved annotation
                    param = inspect.Parameter(
                        name,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        default=default,
                        annotation=annotation,
                    )
                    parameters.append(param)

                return inspect.Signature(parameters)

            # Fallback: create empty signature
            return inspect.Signature()

    def _instantiate_controller(self, controller_class, container=None):
        """
        Instantiate controller with dependency injection.
        Resolves constructor parameters from the container.
        """
        # Use provided container or fall back to stored one
        _container = container or self._container
        
        # If no container, instantiate without DI
        if not _container:
            return controller_class()

        # Get constructor signature
        try:
            sig = inspect.signature(controller_class.__init__)
        except (ValueError, TypeError):
            # No signature available, instantiate without args
            return controller_class()

        # Resolve constructor parameters
        params = {}
        for param_name, param in sig.parameters.items():
            # Skip 'self'
            if param_name == 'self':
                continue

            # Get type annotation
            param_type = param.annotation
            if param_type == inspect.Parameter.empty:
                # No type hint, skip
                continue

            # Resolve dependency from container
            # Container handles multi-strategy resolution + auto-instantiation
            try:
                resolved = _container.make(param_type)
                params[param_name] = resolved
            except Exception:
                # If resolution fails and has default, use it
                if param.default != inspect.Parameter.empty:
                    params[param_name] = param.default
                else:
                    # No default and can't resolve, raise error
                    raise MissingContainerBindingException(
                        f"Cannot resolve dependency '{param_name}' "
                        f"of type '{param_type}' for controller "
                        f"'{controller_class.__name__}'"
                    )

        # Instantiate with resolved dependencies
        return controller_class(**params)

    def _get_param_type(self, param_name: str):
        """Get the expected type for a parameter from method signature."""
        if not self._handler_signature:
            return None

        for name, param in self._handler_signature.parameters.items():
            if name == param_name and param.annotation != inspect.Parameter.empty:
                return param.annotation
        return None

    def _convert_param_value(self, value: str, expected_type):
        """Convert route parameter value to expected type."""
        if expected_type is None or value is None:
            return value

        try:
            # Handle common type conversions
            if expected_type == int:
                return int(value)
            elif expected_type == float:
                return float(value)
            elif expected_type == bool:
                return value.lower() in ("true", "1", "yes", "on")
            elif expected_type == str:
                return str(value)
            elif hasattr(expected_type, "__origin__"):
                # Handle typing generics like Optional[int], List[str], etc.
                import typing

                origin = getattr(expected_type, "__origin__", None)
                args = getattr(expected_type, "__args__", ())

                if origin is typing.Union:
                    # Handle Optional[T] which is Union[T, None]
                    non_none_types = [arg for arg in args if arg is not type(None)]
                    if non_none_types:
                        return self._convert_param_value(value, non_none_types[0])
                elif origin is list:
                    # Handle List[T] - split by comma
                    items = [item.strip() for item in value.split(",")]
                    if args:
                        return [
                            self._convert_param_value(item, args[0]) for item in items
                        ]
                    return items
            else:
                # For other types, try direct conversion
                return expected_type(value)
        except (ValueError, TypeError):
            # If conversion fails, return original value
            return value

    def resolve(self, handler: Any) -> None:
        """
        Resolve any supported handler type into a callable route handler.

        Args:
            handler: The route handler to resolve. Can be:
                - String ("Controller@method")
                - Function/Lambda
                - Controller class
                - Controller instance method
        """
        if isinstance(handler, str):
            # "UserController@index" or "api.v1.UserController@index"
            self.resolve_controller_string(handler)

        elif inspect.ismethod(handler) or inspect.isfunction(handler):
            self._route_handler = handler
            self._handler_signature = self._safe_signature(handler)

        elif inspect.isclass(handler):
            # If it's a controller class, prefer __call__ or index()
            instance = handler()
            if hasattr(instance, "__call__"):
                self._route_handler = instance.__call__
                self._handler_signature = self._safe_signature(self._route_handler)
            elif hasattr(instance, "index"):
                self._route_handler = getattr(instance, "index")
                self._handler_signature = self._safe_signature(self._route_handler)
            else:
                raise RuntimeError(f"Cannot resolve handler from class: {handler}")

        elif hasattr(handler, "__call__"):
            # Any callable object
            self._route_handler = handler.__call__
            self._handler_signature = self._safe_signature(self._route_handler)

        else:
            raise RuntimeError(f"Cannot resolve handler: {handler}")

    def resolve_controller_string(self, handler_path: str) -> None:
        """
        Resolve a controller string in the format "Controller@method".

        Args:
            handler_path: String in format "Controller@method" or
                "path.to.Controller@method"

        Raises:
            RouteRegistrationException: When controller import fails
            ControllerMethodNotFoundException: When method doesn't exist in controller
        """
        if "@" not in handler_path:
            raise ValueError('Handler must be in format "Controller@method"')

        controller_path, method_name = handler_path.split("@")
        path_parts = modularize(controller_path).split(".")
        if len(path_parts) > 1:
            controller_name = path_parts.pop()
            package_path = ".".join(path_parts)
        else:
            controller_name = path_parts[0]
            package_path = ""

        search_paths = [
            f"{base}.{package_path}" if package_path else base
            for base in self._controller_paths
        ]

        # Try to import controller
        controller_class = Loader.find(object, search_paths, controller_name)
        if not controller_class:
            raise RouteRegistrationException(
                f"Controller class '{controller_name}' not found in paths: {search_paths}"
            )

        try:
            # Store controller class and method for lazy instantiation
            self._controller_class = controller_class
            self._controller_method_name = method_name

            # Create a dummy instance to validate method exists and get signature
            temp_instance = controller_class.__new__(controller_class)
            if not hasattr(temp_instance, method_name):
                available_methods = [
                    m
                    for m in dir(temp_instance)
                    if not m.startswith("_") and callable(getattr(temp_instance, m))
                ]
                raise ControllerMethodNotFoundException(
                    controller_name=controller_name,
                    method_name=method_name,
                    available_methods=available_methods,
                )

            # Get the method and ensure it's callable
            method = getattr(temp_instance, method_name)
            if not callable(method):
                raise RouteRegistrationException(
                    f"'{method_name}' in '{controller_name}' is not callable"
                )

            self._handler_signature = self._safe_signature(method)

        except (ControllerMethodNotFoundException, RouteRegistrationException):
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            # Wrap any other exceptions
            raise RouteRegistrationException(
                f"Unexpected error resolving route '{handler_path}': {e}"
            )

    def _resolve_params(self, param_items, providers, container):
        """
        Resolves parameters for a handler using providers (by type or name), container, and default values.
        """
        kwargs = {}
        for name, param in param_items:
            if name == "self" or param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                continue
            annotation = param.annotation
            # Priority: providers (by type or name)
            for key, provider in providers.items():
                if (
                    callable(key) and annotation is not inspect._empty and key(annotation)
                ) or (isinstance(key, str) and name == key):
                    val = provider()
                    if val is not None:
                        kwargs[name] = val
                        break
            else:
                # Type-hint injection for custom classes
                if annotation is not inspect._empty and isinstance(annotation, type):
                    try:
                        kwargs[name] = container.make(annotation)
                        continue
                    except MissingContainerBindingException:
                        if param.default is not inspect._empty:
                            kwargs[name] = param.default
                            continue
                        raise RuntimeError(
                            f"Failed to resolve required dependency by type: {annotation}"
                        )
                # Name-based injection (only if no type hint)
                if annotation is inspect._empty:
                    try:
                        kwargs[name] = container.make(name)
                        continue
                    except MissingContainerBindingException:
                        if param.default is not inspect._empty:
                            kwargs[name] = param.default
                            continue
                        raise RuntimeError(
                            f"Failed to resolve required dependency by name: {name}"
                        )
                if param.default is not inspect._empty:
                    kwargs[name] = param.default
                    continue
                raise RuntimeError(f"Could not resolve parameter: {name}")
        return kwargs

    def _http_providers(self, request, response):
        providers = {
            (lambda t: t is Request): lambda: request,
            (lambda t: t is Response): lambda: response,
        }

        # Add route parameters with automatic type conversion
        route_params = getattr(request, "params", {})
        for param_name, param_value in route_params.items():
            # Get the expected type from method signature
            expected_type = self._get_param_type(param_name)
            converted_value = self._convert_param_value(param_value, expected_type)
            providers[param_name] = lambda v=converted_value: v

        return providers

    def _ws_providers(self, socket, message):
        providers = {
            (lambda t: t is Socket): lambda: socket,
            "socket": lambda: socket,
            "message": lambda: message,
        }

        # Add route parameters with automatic type conversion for WebSocket
        route_params = getattr(socket, "params", {})
        for param_name, param_value in route_params.items():
            # Get the expected type from method signature
            expected_type = self._get_param_type(param_name)
            converted_value = self._convert_param_value(param_value, expected_type)
            providers[param_name] = lambda v=converted_value: v

        return providers

    def _context_map(self):
        return {
            (Request, Response): lambda ctx: self._resolve_params(
                self._handler_signature.parameters.items(),
                self._http_providers(ctx[0], ctx[1]),
                self._container or ctx[0].application,
            ),
            (Socket, dict): lambda ctx: self._resolve_params(
                self._handler_signature.parameters.items(),
                self._ws_providers(ctx[0], ctx[1]),
                self._container or getattr(ctx[0], "application", None),
            ),
        }

    def _get_resolver(self, context):
        for types, resolver in self._context_map().items():
            if (
                isinstance(context, tuple)
                and len(context) == len(types)
                and all(isinstance(context[i], t) for i, t in enumerate(types))
            ):
                return resolver
        raise RuntimeError("Unknown context type for dependency resolution")

    async def handle(self, context: Any) -> Any:
        """
        Resolves dependencies and executes the route handler for any supported context.
        
        Args:
            context: HTTP (Request, Response) or WS (Socket, dict) context
        
        Returns:
            Handler result (automatically awaited if async)
        """
        # Inject route parameters into request if available
        request = context[0] if context else None
        if hasattr(request, "params"):
            for key, value in request.params.items():
                converted_value = RouteParameterValidator.convert_parameter_value(
                    key, value
                )
                setattr(request, f"param_{key}", converted_value)

        # If controller class stored, instantiate with DI at runtime
        if self._controller_class and self._controller_method_name:
            container = self._container or (request.application if request else None)
            controller = self._instantiate_controller(self._controller_class, container)
            route_handler = getattr(controller, self._controller_method_name)
        else:
            route_handler = self._route_handler
            if not route_handler:
                raise RuntimeError("No route handler has been resolved")

        resolver = self._get_resolver(context)
        kwargs = resolver(context)
        result = route_handler(**kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result
