# cara/middleware/Middleware.py

"""
Core Middleware base class for Cara framework.
Laravel-style middleware with automatic parameter parsing and dependency injection.
"""

import inspect
from abc import ABC, abstractmethod
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    Union,
    get_args,
    get_origin,
)


class Middleware(ABC):
    """
    Base class for all middleware with Laravel-style parameter parsing.

    Supports automatic parameter parsing from method signatures:
    - middleware:param1,param2 -> __init__(self, application, param1, param2)
    - Type hints for automatic conversion: int, bool, List[str], Optional[str]
    - Default values supported
    """

    def __init__(self, application: Any, **kwargs):
        self.application = application

        # Store parsed parameters as attributes
        for key, value in kwargs.items():
            setattr(self, key, value)

    @abstractmethod
    async def handle(self, request: Any, next_fn: Callable[[Any], Awaitable[Any]]) -> Any:
        """Handle the request/context. Must be implemented by all middleware."""
        pass

    async def terminate(self, request: Any, response: Any) -> None:
        """Called after response is sent. Override for terminable middleware."""
        pass

    @classmethod
    def create_with_parameters(
        cls, application: Any, parameters: Optional[List[str]] = None
    ) -> "Middleware":
        """
        Factory method to create middleware instance with automatic parameter parsing.
        Uses method signature inspection for type-safe parameter injection.
        """
        return MiddlewareParameterParser.parse_and_create(
            cls, application, parameters or []
        )

    @classmethod
    def with_parameters(cls, *parameters: Any) -> Callable[[Any], "Middleware"]:
        """Laravel-style factory for manual parameter setting."""

        def factory(application: Any) -> "Middleware":
            return cls.create_with_parameters(application, [str(p) for p in parameters])

        return factory


class MiddlewareParameterParser:
    """
    Automatic parameter parser for middleware using method signatures.
    Supports Laravel-style parameter syntax with type conversion.
    """

    @staticmethod
    def parse_and_create(
        middleware_class: Type[Middleware], application: Any, parameters: List[str]
    ) -> Middleware:
        """Create middleware instance with automatic parameter parsing."""
        # Get __init__ signature
        init_signature = inspect.signature(middleware_class.__init__)

        # Extract parameter definitions from signature
        param_definitions = MiddlewareParameterParser._extract_parameter_definitions(
            init_signature
        )

        # Parse raw parameters according to signature
        parsed_params = MiddlewareParameterParser._parse_parameters(
            param_definitions, parameters
        )

        # Create instance with parsed parameters
        return middleware_class(application, **parsed_params)

    @staticmethod
    def _extract_parameter_definitions(
        signature: inspect.Signature,
    ) -> Dict[str, Dict[str, Any]]:
        """Extract parameter definitions from __init__ signature."""
        definitions = {}

        for param_name, param in signature.parameters.items():
            # Skip 'self', 'application', and **kwargs
            if param_name in ["self", "application"] or param.kind == param.VAR_KEYWORD:
                continue

            param_type = (
                param.annotation if param.annotation != inspect.Parameter.empty else str
            )
            default_value = (
                param.default if param.default != inspect.Parameter.empty else None
            )
            is_required = param.default == inspect.Parameter.empty

            definitions[param_name] = {
                "type": param_type,
                "default": default_value,
                "required": is_required,
                "index": len(definitions),  # Parameter order
            }

        return definitions

    @staticmethod
    def _parse_parameters(
        definitions: Dict[str, Dict[str, Any]], raw_parameters: List[str]
    ) -> Dict[str, Any]:
        """Parse raw parameters according to signature definitions."""
        parsed = {}

        for param_name, param_def in definitions.items():
            index = param_def["index"]
            param_type = param_def["type"]
            default_value = param_def["default"]
            is_required = param_def["required"]

            # Get raw value by index
            if index < len(raw_parameters):
                raw_value = raw_parameters[index].strip()
            else:
                if is_required:
                    raise ValueError(f"Required parameter '{param_name}' is missing")
                parsed[param_name] = default_value
                continue

            # Parse according to type
            try:
                parsed_value = MiddlewareParameterParser._convert_value(
                    raw_value, param_type
                )
                parsed[param_name] = parsed_value
            except Exception as e:
                if is_required:
                    raise ValueError(f"Cannot parse parameter '{param_name}': {e}")
                parsed[param_name] = default_value

        return parsed

    @staticmethod
    def _convert_value(raw_value: str, target_type: Type) -> Any:
        """Convert raw string value to target type with support for complex types."""
        if not raw_value:
            return None

        # Handle Optional types (Union[T, None])
        origin = get_origin(target_type)
        if origin is Union:
            args = get_args(target_type)
            if len(args) == 2 and type(None) in args:
                # This is Optional[T]
                actual_type = args[0] if args[1] is type(None) else args[1]
                return MiddlewareParameterParser._convert_value(raw_value, actual_type)

        # Handle List types
        if origin is list:
            list_item_type = get_args(target_type)[0] if get_args(target_type) else str
            values = [v.strip() for v in raw_value.split(",") if v.strip()]
            return [
                MiddlewareParameterParser._convert_basic_type(v, list_item_type)
                for v in values
            ]

        # Handle basic types
        return MiddlewareParameterParser._convert_basic_type(raw_value, target_type)

    @staticmethod
    def _convert_basic_type(value: str, target_type: Type) -> Any:
        """Convert string to basic type."""
        if target_type == str or target_type is str:
            return value
        elif target_type == int or target_type is int:
            return int(value)
        elif target_type == float or target_type is float:
            return float(value)
        elif target_type == bool or target_type is bool:
            return value.lower() in ("true", "1", "yes", "on")
        else:
            # Try direct conversion
            return target_type(value)
