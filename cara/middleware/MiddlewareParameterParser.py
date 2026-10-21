"""Canonical definition of ``MiddlewareParameterParser``."""

from __future__ import annotations

import annotationlib
import inspect
from typing import (
    TYPE_CHECKING,
    Any,
    Union,
    get_args,
    get_origin,
)

from cara.exceptions import ConfigurationException

if TYPE_CHECKING:
    from .Middleware import Middleware


class MiddlewareParameterParser:
    """
    Automatic parameter parser for middleware using method signatures.
    Supports Laravel-style parameter syntax with type conversion.

    A route parameter that is missing or does not convert is a route
    declaration bug, so it raises ``ConfigurationException`` instead of
    falling back to the default: ``can:listings.manage`` quietly checking
    ``view`` is an authorization decision nobody wrote.
    """

    @staticmethod
    def parse_and_create(
        middleware_class: type[Middleware], application: Any, parameters: list[str]
    ) -> Middleware:
        """Create middleware instance with automatic parameter parsing."""
        # Extract parameter definitions from the __init__ signature
        param_definitions = MiddlewareParameterParser._extract_parameter_definitions(
            middleware_class
        )

        # Parse raw parameters according to signature
        parsed_params = MiddlewareParameterParser._parse_parameters(
            middleware_class, param_definitions, parameters
        )

        # Create instance with parsed parameters
        return middleware_class(application, **parsed_params)

    @staticmethod
    def _extract_parameter_definitions(
        middleware_class: type[Middleware],
    ) -> dict[str, dict[str, Any]]:
        """Extract parameter definitions from __init__ signature."""
        init = middleware_class.__init__
        definitions = {}

        for param_name, param in inspect.signature(init).parameters.items():
            # Skip 'self', 'application', and **kwargs
            if param_name in ["self", "application"] or param.kind == param.VAR_KEYWORD:
                continue

            param_type = MiddlewareParameterParser._resolve_annotation(
                middleware_class, init, param
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
    def _resolve_annotation(
        middleware_class: type[Middleware], init: Any, param: inspect.Parameter
    ) -> Any:
        """Return a parameter's annotation as a type.

        Middleware modules use ``from __future__ import annotations``, so the
        signature carries the STRING ``'list[str] | None'``; converting through
        it raised ``TypeError`` and every annotated parameter kept its default.
        Each parameter is resolved on its own so a ``TYPE_CHECKING``-only
        annotation on ``application`` does not break the others.
        """
        annotation = param.annotation
        if annotation is inspect.Parameter.empty:
            return str
        if not isinstance(annotation, str):
            return annotation
        try:
            return annotationlib.ForwardRef(annotation).evaluate(owner=init)
        except Exception as exc:
            raise ConfigurationException(
                f"{middleware_class.__name__}: cannot resolve the annotation "
                f"{annotation!r} of parameter '{param.name}'"
            ) from exc

    @staticmethod
    def _parse_parameters(
        middleware_class: type[Middleware],
        definitions: dict[str, dict[str, Any]],
        raw_parameters: list[str],
    ) -> dict[str, Any]:
        """Parse raw parameters according to signature definitions."""
        parsed = {}
        last_index = len(definitions) - 1

        for param_name, param_def in definitions.items():
            index = param_def["index"]
            param_type = param_def["type"]

            # Get raw value by index
            if index >= len(raw_parameters):
                if param_def["required"]:
                    raise ConfigurationException(
                        f"{middleware_class.__name__}: required parameter "
                        f"'{param_name}' is missing"
                    )
                parsed[param_name] = param_def["default"]
                continue

            # A trailing list parameter takes every remaining value, like
            # Laravel's variadic ``...$guards``: ``auth:jwt,session``.
            if index == last_index and MiddlewareParameterParser._is_list_type(
                param_type
            ):
                raw_value = ",".join(raw_parameters[index:])
            else:
                raw_value = raw_parameters[index].strip()

            # Parse according to type
            try:
                parsed[param_name] = MiddlewareParameterParser._convert_value(
                    raw_value, param_type
                )
            except Exception as e:
                raise ConfigurationException(
                    f"{middleware_class.__name__}: cannot parse parameter "
                    f"'{param_name}' from {raw_value!r}: {e}"
                ) from e

        return parsed

    @staticmethod
    def _is_list_type(target_type: Any) -> bool:
        """Whether ``target_type`` is ``list[...]`` or ``list[...] | None``."""
        if get_origin(target_type) is Union:
            return any(
                get_origin(arg) is list
                for arg in get_args(target_type)
                if arg is not type(None)
            )
        return get_origin(target_type) is list

    @staticmethod
    def _convert_value(raw_value: str, target_type: type) -> Any:
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
    def _convert_basic_type(value: str, target_type: type) -> Any:
        """Convert string to basic type."""
        if target_type is str or target_type is Any:
            return value
        elif target_type is int:
            return int(value)
        elif target_type is float:
            return float(value)
        elif target_type is bool:
            return value.lower() in ("true", "1", "yes", "on")
        else:
            # Try direct conversion
            return target_type(value)
