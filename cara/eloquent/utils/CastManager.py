"""
CastManager - Centralized casting utility for Eloquent ORM

Single responsibility: Handle all casting operations cleanly and efficiently.
Follows DRY and KISS principles for casting management.
"""

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Union

from cara.eloquent.utils.DateManager import DateManager


class CastManager:
    """
    Centralized casting management for Eloquent ORM.

    Handles:
    - Type casting
    - Custom cast registration
    - Cast validation
    - Bidirectional casting (get/set)
    """

    # Built-in cast handlers
    _cast_handlers = {}

    @classmethod
    def register_cast(cls, cast_type: str, handler: callable) -> None:
        """Register a custom cast handler."""
        cls._cast_handlers[cast_type] = handler

    @classmethod
    def cast_value(cls, cast_definition: str, value: Any, operation: str = "get") -> Any:
        """
        Cast a value using the specified cast definition.

        Args:
            cast_definition: Cast definition (e.g., 'datetime:Y-m-d', 'decimal:2')
            value: Value to cast
            operation: 'get' or 'set' operation

        Returns:
            Casted value
        """
        if value is None:
            return None

        # Parse cast definition
        cast_type, cast_options = cls._parse_cast_definition(cast_definition)

        # Check for custom handler
        if cast_type in cls._cast_handlers:
            return cls._cast_handlers[cast_type](value, cast_options, operation)

        # Use built-in casting
        return cls._cast_builtin(cast_type, value, cast_options, operation)

    @classmethod
    def _parse_cast_definition(cls, cast_definition: str) -> tuple:
        """Parse cast definition into type and options."""
        if ":" in cast_definition:
            cast_type, options_str = cast_definition.split(":", 1)
            cast_options = cls._parse_cast_options(options_str)
        else:
            cast_type = cast_definition
            cast_options = {}

        return cast_type, cast_options

    @classmethod
    def _parse_cast_options(cls, options_str: str) -> Dict[str, Any]:
        """Parse cast options string into dictionary."""
        options = {}

        # Handle simple numeric options (e.g., "2" for decimal places)
        if options_str.isdigit():
            options["precision"] = int(options_str)
        elif "," in options_str:
            # Handle multiple options separated by commas
            for option in options_str.split(","):
                if "=" in option:
                    key, value = option.split("=", 1)
                    options[key.strip()] = value.strip()
                else:
                    # Assume it's a precision value
                    if option.strip().isdigit():
                        options["precision"] = int(option.strip())
        else:
            # Single option value
            options["format"] = options_str

        return options

    @classmethod
    def _cast_builtin(
        cls, cast_type: str, value: Any, options: Dict[str, Any], operation: str
    ) -> Any:
        """Handle built-in casting types."""
        cast_type = cast_type.lower()

        # Boolean casting
        if cast_type in ["bool", "boolean"]:
            return cls._cast_boolean(value, operation)

        # Integer casting
        elif cast_type in ["int", "integer"]:
            return cls._cast_integer(value, operation)

        # Float casting
        elif cast_type == "float":
            return cls._cast_float(value, operation)

        # Decimal casting
        elif cast_type == "decimal":
            return cls._cast_decimal(value, options, operation)

        # String casting
        elif cast_type in ["str", "string"]:
            return cls._cast_string(value, operation)

        # JSON casting
        elif cast_type == "json":
            return cls._cast_json(value, operation)

        # Array casting
        elif cast_type == "array":
            return cls._cast_array(value, operation)

        # Date casting
        elif cast_type == "date":
            return cls._cast_date(value, options, operation)

        # DateTime casting
        elif cast_type == "datetime":
            return cls._cast_datetime(value, options, operation)

        # Timestamp casting
        elif cast_type == "timestamp":
            return cls._cast_timestamp(value, operation)

        # Collection casting
        elif cast_type == "collection":
            return cls._cast_collection(value, operation)

        # No casting defined - return as is
        return value

    # ===== Specific Cast Methods =====

    @classmethod
    def _cast_boolean(cls, value: Any, operation: str) -> bool:
        """Cast value to boolean."""
        if operation == "set":
            # When setting, convert various truthy values
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ["true", "1", "yes", "on"]
            if isinstance(value, (int, float)):
                return value != 0
            return bool(value)
        else:
            # When getting, return boolean
            return bool(value)

    @classmethod
    def _cast_integer(cls, value: Any, operation: str) -> int:
        """Cast value to integer."""
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    @classmethod
    def _cast_float(cls, value: Any, operation: str) -> float:
        """Cast value to float."""
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    @classmethod
    def _cast_decimal(
        cls, value: Any, options: Dict[str, Any], operation: str
    ) -> Decimal:
        """Cast value to Decimal with precision."""
        try:
            decimal_value = Decimal(str(value))

            # Apply precision if specified
            precision = options.get("precision", 2)
            if precision is not None:
                # Round to specified decimal places
                quantizer = Decimal("0.1") ** precision
                decimal_value = decimal_value.quantize(quantizer)

            return decimal_value
        except (ValueError, TypeError):
            return Decimal("0")

    @classmethod
    def _cast_string(cls, value: Any, operation: str) -> str:
        """Cast value to string."""
        if value is None:
            return ""
        return str(value)

    @classmethod
    def _cast_json(cls, value: Any, operation: str) -> Union[str, Any]:
        """Cast value to/from JSON."""
        if operation == "set":
            # When setting, convert to JSON string
            if isinstance(value, str):
                # Validate JSON and return as-is
                try:
                    json.loads(value)
                    return value
                except (json.JSONDecodeError, ValueError):
                    # If not valid JSON, encode it
                    return json.dumps(value)
            else:
                # Convert object to JSON
                return json.dumps(value)
        else:
            # When getting, parse JSON string
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    return value
            return value

    @classmethod
    def _cast_array(cls, value: Any, operation: str) -> Union[str, list]:
        """Cast value to/from array."""
        if operation == "set":
            # When setting, convert to JSON string
            if isinstance(value, list):
                return json.dumps(value)
            elif isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        return value
                    else:
                        return json.dumps([parsed])
                except (json.JSONDecodeError, ValueError):
                    return json.dumps([value])
            else:
                return json.dumps([value])
        else:
            # When getting, parse to list
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    return parsed if isinstance(parsed, list) else [parsed]
                except (json.JSONDecodeError, ValueError):
                    return [value]
            elif isinstance(value, list):
                return value
            else:
                return [value]

    @classmethod
    def _cast_date(
        cls, value: Any, options: Dict[str, Any], operation: str
    ) -> Union[str, datetime]:
        """Cast value to/from date."""
        format_str = options.get("format", "Y-m-d")

        if operation == "set":
            # When setting, convert to database format
            return DateManager.to_database_format(value)
        else:
            # When getting, parse and format
            parsed_date = DateManager.parse(value)
            if parsed_date:
                return DateManager.format(parsed_date, format_str)
            return value

    @classmethod
    def _cast_datetime(
        cls, value: Any, options: Dict[str, Any], operation: str
    ) -> Union[str, datetime]:
        """Cast value to/from datetime."""
        format_str = options.get("format", "Y-m-d H:i:s")

        if operation == "set":
            # When setting, convert to database format
            return DateManager.to_database_format(value)
        else:
            # When getting, parse and format
            parsed_date = DateManager.parse(value)
            if parsed_date:
                return DateManager.format(parsed_date, format_str)
            return value

    @classmethod
    def _cast_timestamp(cls, value: Any, operation: str) -> Union[int, datetime]:
        """Cast value to/from timestamp."""
        if operation == "set":
            # When setting, convert to timestamp
            parsed_date = DateManager.parse(value)
            if parsed_date:
                return int(parsed_date.timestamp())
            return int(value) if value else None
        else:
            # When getting, convert timestamp to datetime
            if isinstance(value, (int, float)):
                return DateManager.parse(value)
            return value

    @classmethod
    def _cast_collection(cls, value: Any, operation: str) -> Any:
        """Cast value to Collection."""
        if operation == "set":
            # When setting, store as JSON
            if hasattr(value, "to_array"):
                return json.dumps(value.to_array())
            elif isinstance(value, list):
                return json.dumps(value)
            else:
                return json.dumps([value])
        else:
            # When getting, return as Collection
            try:
                from cara.support.Collection import Collection

                if isinstance(value, str):
                    parsed = json.loads(value)
                    return Collection(parsed)
                elif isinstance(value, list):
                    return Collection(value)
                else:
                    return Collection([value])
            except ImportError:
                # Fallback to list if Collection not available
                return cls._cast_array(value, operation)

    # ===== Validation Methods =====

    @classmethod
    def is_valid_cast_type(cls, cast_type: str) -> bool:
        """Check if cast type is valid."""
        built_in_types = {
            "bool",
            "boolean",
            "int",
            "integer",
            "float",
            "decimal",
            "str",
            "string",
            "json",
            "array",
            "date",
            "datetime",
            "timestamp",
            "collection",
        }

        return cast_type.lower() in built_in_types or cast_type in cls._cast_handlers

    @classmethod
    def get_available_cast_types(cls) -> list:
        """Get list of all available cast types."""
        built_in_types = [
            "bool",
            "boolean",
            "int",
            "integer",
            "float",
            "decimal",
            "str",
            "string",
            "json",
            "array",
            "date",
            "datetime",
            "timestamp",
            "collection",
        ]

        return built_in_types + list(cls._cast_handlers.keys())


# Convenience function
def cast_value(cast_definition: str, value: Any, operation: str = "get") -> Any:
    """
    Convenience function to cast a value.

    Args:
        cast_definition: Cast definition string
        value: Value to cast
        operation: 'get' or 'set'

    Returns:
        Casted value
    """
    return CastManager.cast_value(cast_definition, value, operation)
