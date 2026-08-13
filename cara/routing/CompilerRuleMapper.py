"""CompilerRuleMapper."""

from __future__ import annotations

from typing import Any


class CompilerRuleMapper:
    """Maps Route.compile() compiler types to type converters."""

    @classmethod
    def get_type_converter_for_compiler(cls, compiler_type: str) -> callable | None:
        """Get type converter function for a compiler type."""
        converters = {
            "int": cls._convert_to_int,
            "integer": cls._convert_to_int,
            "bool": cls._convert_to_bool,
            "boolean": cls._convert_to_bool,
            "numeric": cls._convert_to_numeric,
            "string": cls._convert_to_string,
            "alpha": cls._convert_to_string,
            "alphanum": cls._convert_to_string,
            "slug": cls._convert_to_string,
            "uuid": cls._convert_to_string,
        }
        return converters.get(compiler_type)

    @staticmethod
    def _convert_to_int(value: Any) -> int:
        """Convert value to integer."""
        if isinstance(value, int):
            return value
        if isinstance(value, str) and (value.isdigit() or value.lstrip("-").isdigit()):
            return int(value)
        return int(value)  # Will raise ValueError if conversion fails

    @staticmethod
    def _convert_to_bool(value: Any) -> bool:
        """Convert value to boolean."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)

    @staticmethod
    def _convert_to_numeric(value: Any) -> float:
        """Convert value to numeric (float)."""
        if isinstance(value, (int, float)):
            return float(value)
        return float(value)  # Will raise ValueError if conversion fails

    @staticmethod
    def _convert_to_string(value: Any) -> str:
        """Convert value to string."""
        if isinstance(value, str):
            return value
        return str(value)
