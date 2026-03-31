"""
Route Parameter Validator for the Cara framework.

This module provides a robust validation system for route parameters using automatic
rule discovery and intelligent type conversion.
"""

from typing import Any, Dict, List, Optional


class CompilerRuleMapper:
    """Maps Route.compile() patterns to validation rules automatically."""

    # Mapping from compiler pattern to validation rule(s)
    PATTERN_TO_RULES = {
        r"(\d+)": ["integer"],  # int, integer patterns
        r"([a-zA-Z]+)": ["string"],  # string, alpha patterns
        r"([a-zA-Z0-9]+)": ["alphanum"],  # alphanum patterns
        r"([\w-]+)": ["slug"],  # slug patterns
        r"([0-9a-fA-F-]{36})": ["uuid"],  # uuid patterns
        r"(true|false|1|0)": ["boolean"],  # bool patterns
        r"(.*)": [],  # any pattern - no validation
        r"([^/]+)": [],  # default pattern - no validation
    }

    # Type-based compiler names that should have validation
    COMPILER_TYPE_RULES = {
        "int": ["integer"],
        "integer": ["integer"],
        "string": ["string"],
        "alpha": ["string"],
        "alphanum": ["alphanum"],
        "slug": ["slug"],
        "uuid": ["uuid"],
        "bool": ["boolean"],
        "boolean": ["boolean"],
        "numeric": ["numeric"],
        "any": [],
        "default": [],
    }

    @classmethod
    def get_validation_rules_for_compiler(
        cls, compiler_type: str, pattern: str = None
    ) -> List[str]:
        """Get validation rules for a compiler type/pattern."""
        # First try direct compiler type mapping
        if compiler_type in cls.COMPILER_TYPE_RULES:
            return cls.COMPILER_TYPE_RULES[compiler_type].copy()

        # Then try pattern matching
        if pattern:
            for pattern_regex, rules in cls.PATTERN_TO_RULES.items():
                if pattern == pattern_regex:
                    return rules.copy()

        return []

    @classmethod
    def get_type_converter_for_compiler(cls, compiler_type: str) -> Optional[callable]:
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


class RouteParameterValidator:
    """
    Robust route parameter validator with automatic rule discovery and intelligent type conversion.

    Features:
    - Automatic validation rule discovery from Route.compile() patterns
    - Intelligent type conversion based on compiler types
    - Clean error message extraction from validation rules
    - Extensible compiler → validation rule mapping
    """

    _validation_rules: Dict[str, str] = {}
    _compile_rules: Dict[str, str] = {}
    _compile_patterns: Dict[str, str] = {}

    @classmethod
    def set_validation_rules(cls, parameter: str, rules: str) -> None:
        """Set explicit validation rules for a route parameter."""
        cls._validation_rules[parameter] = rules

    @classmethod
    def set_compile_rule(
        cls, parameter: str, compiler_type: str, pattern: str = None
    ) -> None:
        """Set compile rule for a route parameter (from Route.compile())."""
        cls._compile_rules[parameter] = compiler_type
        if pattern:
            cls._compile_patterns[parameter] = pattern

    @classmethod
    def get_all_rules_for_parameter(cls, parameter: str) -> str:
        """Get combined validation rules for a parameter (explicit + auto-discovered)."""
        rules = []

        # Add explicit validation rules
        if explicit_rules := cls._validation_rules.get(parameter):
            rules.append(explicit_rules)

        # Add auto-discovered rules from compiler
        if compiler_type := cls._compile_rules.get(parameter):
            pattern = cls._compile_patterns.get(parameter)
            auto_rules = CompilerRuleMapper.get_validation_rules_for_compiler(
                compiler_type, pattern
            )
            rules.extend(auto_rules)

        return "|".join(rules) if rules else ""

    @classmethod
    def convert_parameter_value(cls, parameter: str, value: Any) -> Any:
        """
        Convert parameter value to appropriate type based on compiler rules.

        Returns converted value or original value if conversion fails/not needed.
        """
        if value is None:
            return value

        # Get compiler type for this parameter
        compiler_type = cls._compile_rules.get(parameter)
        if not compiler_type:
            return value

        # Get converter function
        converter = CompilerRuleMapper.get_type_converter_for_compiler(compiler_type)
        if not converter:
            return value

        # Try to convert, return original value if conversion fails
        try:
            return converter(value)
        except (ValueError, TypeError):
            # Conversion failed - validation will catch this later
            return value

    @classmethod
    def validate_parameters(cls, parameters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Validate route parameters against their rules.

        Returns None if validation passes, structured error dict if it fails.
        """
        # Local import to break circular import
        from cara.validation import Validation

        validation = Validation()

        # Prepare parameters and rules
        validation_rules = {}
        converted_parameters = {}

        for param_name, param_value in parameters.items():
            # Get all rules for this parameter (explicit + auto-discovered)
            all_rules = cls.get_all_rules_for_parameter(param_name)

            if all_rules:
                validation_rules[param_name] = all_rules
                # Convert parameter value based on compiler type
                converted_parameters[param_name] = cls.convert_parameter_value(
                    param_name, param_value
                )
            else:
                # No rules for this parameter
                converted_parameters[param_name] = param_value

        # If no parameters have validation rules, skip validation
        if not validation_rules:
            return None

        # Run validation
        validation_errors = validation.make(converted_parameters, validation_rules)

        # If validation passed, return None
        if not validation_errors:
            return None

        # Extract clean error structure from ValidationErrors object
        return cls._extract_validation_errors(validation_errors)

    @classmethod
    def _extract_validation_errors(cls, validation_errors) -> Dict[str, Any]:
        """Extract clean error structure from ValidationErrors object."""
        # Extract errors using ValidationErrors' interface
        error_dict = validation_errors.errors()
        first_error = validation_errors.first_error()

        return {
            "route_parameter_validation_failed": True,
            "errors": error_dict,
            "first_error": first_error or "Validation failed",
        }

    @classmethod
    def clear_all_rules(cls) -> None:
        """Clear all validation and compile rules."""
        cls._validation_rules.clear()
        cls._compile_rules.clear()
        cls._compile_patterns.clear()

    @classmethod
    def get_debug_info(cls, parameter: str = None) -> Dict[str, Any]:
        """Get debug information about rules and mappings."""
        if parameter:
            return {
                "parameter": parameter,
                "validation_rules": cls._validation_rules.get(parameter),
                "compile_rule": cls._compile_rules.get(parameter),
                "compile_pattern": cls._compile_patterns.get(parameter),
                "combined_rules": cls.get_all_rules_for_parameter(parameter),
            }

        return {
            "all_validation_rules": cls._validation_rules.copy(),
            "all_compile_rules": cls._compile_rules.copy(),
            "all_compile_patterns": cls._compile_patterns.copy(),
        }
