"""
Route Parameter Coercion for the Cara framework.

This module turns a matched URL segment into the Python type its
``Route.compile()`` declaration implies. It does NOT validate: the route's
compiled regex is the only gate a parameter passes through, and by the time a
value arrives here it has already matched.

Everything that once looked like validation here — ``Route.validate()``, an
explicit-rule registry, and a ``validate_parameters()`` entry point — was
removed after an audit found it had no caller anywhere in the framework, in
any product, or in the resolver: rules were registered and then silently
dropped, so a route advertising ``id: int|min:1`` enforced nothing. The route
generator even EMITTED those calls from controller docstrings, actively
steering products onto the dead path. A surface that promises enforcement and
delivers none fails open (DOCTRINE §9), so it is gone rather than half-wired.
Enforce parameter shape in the compiled pattern, or in the controller's
FormRequest.
"""

from __future__ import annotations

from typing import Any

from cara.validation import Validation

from .CompilerRuleMapper import CompilerRuleMapper


class RouteParameterValidator:
    """
    Coerces matched route parameters to the type their compiler declares.

    The registry is process-global and keyed by the BARE parameter name, so
    ``Route.compile("id", "int")`` in one routes file coerces ``id`` on every
    route in the process. That is why coercion failure must stay non-fatal
    (see ``convert_parameter_value``).
    """

    _compile_rules: dict[str, str] = {}
    _compile_patterns: dict[str, str] = {}

    @classmethod
    def set_compile_rule(
        cls, parameter: str, compiler_type: str, pattern: str | None = None
    ) -> None:
        """Set compile rule for a route parameter (from Route.compile())."""
        cls._compile_rules[parameter] = compiler_type
        if pattern:
            cls._compile_patterns[parameter] = pattern

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
        except ValueError, TypeError:
            # Conversion failed - validation will catch this later
            return value

    @classmethod
    def validate_parameters(cls, parameters: dict[str, Any]) -> dict[str, Any] | None:
        """
        Validate route parameters against their rules.

        Returns None if validation passes, structured error dict if it fails.
        """
        # Local import to break circular import

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
    def _extract_validation_errors(cls, validation_errors) -> dict[str, Any]:
        """Extract clean error structure from ValidationErrors object."""
        # Extract errors using ValidationErrors' interface
        error_dict = validation_errors.all()
        first_error = validation_errors.first()

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
    def get_debug_info(cls, parameter: str | None = None) -> dict[str, Any]:
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
