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
            # Non-fatal by design: the registry is process-global (see class doc)
            return value
