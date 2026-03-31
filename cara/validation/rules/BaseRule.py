"""
Base Validation Rule for the Cara framework.

This module provides the base class for all validation rules in the application.
"""

from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.contracts import Rule


class BaseRule(Rule):
    """
    Abstract base class for all validation rules.

    Provides common functionality and enforces the contract for validation rules.
    Handles all message logic centrally to eliminate code duplication.
    """

    def _parse_params(self, raw: str) -> Dict[str, Any]:
        """
        Parse rule parameters from string format.

        Examples:
        - "min:5" -> {"min": "5"}
        - "regex:^[A-Z]+$" -> {"regex": "^[A-Z]+$"}
        - "required" -> {}
        """
        parts = raw.split(":", 1)
        if len(parts) == 2:
            key, val = parts
            return {key: val}
        return {}

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """
        Validate the field value against the rule.

        Must be implemented by subclasses.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement validate method"
        )

    def message(self, field: str, params: Dict[str, Any]) -> str:
        """
        Generate error message for validation failure.

        Handles custom messages centrally, falls back to default_message().
        This method should NOT be overridden by subclasses.
        """
        # Check for custom message first
        if MessageFormatter.has_custom_message(params):
            custom_message = MessageFormatter.get_custom_message(params)
            return MessageFormatter.format_message(custom_message, field, params)

        # Fall back to default message from subclass
        return self.default_message(field, params)

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        """
        Generate default error message for this rule.

        Must be implemented by subclasses.
        This is the only method subclasses need to implement for messages.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement default_message method"
        )

    def get_default_message(self, field: str, params: Dict[str, Any]) -> str:
        """
        Should be overridden by subclasses to provide default error message.
        This method should return the default message for the rule.
        """
        rule_name = self.__class__.__name__.replace("Rule", "").lower()
        return f"The {self._format_attribute_name(field).lower()} field failed {rule_name} validation."

    def _format_message(self, message: str, field: str, params: Dict[str, Any]) -> str:
        """
        Advanced placeholder replacement for custom messages.

        Supported placeholders:
        - :attribute - Field name (user_name -> User Name)
        - :field - Raw field name (user_name)
        - :value - The actual value being validated
        - :rule - The validation rule name
        - Rule-specific placeholders (e.g., :min, :max, :size)
        """
        if not message:
            return message

        # Basic replacements
        replacements = {
            ":attribute": self._format_attribute_name(field),
            ":field": field,
            ":value": str(params.get("_value", "")),
            ":rule": params.get("_rule", ""),
        }

        # Add rule-specific parameters as placeholders
        for key, value in params.items():
            if not key.startswith("_") and key not in ["data"]:
                replacements[f":{key}"] = str(value)

        # Apply all replacements
        formatted_message = message
        for placeholder, replacement in replacements.items():
            formatted_message = formatted_message.replace(placeholder, replacement)

        return formatted_message

    def _format_attribute_name(self, field: str) -> str:
        """Convert field name to human-readable attribute name."""
        # Convert snake_case to Title Case
        # user_name -> User Name
        # email -> Email
        words = field.replace("_", " ").split()
        return " ".join(word.capitalize() for word in words)
