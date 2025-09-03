"""
Message Formatter for Cara Validation System.

This module provides advanced message formatting capabilities for validation rules,
including placeholder replacement and attribute name formatting.
"""

from typing import Any, Dict


class MessageFormatter:
    """
    Utility class for formatting validation messages with advanced placeholder support.

    Handles custom message priority, placeholder replacement, and attribute formatting.
    """

    @staticmethod
    def format_message(custom_message: str, field: str, params: Dict[str, Any]) -> str:
        """
        Format custom message with advanced placeholder replacement.

        Supported placeholders:
        - :attribute - Field name (user_name -> User Name)
        - :field - Raw field name (user_name)
        - :value - The actual value being validated
        - :rule - The validation rule name
        - Rule-specific placeholders (e.g., :min, :max, :size)
        """
        if not custom_message:
            return custom_message

        # Basic replacements
        replacements = {
            ":attribute": MessageFormatter.format_attribute_name(field),
            ":field": field,
            ":value": str(params.get("_value", "")),
            ":rule": params.get("_rule", ""),
        }

        # Add rule-specific parameters as placeholders
        for key, value in params.items():
            if not key.startswith("_") and key not in ["data"]:
                replacements[f":{key}"] = str(value)

        # Apply all replacements
        formatted_message = custom_message
        for placeholder, replacement in replacements.items():
            formatted_message = formatted_message.replace(placeholder, replacement)

        return formatted_message

    @staticmethod
    def format_attribute_name(field: str) -> str:
        """
        Convert field name to human-readable attribute name.

        Examples:
        - user_name -> User Name
        - email -> Email
        - first_name -> First Name
        """
        words = field.replace("_", " ").split()
        return " ".join(word.capitalize() for word in words)

    @staticmethod
    def has_custom_message(params: Dict[str, Any]) -> bool:
        """Check if custom message is available in params."""
        return "_custom_message" in params and params["_custom_message"]

    @staticmethod
    def get_custom_message(params: Dict[str, Any]) -> str:
        """Get custom message from params."""
        return params.get("_custom_message", "")
