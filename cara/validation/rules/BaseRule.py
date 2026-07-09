"""
Base Validation Rule for the Cara framework.

This module provides the base class for all validation rules in the application.
"""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.contracts import Rule


class BaseRule(Rule):
    """
    Abstract base class for all validation rules.

    Provides common functionality and enforces the contract for validation rules.
    Handles all message logic centrally to eliminate code duplication.
    """

    @staticmethod
    def field_present(data: Any, field: str) -> bool:
        """Whether ``field`` exists in ``data``, following dotted paths.

        Wildcard-expanded rule fields arrive as concrete dotted paths
        (``items.0.name``) while ``params["_data"]`` stays nested — a
        flat ``field in data`` check silently reports every nested
        field as absent.
        """
        node = data
        for segment in field.split("."):
            if isinstance(node, dict):
                if segment not in node:
                    return False
                node = node[segment]
            elif isinstance(node, list):
                if not segment.isdigit() or int(segment) >= len(node):
                    return False
                node = node[int(segment)]
            else:
                return False
        return True

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        """
        Validate the field value against the rule.

        Must be implemented by subclasses.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement validate method"
        )

    def message(self, field: str, params: dict[str, Any]) -> str:
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

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        """
        Generate default error message for this rule.

        Must be implemented by subclasses.
        This is the only method subclasses need to implement for messages.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement default_message method"
        )
