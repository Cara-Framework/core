"""
Validation Interface for the Cara framework.

This module defines the contract that any validation class must implement, specifying required
methods for validation operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class Validation(ABC):
    """Contract that any Validator must satisfy."""

    @staticmethod
    @abstractmethod
    def make(
        data: Dict[str, Any],
        rules: Dict[str, str],
        messages: Dict[str, str] = None,
    ) -> "Validation":
        """
        Run validation on `data` according to `rules` (Laravel-style).

        Returns a new Validation instance for chaining .fails() or .passes() checks.

        :param data: e.g. {"username": "alice", "email": "alice@example.com"}
        :param rules: e.g. {"username": "required|min:3", "email": "required|email"}
        :param messages: Optional custom error messages
        :return: Validation instance (for method chaining)

        Usage:
        - validator = Validation.make(data, rules)
        - if validator.fails(): ...
        """

    @abstractmethod
    def fails(self) -> bool:
        """Return True if validation failed."""

    @abstractmethod
    def passes(self) -> bool:
        """Return True if validation passed."""

    @abstractmethod
    def errors(self):
        """
        Return ValidationErrors object with all errors.
        """

    @abstractmethod
    def validated(self) -> Dict[str, Any]:
        """Return only the key/value pairs that passed all rules."""
