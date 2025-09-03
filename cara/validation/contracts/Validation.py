"""
Validation Interface for the Cara framework.

This module defines the contract that any validation class must implement, specifying required
methods for validation operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class Validation(ABC):
    """Contract that any Validator must satisfy."""

    @abstractmethod
    def make(self, data: Dict[str, Any], rules: Dict[str, str]) -> None:
        """
        Run validation on `data` according to `rules`.

        :param data: e.g. {"username": "alice", "email": "alice@example.com"}
        :param rules: e.g. {"username": "required|min:3", "email": "required|email"}
        :raises ValidationException: if any rule fails
        """

    @abstractmethod
    def fails(self) -> bool:
        """Return True if last call to `make()` found any errors."""

    @abstractmethod
    def errors(self) -> Dict[str, list[str]]:
        """
        Return a mapping: field → list of error_messages.
        """

    @abstractmethod
    def validated(self) -> Dict[str, Any]:
        """Return only the key/value pairs that passed all rules."""
