"""
Validation Rule Interface for the Cara framework.

This module defines the contract that any validation rule must implement, specifying required methods for validation logic.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class Rule(ABC):
    """
    Contract that every validation rule must satisfy.
    """

    @abstractmethod
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """
        Execute validation logic.

        :param field: The name of the field being validated (e.g. "age").
        :param value: The actual value from input data for that field.
        :param params: Parsed parameters for the rule (e.g. {"min": "5"}).
        :return: True if the value passes the rule, False otherwise.
        """

    @abstractmethod
    def message(self, field: str, params: Dict[str, Any]) -> str:
        """
        Return the error message for this rule, using field name and params.

        :param field: The name of the field.
        :param params: Parsed parameters for the rule.
        :return: A human-readable error string.
        """
