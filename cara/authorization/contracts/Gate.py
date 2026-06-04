"""
Gate interface for authorization.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any


class Gate(ABC):
    """
    Gate contract interface for managing authorization checks.
    """

    @abstractmethod
    def define(self, ability: str, callback: Callable | str) -> None:
        """
        Define a new ability.
        """
        pass

    @abstractmethod
    def allows(self, ability: str, *args) -> bool:
        """
        Check if the current user is authorized for the given ability.
        """
        pass

    @abstractmethod
    def denies(self, ability: str, *args) -> bool:
        """
        Check if the current user is denied the given ability.
        """
        pass

    @abstractmethod
    def any(self, abilities: list[str], *args) -> bool:
        """
        Check if the current user has any of the given abilities.
        """
        pass

    @abstractmethod
    def authorize(self, ability: str, *args) -> None:
        """
        Authorize the given ability or raise an exception.
        """
        pass

    @abstractmethod
    def for_user(self, user: Any):
        """
        Get a gate instance for the given user.
        """
        pass
