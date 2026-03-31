"""
Gate interface for authorization.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, List, Union


class Gate(ABC):
    """
    Gate contract interface for managing authorization checks.
    """

    @abstractmethod
    def define(self, ability: str, callback: Union[Callable, str]) -> None:
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
    def any(self, abilities: List[str], *args) -> bool:
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
