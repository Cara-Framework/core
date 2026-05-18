"""
User Resolver Interface.
"""

from abc import ABC, abstractmethod
from typing import Any


class UserResolver(ABC):
    """
    Interface for user resolvers.
    """

    @abstractmethod
    def resolve_user(self, identifier: str, context: dict[str, Any] = None) -> Any | None:
        """Resolve a user by identifier."""
        pass

    @abstractmethod
    def resolve_user_by_credentials(self, credentials: dict[str, Any]) -> Any | None:
        """Resolve a user by credentials."""
        pass
