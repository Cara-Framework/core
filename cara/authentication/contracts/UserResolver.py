"""
User Resolver Interface.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class UserResolver(ABC):
    """
    Interface for user resolvers.
    """

    @abstractmethod
    def resolve_user(
        self, identifier: str, context: Dict[str, Any] = None
    ) -> Optional[Any]:
        """Resolve a user by identifier."""
        pass

    @abstractmethod
    def resolve_user_by_credentials(self, credentials: Dict[str, Any]) -> Optional[Any]:
        """Resolve a user by credentials."""
        pass
