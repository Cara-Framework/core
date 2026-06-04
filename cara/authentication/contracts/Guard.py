"""
Guard Interface for authentication guards.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Guard(ABC):
    """
    Interface for authentication guards.
    """

    @abstractmethod
    def check(self) -> bool:
        """Check if the current request is authenticated."""
        pass

    @abstractmethod
    def guest(self) -> bool:
        """Check if the current request is a guest."""
        pass

    @abstractmethod
    def user(self) -> Any | None:
        """Get the currently authenticated user."""
        pass

    @abstractmethod
    def id(self) -> Any | None:
        """Get the ID of the authenticated user."""
        pass

    @abstractmethod
    def attempt(self, credentials: dict[str, Any]) -> bool:
        """Attempt to authenticate using credentials."""
        pass

    @abstractmethod
    def logout(self) -> None:
        """Log the user out."""
        pass

    @abstractmethod
    def login(self, user) -> str:
        """Log a user in and return token."""
        pass

    @abstractmethod
    def validate_token(self, token: str) -> bool:
        """Validate a token."""
        pass
