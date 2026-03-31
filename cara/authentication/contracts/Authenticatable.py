"""
Authenticatable Interface for users.
"""

from typing import Any, Optional


class Authenticatable:
    """
    Interface for authenticatable users with default implementations.

    User models can inherit this and override only the methods they need.
    Default implementations work for most JWT-based authentication scenarios.
    """

    def get_auth_identifier(self) -> Any:
        """Get the unique identifier for the user."""
        # Try to get auth_id first, fallback to id
        if hasattr(self, "get_auth_id"):
            return self.get_auth_id()
        return getattr(self, "id", None)

    def get_auth_password(self) -> str:
        """Get the password for the user. Default empty for JWT-only auth."""
        return ""

    def get_remember_token(self) -> Optional[str]:
        """Get the remember token for the user."""
        return getattr(self, "remember_token", None)

    def set_remember_token(self, value: str) -> None:
        """Set the remember token for the user."""
        if hasattr(self, "remember_token"):
            self.remember_token = value

    def get_remember_token_name(self) -> str:
        """Get the remember token column name."""
        return "remember_token"
