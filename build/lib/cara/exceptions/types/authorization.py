"""Authorization-related exceptions for the Cara framework."""

from typing import Any, Dict, Optional

from .base import CaraException


class AuthorizationException(CaraException):
    """
    Base for auth or permission errors.
    HTTP 403 Forbidden.
    """

    is_http_exception = True
    status_code = 403

    def __init__(
        self,
        message: str = "This action is unauthorized.",
        ability: Optional[str] = None,
        user: Optional[Any] = None,
        resource: Optional[Any] = None,
        status_code: int = 403,
    ):
        """
        Initialize the authorization failed exception.
        """
        super().__init__(message)
        self.message = message
        self.ability = ability
        self.user = user
        self.resource = resource
        self.status_code = status_code

    def __str__(self) -> str:
        """
        String representation of the exception.
        """
        return self.message

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for JSON response."""
        return {
            "error": str(self),
            "type": "authorization_error",
        }


class AuthorizationFailedException(AuthorizationException):
    """Thrown when a user is not authorized to perform an action."""

    is_http_exception = True
    status_code = 403

    def __init__(
        self,
        message: str = "This action is unauthorized.",
        ability: Optional[str] = None,
        user: Optional[Any] = None,
        resource: Optional[Any] = None,
        status_code: int = 403,
    ):
        """
        Initialize the authorization failed exception.
        """
        super().__init__(message)
        self.message = message
        self.ability = ability
        self.user = user
        self.resource = resource
        self.status_code = status_code

    def __str__(self) -> str:
        """
        String representation of the exception.
        """
        return self.message
