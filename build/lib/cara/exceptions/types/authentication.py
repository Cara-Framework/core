"""Authentication-related exceptions for the Cara framework."""

from typing import Any, Dict

from .base import CaraException


class AuthenticationException(CaraException):
    """
    Base exception for authentication-related errors.
    HTTP 401 Unauthorized.
    """

    is_http_exception = True
    status_code = 401

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for JSON response."""
        return {
            "error": str(self),
            "type": "authentication_error",
        }


class InvalidTokenException(AuthenticationException):
    """Thrown when a JWT or session token is invalid."""

    pass


class AuthenticationConfigurationException(CaraException):
    """
    Exception raised when authentication configuration is invalid or missing.

    This includes missing secrets, invalid drivers, or malformed configuration.
    This is not an HTTP exception as it's a server configuration issue.
    """

    pass


class TokenExpiredException(AuthenticationException):
    """
    Exception raised when an authentication token has expired.
    """

    pass


class TokenInvalidException(AuthenticationException):
    """
    Exception raised when an authentication token is invalid.
    """

    pass


class TokenBlacklistedException(AuthenticationException):
    """
    Exception raised when an authentication token has been blacklisted.
    """

    pass


class UserNotFoundException(AuthenticationException):
    """
    Exception raised when a user cannot be found.
    """

    pass


class ApiKeyInvalidException(AuthenticationException):
    """
    Exception raised when an API key is invalid or not found.
    """

    pass
