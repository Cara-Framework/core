"""Authentication-related exceptions for the Cara framework."""

from __future__ import annotations

from typing import Any

from .Base import CaraException


class AuthenticationException(CaraException):
    """
    Base exception for authentication-related errors.
    HTTP 401 Unauthorized.
    """

    is_http_exception = True
    status_code = 401

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary for JSON response."""
        return {
            "error": str(self),
            "type": "authentication_error",
        }


class InvalidTokenException(AuthenticationException):
    """Thrown when a JWT or session token is invalid."""

    pass


class AccountLockedException(CaraException):
    """Account temporarily locked (e.g. too many failed login attempts).

    HTTP 429 Too-Many-Requests rather than 401/403: the credentials may be
    correct — the lock is a brute-force policy, not an authorization
    decision. ``retry_after_seconds`` carries the remaining window.
    """

    is_http_exception = True
    status_code = 429

    def __init__(self, message: str = "Account temporarily locked", retry_after_seconds: int = 0):
        super().__init__(message)
        self.retry_after_seconds = int(retry_after_seconds)

    def to_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"error": str(self), "type": "rate_limit_exceeded"}
        if self.retry_after_seconds > 0:
            body["retry_after"] = self.retry_after_seconds
        return body


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


__all__ = [
    "AccountLockedException",
    "AuthenticationException",
    "InvalidTokenException",
    "AuthenticationConfigurationException",
    "TokenExpiredException",
    "TokenInvalidException",
    "TokenBlacklistedException",
    "UserNotFoundException",
    "ApiKeyInvalidException",
]
