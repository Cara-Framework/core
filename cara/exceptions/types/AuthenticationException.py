"""AuthenticationException."""

from __future__ import annotations

from typing import Any

from .CaraException import CaraException


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
