"""TokenBlacklistedException."""

from __future__ import annotations

from .AuthenticationException import AuthenticationException


class TokenBlacklistedException(AuthenticationException):
    """
    Exception raised when an authentication token has been blacklisted.
    """

    pass
