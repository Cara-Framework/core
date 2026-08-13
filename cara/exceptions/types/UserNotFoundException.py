"""UserNotFoundException."""

from __future__ import annotations

from .AuthenticationException import AuthenticationException


class UserNotFoundException(AuthenticationException):
    """
    Exception raised when a user cannot be found.
    """

    pass
