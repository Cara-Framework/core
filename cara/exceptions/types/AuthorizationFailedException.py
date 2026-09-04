"""AuthorizationFailedException."""

from __future__ import annotations

from .AuthorizationException import AuthorizationException


class AuthorizationFailedException(AuthorizationException):
    """Thrown when a user is not authorized to perform an action."""
