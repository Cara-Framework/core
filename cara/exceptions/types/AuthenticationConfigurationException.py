"""AuthenticationConfigurationException."""

from __future__ import annotations

from .CaraException import CaraException


class AuthenticationConfigurationException(CaraException):
    """
    Exception raised when authentication configuration is invalid or missing.

    This includes missing secrets, invalid drivers, or malformed configuration.
    This is not an HTTP exception as it's a server configuration issue.
    """

    pass
