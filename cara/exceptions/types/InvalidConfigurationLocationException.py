"""InvalidConfigurationLocationException."""

from __future__ import annotations

from .ConfigurationException import ConfigurationException


class InvalidConfigurationLocationException(ConfigurationException):
    """
    Exception raised when configuration location is invalid or inaccessible.
    """

    pass
