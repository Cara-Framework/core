"""InvalidConfigurationSetupException."""

from __future__ import annotations

from .ConfigurationException import ConfigurationException


class InvalidConfigurationSetupException(ConfigurationException):
    """
    Exception raised when configuration setup is invalid or incomplete.
    """

    pass
