"""Configuration-related exceptions for the Cara framework."""

from .base import CaraException


class ConfigurationException(CaraException):
    """Base class for configuration-related exceptions."""

    pass


class InvalidConfigurationLocationException(ConfigurationException):
    """
    Exception raised when configuration location is invalid or inaccessible.
    """

    pass


class InvalidConfigurationSetupException(ConfigurationException):
    """
    Exception raised when configuration setup is invalid or incomplete.
    """

    pass
