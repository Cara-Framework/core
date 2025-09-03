"""
Configuration Provider for the Cara framework.

This module provides the service provider that binds and loads the application configuration, making
it accessible throughout the Cara application.
"""

from cara.foundation import Provider

from .Configuration import Configuration


class ConfigurationProvider(Provider):
    """Binds a Configuration instance under "config" during registration, then actually loads all
    config files when the provider is booted."""

    def __init__(self, application):
        self.application = application

    def register(self) -> None:
        config = Configuration(self.application)
        config.load()
        self.application.bind("config", config)
