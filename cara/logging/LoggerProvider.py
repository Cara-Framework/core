"""
Logger Provider for Cara Framework

This module provides the logging service provider for the Cara framework.
It configures Loguru through Cara's channel system and provides centralized logging.
"""

import logging
from typing import Any, Dict

from cara.configuration import config
from cara.foundation import Provider


class LoggerProvider(Provider):
    """
    Service provider for logging services.

    Configures and registers the logging system with the application container.
    Uses Cara's channel-based configuration system for flexible logging setup.
    """

    def register(self) -> None:
        """Register logging services with the application container."""
        logging_config = config("logging", {})

        # Validate logging config early - fail fast on startup
        self._validate_logging_config(logging_config)

        # Configure and bind Cara Logger
        self._configure_cara_logger(logging_config)

        # Configure Python logging interception if enabled
        if logging_config.get("intercept", True):
            self._configure_logging_interception(logging_config)

    def _validate_logging_config(self, logging_config: Dict[str, Any]) -> None:
        """
        Validate logging configuration early to fail fast on startup.

        Args:
            logging_config: Configuration from config/logging.py

        Raises:
            ValueError: If any channel has invalid log level
        """
        valid_levels = [
            "TRACE",
            "DEBUG",
            "INFO",
            "SUCCESS",
            "WARNING",
            "ERROR",
            "CRITICAL",
        ]

        channels = logging_config.get("channels", {})
        for channel_name, channel_config in channels.items():
            level = channel_config.get("LEVEL", "DEBUG")
            if level.upper() not in valid_levels:
                raise ValueError(
                    f"Invalid log level '{level}' for channel '{channel_name}'. "
                    f"Valid levels: {', '.join(valid_levels)}. "
                    f"Check your .env file or config/logging.py"
                )

    def _configure_cara_logger(self, logging_config: Dict[str, Any]) -> None:
        """
        Configure Cara Logger with channel-based configuration.

        Args:
            logging_config: Configuration from config/logging.py
        """
        from cara.logging import Logger

        # Set configuration for Logger class
        Logger.set_config(logging_config)

        # Create Logger instance - validation already done above
        logger_instance = Logger()

        # Bind to container for facade resolution
        self.application.bind("logger", logger_instance)

    def _configure_logging_interception(self, logging_config: Dict[str, Any]) -> None:
        """
        Laravel-style external library logging integration.

        Instead of silencing external libraries, we inject Cara Logger into them.
        This ensures all logs go through Cara's formatting system consistently.
        """
        from cara.logging.PythonLoggerAdapter import install_cara_loggers

        # Install Cara loggers for all external libraries
        # This replaces their Python loggers with Cara-compatible adapters
        install_cara_loggers()

        # Also configure some basic Python logging settings for compatibility
        logging.basicConfig(level=logging.WARNING, force=True)

        # Ensure root logger doesn't interfere
        logging.getLogger().handlers.clear()
        logging.getLogger().addHandler(logging.NullHandler())
