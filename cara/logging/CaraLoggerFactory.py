"""Canonical definition of ``CaraLoggerFactory``."""

from __future__ import annotations

import logging

from .CaraPythonLoggerAdapter import CaraPythonLoggerAdapter


class CaraLoggerFactory:
    """
    Factory for creating library-specific Cara loggers.
    Laravel-style: each library gets its own configured logger instance.
    """

    @staticmethod
    def create_for_library(
        library_name: str, level: str = "INFO"
    ) -> CaraPythonLoggerAdapter:
        """
        Create a Cara-compatible logger for external library.

        Args:
            library_name: Name of the external library (e.g., 'httpx', 'pika')
            level: Log level for this library

        Returns:
            CaraPythonLoggerAdapter instance
        """
        # Convert string level to logging constant
        numeric_level = getattr(logging, level.upper(), logging.INFO)

        # Create adapter
        adapter = CaraPythonLoggerAdapter(library_name, numeric_level)

        return adapter
