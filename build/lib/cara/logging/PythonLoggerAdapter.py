"""
Python Logging Adapter for Cara Framework.

This adapter makes Cara Logger compatible with Python's standard logging interface,
allowing external libraries to use Cara's logging system seamlessly.
Laravel-style approach: inject our logger into external libraries with category support.
"""

import logging
from typing import Any, Dict, Optional

from cara.facades import Log
from cara.logging.LogStyle import LogStyle


class CaraPythonLoggerAdapter(logging.Logger):
    """
    Adapter that makes Cara Logger compatible with Python's logging interface.

    External libraries can use this as a standard Python logger,
    but all logs will go through Cara's logging system with consistent formatting.
    """

    def __init__(self, name: str, level: int = logging.NOTSET):
        """Initialize the adapter."""
        super().__init__(name, level)
        self.cara_logger = Log
        self.library_name = name
        self.category = self._get_category_for_library(name)

    def _get_category_for_library(self, library_name: str) -> str:
        """Map external library names to Cara categories."""
        library_category_map = {
            # External Web Server & HTTP
            "uvicorn.error": "external.uvicorn",
            "uvicorn.access": "external.uvicorn",
            "uvicorn": "external.uvicorn",
            "httpx": "external.httpx",
            "httpcore": "external.httpx",
            "urllib3": "external.httpx",
            "requests": "external.httpx",
            # External Database & ORM
            "eloquent.models.hydrate": "external.eloquent",
            "eloquent.models": "external.eloquent",
            "eloquent": "external.eloquent",
            # External Message Queues
            "pika": "external.pika",
            "amqp": "external.pika",
            "rabbitmq": "external.pika",
            # External Cache & Redis (not in config but for future)
            "redis": "external.redis",
            "aioredis": "external.redis",
            # External Scheduler (maps to uvicorn for simplicity)
            "apscheduler": "external.uvicorn",
            # External WebSockets (maps to cara websocket)
            "websockets": "cara.websocket",
        }

        # Try exact match first
        if library_name in library_category_map:
            return library_category_map[library_name]

        # Try prefix matching
        for lib_prefix, category in library_category_map.items():
            if library_name.startswith(lib_prefix):
                return category

        # Default fallback
        return f"external.{library_name.split('.')[0]}"

    def _log(
        self,
        level: int,
        msg: Any,
        args: tuple,
        exc_info=None,
        extra: Optional[Dict] = None,
        stack_info: bool = False,
    ):
        """Internal log method that forwards to Cara Logger."""
        if not self.isEnabledFor(level):
            return

        # Convert Python logging level to Cara level
        level_mapping = {
            logging.DEBUG: "debug",
            logging.INFO: "info",
            logging.WARNING: "warning",
            logging.ERROR: "error",
            logging.CRITICAL: "error",
        }

        cara_level = level_mapping.get(level, "info")

        # Override level for specific external categories that should be DEBUG
        external_debug_categories = {
            "external.httpx",
            "external.uvicorn",
            "external.eloquent",
        }
        if self.category in external_debug_categories and cara_level == "info":
            cara_level = "debug"

        # Format message with args
        if args:
            try:
                message = msg % args
            except (TypeError, ValueError):
                message = str(msg)
        else:
            message = str(msg)

        # Clean up message prefix if it already contains library name
        if message.startswith(f"[{self.library_name}]"):
            clean_message = message[len(f"[{self.library_name}]") :].strip()
        else:
            clean_message = message

        # Forward to Cara Logger with category
        getattr(self.cara_logger, cara_level)(
            clean_message,
            category=self.category,
            style=LogStyle.NORMAL,
            exc_info=exc_info,
        )


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


def install_cara_loggers():
    """
    Install Cara loggers for all external libraries.
    Laravel-style: inject our logger into external libraries.
    """
    # Laravel-style library configuration with appropriate defaults
    library_configs = {
        # Web Server (usually noisy, so DEBUG by default)
        "uvicorn.error": "INFO",  # Server errors - keep visible
        "uvicorn.access": "DEBUG",  # Access logs - usually too noisy
        "uvicorn": "DEBUG",  # General uvicorn logs
        # HTTP Clients (can be useful for debugging API calls)
        "httpx": "INFO",  # HTTP requests - good for debugging
        "httpcore": "DEBUG",  # Low-level HTTP - usually too detailed
        "urllib3": "DEBUG",  # urllib3 details - noisy
        "requests": "INFO",  # HTTP requests - useful
        # Database ORM (very noisy)
        "eloquent.models.hydrate": "DEBUG",  # Model hydration - very noisy
        "eloquent.models": "DEBUG",  # General model logs
        "eloquent": "DEBUG",  # ORM logs
        # Message Queues (important for debugging)
        "pika": "INFO",  # RabbitMQ - keep for queue debugging
        # Cache & Storage
        "redis": "DEBUG",  # Redis operations - can be noisy
        "aioredis": "DEBUG",  # Async Redis
        # Scheduler & Background Tasks
        "apscheduler": "WARNING",  # Scheduler - only important events
        # WebSockets (connection events)
        "websockets": "INFO",  # WebSocket connections - useful
    }

    for library_name, level in library_configs.items():
        # Create Cara-compatible logger
        cara_logger = CaraLoggerFactory.create_for_library(library_name, level)

        # Replace the library's logger
        original_logger = logging.getLogger(library_name)
        original_logger.handlers.clear()
        original_logger.propagate = False
        original_logger.setLevel(getattr(logging, level))

        # Monkey patch to use our adapter
        logging.getLogger(library_name).__class__ = CaraPythonLoggerAdapter
        logging.getLogger(library_name).cara_logger = Log
        logging.getLogger(library_name).library_name = library_name
        logging.getLogger(library_name).category = cara_logger.category
