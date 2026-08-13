"""Canonical definition of ``CaraPythonLoggerAdapter``."""

from __future__ import annotations

import logging
from typing import Any

from cara.facades import Log
from cara.support import redact_log_secrets


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

    # Pretty display names for external libraries so the log module column
    # shows a meaningful service name instead of "CaraPythonLoggerAdapter".
    _LIBRARY_DISPLAY_NAMES: dict[str, str] = {
        "httpx": "Httpx",
        "httpcore": "Httpx",
        "urllib3": "Urllib3",
        "requests": "Requests",
        "uvicorn": "Uvicorn",
        "uvicorn.error": "Uvicorn",
        "uvicorn.access": "Uvicorn",
        "pika": "RabbitMQ",
        "amqp": "RabbitMQ",
        "redis": "Redis",
        "aioredis": "Redis",
        "apscheduler": "Scheduler",
        "websockets": "WebSocket",
        "eloquent": "Eloquent",
        "eloquent.models": "Eloquent",
        "eloquent.models.hydrate": "Eloquent",
    }

    @property
    def _display_name(self) -> str:
        """Return a human-friendly name for the log module column."""
        name = self._LIBRARY_DISPLAY_NAMES.get(self.library_name)
        if name:
            return name
        # Fallback: capitalize the root library name
        return self.library_name.split(".")[0].capitalize()

    def _log(
        self,
        level: int,
        msg: Any,
        args: tuple,
        exc_info=None,
        extra: dict | None = None,
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
            except TypeError, ValueError:
                message = str(msg)
        else:
            message = str(msg)

        # Clean up message prefix if it already contains library name
        if message.startswith(f"[{self.library_name}]"):
            clean_message = message[len(f"[{self.library_name}]") :].strip()
        else:
            clean_message = message

        clean_message = redact_log_secrets(clean_message)

        # Forward to Cara Logger with category and module override
        getattr(self.cara_logger, cara_level)(
            clean_message,
            category=self.category,
            exc_info=exc_info,
            _module_override=self._display_name,
        )
