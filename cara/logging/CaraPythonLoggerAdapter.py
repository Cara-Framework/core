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
        return self.category_for(library_name)

    @staticmethod
    def category_for(library_name: str) -> str:
        """The Cara category of a third-party stdlib logger name.

        A static mapping so the root bridge in ``PythonLoggerAdapter`` files a
        CHILD logger's record (``pika.channel``) under the same category as
        its library's own logger (``pika``).
        """
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
        return self.display_name_for(self.library_name)

    @classmethod
    def display_name_for(cls, library_name: str) -> str:
        """The module-column name of a third-party stdlib logger name."""
        name = cls._LIBRARY_DISPLAY_NAMES.get(library_name)
        if name:
            return name
        # A child logger (``pika.channel``) shows its library's name.
        name = cls._LIBRARY_DISPLAY_NAMES.get(library_name.split(".")[0])
        if name:
            return name
        return library_name.split(".")[0].capitalize()

    #: Library categories whose INFO is chatter, demoted to DEBUG.
    _EXTERNAL_DEBUG_CATEGORIES = frozenset(
        {"external.httpx", "external.uvicorn", "external.eloquent"}
    )

    @classmethod
    def cara_level_for(cls, levelno: int, category: str) -> str:
        """The Cara level name of a stdlib level number for one category."""
        if levelno >= logging.ERROR:
            cara_level = "error"
        elif levelno >= logging.WARNING:
            cara_level = "warning"
        elif levelno >= logging.INFO:
            cara_level = "info"
        else:
            cara_level = "debug"
        if category in cls._EXTERNAL_DEBUG_CATEGORIES and cara_level == "info":
            cara_level = "debug"
        return cara_level

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

        cara_level = self.cara_level_for(level, self.category)

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
