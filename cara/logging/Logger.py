"""
Logger Implementation for the Cara framework.

This module provides a clean, simple logger with style-based formatting.
"""

import inspect
import sys
import traceback
import uuid
from typing import Any, Optional, Union

from loguru import logger as _loguru_logger

from cara.logging import CategoryFilter
from cara.logging.contracts import Logger
from cara.logging.LogStyle import HttpColorizer


class Logger(Logger):
    """
    Clean Logger implementation with style-based formatting.

    Features:
    - Style-based logging (NORMAL, SILENT, DATABASE, etc.)
    - Simple API without unnecessary complexity
    - Efficient context binding
    - Clean formatter system
    """

    _initialized: bool = False
    _config: dict = {}

    @classmethod
    def force_reinitialize(cls) -> None:
        """Force re-initialization of the logger (for development)."""
        cls._initialized = False

    def __init__(self, name: str = "app", config: Optional[dict] = None) -> None:
        self._name = name
        if config:
            Logger._config = config
        if not Logger._initialized:
            self._setup_logger()

    @classmethod
    def set_config(cls, config: dict) -> None:
        """Set logging configuration."""
        cls._config = config

    def _setup_logger(self) -> None:
        """Setup loguru logger with our custom formatter."""
        if Logger._initialized:
            return

        # Configure third-party logging to avoid log pollution
        self._configure_third_party_logging()

        # Remove all existing handlers
        _loguru_logger.remove()

        # Setup channels using ChannelConfigurator
        from cara.logging.ChannelConfigurator import ChannelConfigurator

        configurator = ChannelConfigurator(_loguru_logger)
        configurator.configure()

        Logger._initialized = True

    def _configure_third_party_logging(self) -> None:
        """Configure third-party library logging levels to avoid log pollution."""
        import logging

        # Disable ALL httpx related logging
        httpx_loggers = [
            "httpx",
            "httpcore",
            "h11",
            "hpack",
            "httpcore._async",
            "httpcore._sync",
            "httpx._client",
            "httpx._config",
            "httpx._utils",
            "httpx._models",
            "httpcore.http11",
            "httpcore.http2",
            "httpcore.connection",
            "httpcore.connection_pool",
        ]

        for logger_name in httpx_loggers:
            logging.getLogger(logger_name).setLevel(logging.CRITICAL)
            # Also disable all handlers
            logger = logging.getLogger(logger_name)
            logger.handlers = []
            logger.propagate = False

        # Other third-party libraries that might be verbose
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)

    def _get_console_log_level(self) -> str:
        """Get console log level from configuration."""
        try:
            from cara.configuration import config

            # Get console channel configuration
            console_config = config("logging.channels.console", {})
            level = console_config.get("LEVEL", "DEBUG")
            return level.upper()
        except:
            # Fallback to DEBUG if config is not available
            return "DEBUG"

    def _should_log_level(self, level: str) -> bool:
        """Check if a log level should be logged based on console configuration."""
        try:
            import logging as pylogging

            # Get current console log level
            console_level = self._get_console_log_level()

            # Convert to numeric levels for comparison
            current_levelno = getattr(pylogging, level.upper(), 0)
            console_levelno = getattr(pylogging, console_level.upper(), 0)

            return current_levelno >= console_levelno
        except:
            # If anything fails, allow logging
            return True

    def _get_caller_info(self) -> tuple[str, str]:
        """Get caller module and line info efficiently."""
        frame = inspect.currentframe()
        try:
            # Go back 3 frames: _get_caller_info -> _log -> public_method -> actual_caller
            for _ in range(3):
                frame = frame.f_back
                if not frame:
                    return "App", "0"

            # Try to get class name first, then module name
            if "self" in frame.f_locals:
                cls_name = type(frame.f_locals["self"]).__name__
                return cls_name, str(frame.f_lineno)

            module_name = frame.f_globals.get("__name__", "App")
            simple_name = module_name.split(".")[-1].replace("_", "").capitalize()
            return simple_name, str(frame.f_lineno)
        except:
            return "App", "0"
        finally:
            del frame

    def _get_request_id(self) -> str:
        """Get current request ID or generate a new one."""
        try:
            from cara.http.request.context import current_request

            req = current_request.get()
            return getattr(req, "request_id", str(uuid.uuid4())[:8])
        except:
            return str(uuid.uuid4())[:8]

    def _format_exception(
        self, exc_info: Union[bool, Exception, tuple]
    ) -> Optional[str]:
        """Format exception information."""
        if not exc_info:
            return None

        if exc_info is True:
            exc_type, exc_value, exc_tb = sys.exc_info()
        elif isinstance(exc_info, Exception):
            exc_type = type(exc_info)
            exc_value = exc_info
            exc_tb = exc_info.__traceback__
        elif isinstance(exc_info, tuple) and len(exc_info) == 3:
            exc_type, exc_value, exc_tb = exc_info
        else:
            return None

        if not exc_tb:
            return None

        return "".join(traceback.format_exception(exc_type, exc_value, exc_tb))

    def _log(
        self,
        level: str,
        message: str,
        category: Optional[str] = None,
        exception: Optional[Exception] = None,
        exc_info: Union[bool, Exception, tuple, None] = None,
    ) -> None:
        """Internal logging method."""
        # Check if we should log this based on category filters
        if category and not CategoryFilter.should_log(level, category):
            return

        # For non-categorized logs, check against console log level
        if not category and not self._should_log_level(level):
            return

        # Get caller info
        module, line = self._get_caller_info()
        request_id = self._get_request_id()
        service = self._config.get("service_name", "")

        # Apply HTTP coloring if this is an HTTP request log
        if category in ("http.requests", "cara.http.requests"):
            formatted_message = HttpColorizer.colorize_http_message(message)
        else:
            formatted_message = message

        # Get log method and execute with our pre-formatted message
        log_method = getattr(_loguru_logger, level.lower())

        # Get level short form
        level_short = self._get_level_short(level)

        # Get message color based on level
        message_color = self._get_message_color(level)

        # Get level color (for level labels and brackets)
        level_color = self._get_level_color(level)

        # Bind context for format strings that might use it
        bound_logger = _loguru_logger.bind(
            module=module,
            service_name=service,
            request_id=request_id,
            level_short=level_short,
            message_color=message_color,
            level_color=level_color,
        )
        bound_log_method = getattr(bound_logger, level.lower())

        # Handle exception and exc_info
        if exc_info:
            exc_str = self._format_exception(exc_info)
            if exc_str:
                formatted_message = f"{formatted_message.strip()}\n{exc_str}"
        elif exception:
            bound_logger.opt(depth=2).log(
                level, formatted_message.strip(), exception=exception
            )
            return

        # Use depth=2 to show the actual caller's line number, not Logger._log line
        bound_logger.opt(depth=2).log(level, formatted_message.strip())

    def _get_level_short(self, level: str) -> str:
        """Convert level to short format."""
        level_map = {
            "DEBUG": "D",
            "INFO": "I",
            "WARNING": "W",
            "ERROR": "E",
            "CRITICAL": "C",
        }
        return level_map.get(level, level[0])

    def _get_message_color(self, level: str) -> str:
        """Get ANSI color code based on level."""
        color_map = {
            "DEBUG": "\x1b[38;2;102;102;102m",  # #666666 - Gray for debug
            "INFO": "\x1b[38;2;219;169;88m",  # #dba958 - Orange for info
            "WARNING": "\x1b[38;2;255;255;0m",  # #ffff00 - Yellow for warning
            "ERROR": "\x1b[38;2;255;0;0m",  # #ff0000 - Red for error
            "CRITICAL": "\x1b[38;2;255;255;255m",  # #ffffff - White for critical
        }
        return color_map.get(level, "\x1b[38;2;219;169;88m")

    def _get_level_color(self, level: str) -> str:
        """Get ANSI color code for level labels and brackets."""
        color_map = {
            "DEBUG": "\x1b[38;2;102;102;102m",  # #666666 - Gray for debug
            "INFO": "\x1b[38;2;219;169;88m",  # #dba958 - Orange for info
            "WARNING": "\x1b[38;2;255;255;0m",  # #ffff00 - Yellow for warning
            "ERROR": "\x1b[38;2;255;0;0m",  # #ff0000 - Red for error
            "CRITICAL": "\x1b[38;2;255;255;255m",  # #ffffff - White for critical
        }
        return color_map.get(level, "\x1b[38;2;219;169;88m")

    # Public API methods
    def debug(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
        exc_info: Union[bool, Exception, tuple, None] = None,
    ) -> None:
        """Log debug message."""
        self._log("DEBUG", message, category, exc_info=exc_info)

    def info(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
        exc_info: Union[bool, Exception, tuple, None] = None,
    ) -> None:
        """Log info message."""
        self._log("INFO", message, category, exc_info=exc_info)

    def warning(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
        exc_info: Union[bool, Exception, tuple, None] = None,
    ) -> None:
        """Log warning message."""
        self._log("WARNING", message, category, exc_info=exc_info)

    def error(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
        exception: Optional[Exception] = None,
        exc_info: Union[bool, Exception, tuple, None] = None,
    ) -> None:
        """Log error message."""
        self._log("ERROR", message, category, exception, exc_info)

    def critical(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
        exc_info: Union[bool, Exception, tuple, None] = None,
    ) -> None:
        """Log critical message."""
        self._log("CRITICAL", message, category, exc_info=exc_info)

    def exception(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
        exc_info: Union[bool, Exception, tuple, None] = True,
    ) -> None:
        """Log an exception message with backtrace."""
        self._log("ERROR", message, category, exc_info=exc_info)

    # Convenience methods for different styles
    def database(self, message: str, level: str = "DEBUG") -> None:
        """Log a database query (muted style)."""
        self._log(level.upper(), message, "db.queries")
