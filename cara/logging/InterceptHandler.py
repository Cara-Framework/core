"""
A logging.Handler that forwards Python stdlib log records into Loguru.

Used to intercept 'uvicorn.error' and 'uvicorn.access' and send them through Loguru.
"""

import logging
from loguru import logger as loguru_logger


class InterceptHandler(logging.Handler):
    """Intercepts stdlib log records and re-emits them to Loguru with the correct context."""

    def emit(self, record: logging.LogRecord) -> None:
        # Map the stdlib level name to Loguru's level (fallback to numeric if not found)
        try:
            level = loguru_logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Walk the frame stack to skip over logging internals, so Loguru attributes the call
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_globals.get("__name__", "").startswith("logging"):
            frame = frame.f_back
            depth += 1

        loguru_logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )
