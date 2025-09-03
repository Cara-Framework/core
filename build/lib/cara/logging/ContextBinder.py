"""
Logging Context Binder for the Cara framework.

This module provides utilities to bind contextual information to log records for enhanced logging.
"""

from typing import Any


class ContextBinder:
    """Binds contextual data (name, module, request_id, service_name) to each log call."""

    @staticmethod
    def bind(
        logger: Any,
        name: str,
        module: str,
        request_id: str,
        service_name: str = "Library",
    ) -> Any:
        return logger.bind(
            name=name, module=module, request_id=request_id, service_name=service_name
        )
