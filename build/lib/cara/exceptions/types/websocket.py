"""
WebSocket Exception Types for the Cara framework.

This module defines exception types related to WebSocket operations.
"""

from typing import Optional

from .base import CaraException


class WebSocketException(CaraException):
    """
    Base for WebSocket-specific exceptions.

    Error codes:
    - 4000: Generic error
    - 4001: Message handling error
    - 4002: Connection setup error
    - 4003: Protocol error
    - 4004: Route not found
    - 4005: Middleware error
    - 4006: Authentication error
    - 4007: Authorization error
    - 4008: Rate limit exceeded
    - 4009: Invalid message format
    - 4010: Connection timeout
    - 4011: Server error
    """

    def __init__(self, message: str, code: Optional[int] = 4000):
        super().__init__(message)
        self.code = code

    def __str__(self):
        return f"[{self.code}] {super().__str__()}"
