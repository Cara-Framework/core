"""
Base HTTP Response Module.

Core response functionality with ASGI support and basic properties.
Laravel-inspired design with clean separation of concerns.
"""

import json
from typing import Any, Dict, List, Tuple, Union

from cara.http.request import HeaderBag
from cara.support.Http import HTTP_STATUS_CODES


class BaseResponse:
    """
    Base HTTP Response class with core functionality.

    Handles basic response properties, ASGI interface, and fundamental operations.
    Extended by specialized response classes for specific content types.
    """

    def __init__(self, application: Any):
        """
        Initialize a new BaseResponse instance.

        Args:
            application: The application container instance
        """
        self.application = application
        self.content = b""
        self._status = 200
        self.statuses: dict = HTTP_STATUS_CODES
        self.header_bag = HeaderBag()
        self._sent = False

    def clone_from(self, other: "BaseResponse") -> None:
        """Clone all attributes from another BaseResponse object."""
        self.application = other.application
        self.content = other.content
        self._status = other._status
        self.statuses = other.statuses.copy()
        self.header_bag = other.header_bag.copy()
        self._sent = other._sent

    def is_sent(self) -> bool:
        """Check if response has been sent."""
        return self._sent

    async def __call__(self, scope: Dict, receive: Any, send: Any) -> None:
        """Handle ASGI response."""
        if self._sent:
            return

        try:
            self.prepare_content()
            await self._send_response(scope, receive, send)
            self._sent = True
        except Exception as e:
            await self._handle_error(e, send)

    def prepare_content(self) -> None:
        """Prepare response content, ensuring it's in bytes."""
        if not isinstance(self.content, bytes):
            self.content = str(self.content).encode("utf-8")

    async def _send_response(self, scope: Dict, receive: Any, send: Any) -> None:
        """Send the response through ASGI interface."""
        headers = self.get_headers()
        await send(
            {
                "type": "http.response.start",
                "status": self._status,
                "headers": headers,
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": self.content,
                "more_body": False,
            }
        )

    async def _handle_error(self, error: Exception, send: Any) -> None:
        """Handle response errors gracefully."""
        if self._sent:
            return

        try:
            error_payload = {
                "success": False,
                "message": "Internal Server Error",
                "error": str(error),
            }
            error_content = json.dumps(error_payload).encode("utf-8")
            headers = [
                (b"content-type", b"application/json; charset=utf-8"),
                (b"content-length", str(len(error_content)).encode()),
            ]
            await send(
                {
                    "type": "http.response.start",
                    "status": 500,
                    "headers": headers,
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": error_content,
                    "more_body": False,
                }
            )
        except Exception:
            # Last resort fallback
            fallback_body = b"Internal Server Error"
            await send(
                {
                    "type": "http.response.start",
                    "status": 500,
                    "headers": [
                        (b"content-type", b"text/plain; charset=utf-8"),
                        (b"content-length", str(len(fallback_body)).encode()),
                    ],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": fallback_body,
                    "more_body": False,
                }
            )

    # =============================================================================
    # BASIC PROPERTIES AND METHODS
    # =============================================================================

    def status(self, status: Union[str, int]) -> "BaseResponse":
        """Laravel-style status setter."""
        self._status = int(status)
        return self

    def get_status_code(self) -> int:
        """Get current status code."""
        return self._status

    @property
    def status_code(self) -> int:
        """Get current status code (property for compatibility)."""
        return self._status

    def data(self) -> bytes:
        """Get response content as bytes."""
        return self.to_bytes()

    def to_bytes(self) -> bytes:
        """Convert content to bytes."""
        if isinstance(self.content, bytes):
            return self.content
        return str(self.content).encode("utf-8")

    def get_headers(self) -> List[Tuple[bytes, bytes]]:
        """Get all headers as list of byte tuples for ASGI."""
        return [(k.encode(), v.encode()) for k, v in self.header_bag.all().items()]

    def set_content(self, content: Union[str, bytes]) -> "BaseResponse":
        """
        Laravel-style content setter method.

        Usage:
            response.set_content("Hello World")  # Set content
            response.set_content(b"Binary data")  # Set binary content

        Args:
            content: Content to set

        Returns:
            BaseResponse: Self for method chaining
        """
        # Set content using content attribute
        if isinstance(content, str):
            self.content = content.encode("utf-8")
        else:
            self.content = content
        return self
