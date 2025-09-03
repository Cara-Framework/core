"""
Default Exception Handler.

Professional exception handler using proper exception hierarchy.
"""

import traceback
from typing import Any, Dict, Optional


class DefaultExceptionHandler:
    """
    Professional exception handler using exception class hierarchy.
    """

    def __init__(self, application=None):
        self.application = application

    async def handle(
        self,
        exception: Exception,
        request: Any,
        scope: Dict[str, Any],
        receive: Any,
        send: Any,
    ) -> None:
        """Main entry point - handles exception properly."""
        self.log_exception(exception)
        status_code = self.get_status_code(exception)
        response_data = self.format_response(exception, status_code)
        await self.send_response(response_data, status_code, scope, receive, send)

    def get_status_code(self, exception: Exception) -> int:
        """Get HTTP status code from exception - Laravel style."""
        # First check instance attribute (for dynamic setting)
        if hasattr(exception, "status_code") and exception.status_code is not None:
            return exception.status_code

        # Then check class attribute (Laravel style)
        if (
            hasattr(exception.__class__, "status_code")
            and exception.__class__.status_code is not None
        ):
            return exception.__class__.status_code

        # Default to 500 for unknown exceptions
        return 500

    def format_response(self, exception: Exception, status_code: int) -> Dict[str, Any]:
        """Format the exception into a response."""
        # If exception has its own to_dict method, use it
        if hasattr(exception, "to_dict") and callable(exception.to_dict):
            return exception.to_dict()

        # Default formatting for exceptions without to_dict
        return self.format_error(exception, status_code)

    def format_error(self, exception: Exception, status_code: int) -> Dict[str, Any]:
        """Format general errors."""
        response = {"error": str(exception)}

        if self.is_debug_mode():
            response.update(
                {
                    "type": exception.__class__.__name__,
                    "file": self.get_exception_file(exception),
                    "line": self.get_exception_line(exception),
                    "trace": self.get_trace(exception),
                }
            )

        return response

    def log_exception(self, exception: Exception) -> None:
        """Log the exception."""
        try:
            from cara.facades import Log

            Log.error(f"{exception.__class__.__name__}: {str(exception)}")
        except:
            print(f"🚨 {exception.__class__.__name__}: {str(exception)}")

    async def send_response(
        self,
        data: Dict[str, Any],
        status_code: int,
        scope: Dict[str, Any],
        receive: Any,
        send: Any,
    ) -> None:
        """Send the response."""
        try:
            if self.application:
                response = self.application.make("response")
                response.json(data, status=status_code)
                if not scope.get("response_sent") and not response.is_sent():
                    await response(scope, receive, send)
            else:
                await self.send_manual_response(data, status_code, scope, receive, send)
        except Exception:
            await self.send_manual_response(data, status_code, scope, receive, send)

    async def send_manual_response(
        self,
        data: Dict[str, Any],
        status_code: int,
        scope: Dict[str, Any],
        receive: Any,
        send: Any,
    ) -> None:
        """Manual response fallback."""
        import json

        response_body = json.dumps(data).encode("utf-8")

        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(response_body)).encode()],
                ],
            }
        )

        await send(
            {
                "type": "http.response.body",
                "body": response_body,
            }
        )

    def is_debug_mode(self) -> bool:
        """Check if in debug mode."""
        try:
            from cara.configuration import config

            return config("app.debug", False)
        except:
            return False

    def get_exception_file(self, exception: Exception) -> Optional[str]:
        """Get file where exception occurred."""
        try:
            tb = exception.__traceback__
            if tb:
                while tb.tb_next:
                    tb = tb.tb_next
                return tb.tb_frame.f_code.co_filename
        except:
            pass
        return None

    def get_exception_line(self, exception: Exception) -> Optional[int]:
        """Get line where exception occurred."""
        try:
            tb = exception.__traceback__
            if tb:
                while tb.tb_next:
                    tb = tb.tb_next
                return tb.tb_lineno
        except:
            pass
        return None

    def get_trace(self, exception: Exception) -> list:
        """Get formatted traceback."""
        try:
            return traceback.format_exc().split("\n")
        except:
            return []
