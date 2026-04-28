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

    # Generic message for unexpected 5xx errors when not in debug. The real
    # exception still hits the logs (with exc_info) — we just don't ship
    # internals (SQL errors, file paths, lib stack frames) to the caller.
    _GENERIC_5XX_MESSAGE = "Internal server error"

    def format_error(self, exception: Exception, status_code: int) -> Dict[str, Any]:
        """Format general errors."""
        debug = self.is_debug_mode()

        # In production, redact the raw exception message for any unexpected
        # 5xx — `str(exception)` can carry SQL fragments, library internals,
        # or filesystem paths. 4xx messages are intentional (validation /
        # not-found / forbidden) and stay verbatim so callers can act.
        if status_code >= 500 and not debug:
            response = {"error": self._GENERIC_5XX_MESSAGE}
        else:
            response = {"error": str(exception)}

        if debug:
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

            Log.error(
                f"{exception.__class__.__name__}: {str(exception)}",
                category="cara.exceptions",
                exc_info=True,
            )
        except ImportError:
            pass

    def _cors_headers_for_scope(self, scope: Dict[str, Any]) -> list:
        """Build CORS header pairs for an error response.

        Mirrors the credentials/wildcard guard in ``HandleCors``: when
        credentials are enabled we MUST NOT echo an arbitrary origin
        next to ``Access-Control-Allow-Credentials: true``. The fix in
        the live HandleCors path was useless if the exception path
        kept reflecting; both have to apply the same rule.
        """
        try:
            from cara.configuration import config

            allowed_origins = config("cors.cors.allowed_origins", ["*"])
            allowed_origins_patterns = config("cors.cors.allowed_origins_patterns", [])
            supports_credentials = config("cors.cors.supports_credentials", False)
            allowed_methods = config("cors.cors.allowed_methods", ["*"])
            allowed_headers = config("cors.cors.allowed_headers", ["*"])
            max_age = config("cors.cors.max_age", 0)
        except Exception:
            allowed_origins = ["*"]
            allowed_origins_patterns = []
            supports_credentials = False
            allowed_methods = ["*"]
            allowed_headers = ["*"]
            max_age = 0

        raw_headers = dict(scope.get("headers", []))
        origin = raw_headers.get(b"origin", b"").decode()

        def _explicit_match(o: str) -> bool:
            if not o:
                return False
            if o in allowed_origins:
                return True
            import re as _re

            for pat in allowed_origins_patterns or []:
                if _re.match(pat, o):
                    return True
            return False

        headers: list = []

        if supports_credentials:
            # Only echo when there's an explicit allowlist match;
            # never with a wildcard.
            if origin and _explicit_match(origin):
                headers.append([b"access-control-allow-origin", origin.encode()])
                headers.append([b"vary", b"Origin"])
        else:
            if "*" in allowed_origins:
                headers.append([b"access-control-allow-origin", b"*"])
            elif origin and _explicit_match(origin):
                headers.append([b"access-control-allow-origin", origin.encode()])
                headers.append([b"vary", b"Origin"])

        if allowed_methods:
            headers.append(
                [b"access-control-allow-methods", ", ".join(allowed_methods).encode()]
            )
        if allowed_headers:
            headers.append(
                [b"access-control-allow-headers", ", ".join(allowed_headers).encode()]
            )
        if supports_credentials:
            headers.append([b"access-control-allow-credentials", b"true"])
        if max_age:
            headers.append([b"access-control-max-age", str(max_age).encode()])

        return headers

    async def send_response(
        self,
        data: Dict[str, Any],
        status_code: int,
        scope: Dict[str, Any],
        receive: Any,
        send: Any,
    ) -> None:
        """Send the response."""
        cors = self._cors_headers_for_scope(scope)
        try:
            if self.application:
                response = self.application.make("response")
                response.json(data, status=status_code)
                for key, val in cors:
                    response.header(key.decode(), val.decode())
                if not scope.get("response_sent") and not response.is_sent():
                    await response(scope, receive, send)
            else:
                await self.send_manual_response(
                    data, status_code, scope, receive, send, cors
                )
        except Exception:
            await self.send_manual_response(
                data, status_code, scope, receive, send, cors
            )

    async def send_manual_response(
        self,
        data: Dict[str, Any],
        status_code: int,
        scope: Dict[str, Any],
        receive: Any,
        send: Any,
        extra_headers: Optional[list] = None,
    ) -> None:
        """Manual response fallback."""
        import json

        response_body = json.dumps(data).encode("utf-8")

        headers = [
            [b"content-type", b"application/json"],
            [b"content-length", str(len(response_body)).encode()],
        ]
        if extra_headers:
            headers.extend(extra_headers)

        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": headers,
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
        except Exception:
            return False

    def get_exception_file(self, exception: Exception) -> Optional[str]:
        """Get file where exception occurred."""
        try:
            tb = exception.__traceback__
            if tb:
                while tb.tb_next:
                    tb = tb.tb_next
                return tb.tb_frame.f_code.co_filename
        except Exception:
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
        except Exception:
            pass
        return None

    def get_trace(self, exception: Exception) -> list:
        """Get formatted traceback."""
        try:
            return traceback.format_exc().split("\n")
        except Exception:
            return []
