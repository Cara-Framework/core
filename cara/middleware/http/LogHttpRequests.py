"""
HTTP Request Logging Middleware for the Cara framework.

This middleware logs HTTP requests with beautiful colorful output using ANSI escape codes.
This is the primary HTTP access log middleware and must be first in the middleware stack.
"""

import time
from typing import Callable

from cara.facades import Log
from cara.http import Request
from cara.middleware import Middleware


class LogHttpRequests(Middleware):
    """Log HTTP requests with beautiful timing and colorful information."""

    # ANSI Color codes
    COLORS = {
        "reset": "\033[0m",
        "bold": "\033[1m",
        "dim": "\033[2m",
        "cyan": "\033[96m",
        "yellow": "\033[93m",
        "green": "\033[92m",
        "blue": "\033[94m",
        "red": "\033[91m",
        "magenta": "\033[95m",
        "white": "\033[97m",
    }

    async def handle(self, request: Request, next: Callable):
        """Handle the HTTP request and log it."""
        start_time = time.time()

        # Get client IP (sync method)
        client_ip = request.ip() or "unknown"

        # Process the request
        response = await next(request)

        # Calculate timing
        duration_ms = round((time.time() - start_time) * 1000, 2)

        # Build log message using sync and async methods appropriately
        method = request.method  # sync property
        path = request.path  # sync property

        # Get query string from scope if available
        query_string = request.scope.get("query_string", b"").decode()
        if query_string:
            path += f"?{query_string}"

        status_code = response.get_status_code()
        status_color, status_symbol = self._get_status_info(status_code)

        # Build beautiful log message (similar to LogResponses format)
        colored_message = (
            f"{self.COLORS['dim']}🌐 HTTP:{self.COLORS['reset']} "
            f"{self.COLORS['cyan']}{client_ip}{self.COLORS['reset']} "
            f"{self.COLORS['dim']}->{self.COLORS['reset']} "
            f"{self.COLORS['bold']}{method}{self.COLORS['reset']} "
            f"{self.COLORS['magenta']}{path}{self.COLORS['reset']} "
            f"{status_symbol} "
            f"{self.COLORS['bold']}{status_color}{status_code}{self.COLORS['reset']} "
            f"{self.COLORS['dim']}|{self.COLORS['reset']} "
            f"{self.COLORS['yellow']}{duration_ms}ms{self.COLORS['reset']}"
        )

        # Log the beautiful message
        Log.info(colored_message, category="cara.http.requests")

        return response

    def _get_status_info(self, status_code: int) -> tuple[str, str]:
        """Get color and symbol for HTTP status code."""
        if 200 <= status_code < 300:
            return self.COLORS["green"], "✓"
        elif 300 <= status_code < 400:
            return self.COLORS["blue"], "→"
        elif 400 <= status_code < 500:
            return self.COLORS["yellow"], "⚠"
        else:
            return self.COLORS["red"], "✗"
