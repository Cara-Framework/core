"""
Log Colors for HTTP Request Logging.

This module provides ANSI color codes and formatting for HTTP request logs.
"""

import re
from enum import Enum


class LogType(Enum):
    """HTTP log types for different colorization styles."""

    HTTP = "http"
    ERROR = "error"
    WARNING = "warning"
    SUCCESS = "success"
    DEFAULT = "default"


class AnsiColors:
    """ANSI color codes for terminal output."""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    # Method colors
    BLUE = "\033[38;5;75m"  # GET - Blue
    GREEN = "\033[38;5;114m"  # POST - Green
    YELLOW = "\033[38;5;220m"  # PUT - Yellow
    RED = "\033[38;5;204m"  # DELETE - Red
    PURPLE = "\033[38;5;176m"  # PATCH - Purple
    CYAN = "\033[38;5;80m"  # OPTIONS/IP - Cyan

    # Status colors
    SUCCESS = "\033[38;5;114m"  # 2xx - Green
    REDIRECT = "\033[38;5;220m"  # 3xx - Yellow
    CLIENT_ERR = "\033[38;5;220m"  # 4xx - Yellow
    SERVER_ERR = "\033[38;5;204m"  # 5xx - Red

    WHITE = "\033[37m"
    GRAY = "\033[38;5;240m"


class LogColors:
    """HTTP log colorization handler."""

    def __init__(self):
        self.colors = AnsiColors()

        # Uvicorn format: INFO:     127.0.0.1:59468 - "GET /api/receipts HTTP/1.1" 200 OK
        self.uvicorn_pattern = (
            r'(INFO:\s+)([0-9\.:a-zA-Z-]+)\s+-\s+"([A-Z]+)\s+([^"]+)"\s+(\d+)\s*(.*)'
        )

        # Framework format: 2025-06-28 18:36:25.036 | Library | LogHttpRequests | INFO | 127.0.0.1 - GET /api/clients ✓ 200 (15.2ms)
        self.framework_pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \| ([^|]+) \| ([^|]+) \| (INFO|DEBUG|WARNING|ERROR)\s+\| (.+)"

        self.method_colors = {
            "GET": self.colors.BLUE,
            "POST": self.colors.GREEN,
            "PUT": self.colors.YELLOW,
            "DELETE": self.colors.RED,
            "PATCH": self.colors.PURPLE,
            "OPTIONS": self.colors.CYAN,
        }

    def colorize_line(self, line: str) -> str:
        """Colorize a log line based on its type."""
        log_type = self._detect_log_type(line)

        if log_type == LogType.HTTP:
            return self._colorize_http_request(line)
        elif log_type == LogType.ERROR:
            return f"{self.colors.RED}{line}{self.colors.RESET}"
        elif log_type == LogType.WARNING:
            return f"{self.colors.YELLOW}{line}{self.colors.RESET}"
        elif log_type == LogType.SUCCESS:
            return f"{self.colors.GREEN}{line}{self.colors.RESET}"
        else:
            return f"{self.colors.YELLOW}{line}{self.colors.RESET}"

    def _detect_log_type(self, line: str) -> LogType:
        """Detect the type of log line."""
        line_lower = line.lower()

        # HTTP requests (both uvicorn and framework format)
        if self._is_http_log(line):
            return LogType.HTTP

        # Error messages
        if any(
            keyword in line_lower
            for keyword in ["error", "exception", "traceback", "failed"]
        ):
            return LogType.ERROR

        # Warning messages
        if any(keyword in line_lower for keyword in ["warning", "×", "⚠"]):
            return LogType.WARNING

        # Success messages
        if any(keyword in line_lower for keyword in ["started", "running", "listening"]):
            return LogType.SUCCESS

        return LogType.DEFAULT

    def _is_http_log(self, line: str) -> bool:
        """Check if line is an HTTP request log."""
        # Uvicorn format
        if " - " in line and any(
            method in line
            for method in ['"GET ', '"POST ', '"PUT ', '"DELETE ', '"PATCH ']
        ):
            return True

        # Framework format
        if re.match(self.framework_pattern, line):
            framework_match = re.match(self.framework_pattern, line)
            if framework_match:
                message = framework_match.group(5)
                return any(
                    method in message
                    for method in [" GET ", " POST ", " PUT ", " DELETE ", " PATCH "]
                )

        return False

    def _colorize_http_request(self, line: str) -> str:
        """Colorize HTTP request log lines."""
        # Try framework format first
        framework_match = re.match(self.framework_pattern, line)
        if framework_match:
            return self._colorize_framework_http(framework_match)

        # Try uvicorn format
        uvicorn_match = re.match(self.uvicorn_pattern, line)
        if uvicorn_match:
            return self._colorize_uvicorn_http(uvicorn_match)

        # Fallback
        return f"{self.colors.YELLOW}{line}{self.colors.RESET}"

    def _colorize_framework_http(self, match) -> str:
        """Colorize framework format HTTP logs."""
        timestamp = match.group(1)  # "2025-06-28 18:36:25.036"
        app_name = match.group(2)  # "Library"
        class_name = match.group(3)  # "LogHttpRequests"
        level = match.group(4)  # "INFO"
        message = match.group(5)  # "127.0.0.1 - GET /api/clients ✓ 200 (15.2ms)"

        # Parse HTTP message: "127.0.0.1 - GET /api/clients ✓ 200 (15.2ms)"
        http_pattern = r"([^\s]+)\s+-\s+([A-Z]+)\s+([^\s]+)\s+([✓↗⚠✗])\s+(\d+)\s*(.*)"
        http_match = re.match(http_pattern, message)

        if http_match:
            ip_address = http_match.group(1)
            method = http_match.group(2)
            path = http_match.group(3)
            status_symbol = http_match.group(4)
            status_code = int(http_match.group(5))
            timing = http_match.group(6) or ""

            method_color = self.method_colors.get(method, self.colors.WHITE)
            status_color, _ = self._get_status_display(status_code)

            # Format with framework style
            formatted_line = (
                f"{self.colors.GRAY}{timestamp}{self.colors.RESET} | "
                f"{self.colors.CYAN}{app_name}{self.colors.RESET} | "
                f"{self.colors.YELLOW}{class_name}{self.colors.RESET} | "
                f"{self.colors.WHITE}{level}{self.colors.RESET} | "
                f"{self.colors.CYAN}{ip_address}{self.colors.RESET} {self.colors.GRAY}-{self.colors.RESET} "
                f"{method_color}{method}{self.colors.RESET} "
                f"{self.colors.BOLD}{self.colors.WHITE}{path}{self.colors.RESET} "
                f"{status_color}{status_symbol} {status_code}{self.colors.RESET}"
            )

            if timing.strip():
                formatted_line += (
                    f" {self.colors.GRAY}{timing.strip()}{self.colors.RESET}"
                )

            return formatted_line

        # Fallback to simple framework format
        return (
            f"{self.colors.GRAY}{timestamp}{self.colors.RESET} | "
            f"{self.colors.CYAN}{app_name}{self.colors.RESET} | "
            f"{self.colors.YELLOW}{class_name}{self.colors.RESET} | "
            f"{self.colors.WHITE}{level}{self.colors.RESET} | "
            f"{self.colors.WHITE}{message}{self.colors.RESET}"
        )

    def _colorize_uvicorn_http(self, match) -> str:
        """Colorize uvicorn format HTTP logs."""
        info_prefix = match.group(1)  # "INFO:     "
        ip_address = match.group(2)  # "127.0.0.1:59468"
        method = match.group(3)  # "GET"
        path = match.group(4)  # "/api/receipts/john-jackson?date=2025-06-28 HTTP/1.1"
        status_code = int(match.group(5))  # 200
        extra = match.group(6) or ""  # "OK"

        # Clean up path (remove HTTP/1.1 part)
        if " HTTP/" in path:
            path = path.split(" HTTP/")[0]

        # Get method color
        method_color = self.method_colors.get(method, self.colors.WHITE)

        # Get status color and symbol
        status_color, status_symbol = self._get_status_display(status_code)

        # Format path with query params
        path_display = self._format_path(path)

        # Build colorized line
        formatted_line = (
            f"{self.colors.GRAY}{info_prefix}{self.colors.RESET}"
            f"{self.colors.CYAN}{ip_address}{self.colors.RESET} {self.colors.GRAY}-{self.colors.RESET} "
            f"{method_color}{method}{self.colors.RESET} "
            f"{path_display} "
            f"{status_color}{status_symbol} {status_code}{self.colors.RESET}"
        )

        if extra.strip():
            formatted_line += f" {self.colors.GRAY}{extra.strip()}{self.colors.RESET}"

        return formatted_line

    def _get_status_display(self, status_code: int) -> tuple[str, str]:
        """Get color and symbol for HTTP status code."""
        if 200 <= status_code < 300:
            return self.colors.SUCCESS, "✓"
        elif 300 <= status_code < 400:
            return self.colors.REDIRECT, "↗"
        elif 400 <= status_code < 500:
            return self.colors.CLIENT_ERR, "⚠"
        else:
            return self.colors.SERVER_ERR, "✗"

    def _format_path(self, path: str) -> str:
        """Format path with query parameters."""
        if "?" in path:
            base_path, query_params = path.split("?", 1)
            return f"{self.colors.BOLD}{self.colors.WHITE}{base_path}{self.colors.RESET}{self.colors.GRAY}?{query_params}{self.colors.RESET}"
        else:
            return f"{self.colors.BOLD}{self.colors.WHITE}{path}{self.colors.RESET}"
