"""
Log Style System for the Cara framework.

This module provides structured styling for different types of logs.
"""

import re
from enum import Enum
from typing import Dict


class LogStyle(Enum):
    """Log style types for different contexts."""

    NORMAL = "normal"
    SILENT = "silent"  # Muted/gray colors
    DATABASE = "database"  # Database queries
    HTTP = "http"  # HTTP requests
    SYSTEM = "system"  # System messages
    ERROR = "error"  # Error highlighting


class ColorTheme:
    """Color theme manager for log styling."""

    # ANSI color codes
    RESET = "\x1b[0m"
    BOLD = "\x1b[1m"

    # Normal theme colors
    NORMAL_COLORS = {
        "DEBUG": "\x1b[38;5;245m",
        "INFO": "\x1b[38;2;229;192;123m",
        "WARNING": "\x1b[33m",
        "ERROR": "\x1b[31m",
        "CRITICAL": "\x1b[41m\x1b[97m",
        "time": "\x1b[38;5;136m",
        "module": "\x1b[38;2;198;120;221m",
        "service": "\x1b[38;2;97;175;239m",
        "line": "\x1b[38;5;240m",
    }

    # Silent/muted theme colors (grays)
    SILENT_COLORS = {
        "DEBUG": "\x1b[38;5;240m",  # Dark gray
        "INFO": "\x1b[38;5;242m",  # Medium gray
        "WARNING": "\x1b[38;5;244m",  # Light gray
        "ERROR": "\x1b[38;5;246m",  # Lighter gray
        "CRITICAL": "\x1b[38;5;248m",  # Very light gray
        "time": "\x1b[38;5;240m",
        "module": "\x1b[38;5;243m",
        "service": "\x1b[38;5;242m",
        "line": "\x1b[38;5;239m",
    }

    # Error theme colors (reds)
    ERROR_COLORS = {
        "DEBUG": "\x1b[38;5;203m",
        "INFO": "\x1b[38;5;205m",
        "WARNING": "\x1b[38;5;208m",
        "ERROR": "\x1b[38;5;196m",
        "CRITICAL": "\x1b[41m\x1b[97m",
        "time": "\x1b[38;5;203m",
        "module": "\x1b[38;5;205m",
        "service": "\x1b[38;5;207m",
        "line": "\x1b[38;5;203m",
    }

    # HTTP method colors
    HTTP_METHOD_COLORS = {
        "GET": "\x1b[38;5;75m",  # Blue
        "POST": "\x1b[38;5;114m",  # Green
        "PUT": "\x1b[38;5;220m",  # Yellow
        "DELETE": "\x1b[38;5;204m",  # Red
        "PATCH": "\x1b[38;5;176m",  # Purple
        "OPTIONS": "\x1b[38;5;80m",  # Cyan
    }

    @classmethod
    def get_colors(cls, style: LogStyle) -> Dict[str, str]:
        """Get color scheme for a log style."""
        if style == LogStyle.SILENT or style == LogStyle.DATABASE:
            return cls.SILENT_COLORS
        elif style == LogStyle.ERROR:
            return cls.ERROR_COLORS
        else:
            return cls.NORMAL_COLORS

    @classmethod
    def _colorize_http_message(cls, message: str) -> str:
        """Colorize HTTP request message."""
        # Pattern: "127.0.0.1 - GET /api/receipts/john-jackson?date=2025-06-28 ✓ 200 (8.36ms)"
        http_pattern = r"([^\s]+)\s+-\s+([A-Z]+)\s+([^\s]+)\s+([✓↗⚠✗])\s+(\d+)\s*(.*)"
        match = re.match(http_pattern, message)

        if match:
            ip_address = match.group(1)
            method = match.group(2)
            path = match.group(3)
            status_symbol = match.group(4)
            status_code = int(match.group(5))
            timing = match.group(6) or ""

            # Get colors
            method_color = cls.HTTP_METHOD_COLORS.get(
                method, "\x1b[37m"
            )  # White fallback
            status_color = cls._get_status_color(status_code)

            # Format path with query params
            if "?" in path:
                base_path, query_params = path.split("?", 1)
                path_colored = f"{cls.BOLD}\x1b[37m{base_path}{cls.RESET}\x1b[38;5;240m?{query_params}{cls.RESET}"
            else:
                path_colored = f"{cls.BOLD}\x1b[37m{path}{cls.RESET}"

            # Build colorized message
            colorized = (
                f"\x1b[38;5;80m{ip_address}{cls.RESET} "  # IP in cyan
                f"\x1b[38;5;240m-{cls.RESET} "  # Dash in gray
                f"{method_color}{method}{cls.RESET} "  # Method in method color
                f"{path_colored} "  # Path in bold white + gray query
                f"{status_color}{status_symbol} {status_code}{cls.RESET}"  # Status colored
            )

            if timing.strip():
                colorized += (
                    f" \x1b[38;5;240m{timing.strip()}{cls.RESET}"  # Timing in gray
                )

            return colorized

        # Fallback to original message
        return message

    @classmethod
    def _get_status_color(cls, status_code: int) -> str:
        """Get color for HTTP status code."""
        if 200 <= status_code < 300:
            return "\x1b[38;5;114m"  # Green
        elif 300 <= status_code < 400:
            return "\x1b[38;5;220m"  # Yellow
        elif 400 <= status_code < 500:
            return "\x1b[38;5;220m"  # Yellow
        else:
            return "\x1b[38;5;204m"  # Red

    @classmethod
    def format_log(
        cls,
        style: LogStyle,
        level: str,
        time_str: str,
        service: str,
        module: str,
        message: str,
        line: str,
    ) -> str:
        """Format a log message with the appropriate color theme."""
        colors = cls.get_colors(style)

        time_colored = f"{colors['time']}{time_str}{cls.RESET}"
        level_colored = (
            f"{colors[level]}{level:<8}{cls.RESET}" if level in colors else f"{level:<8}"
        )
        service_colored = f"{colors['service']}{service}{cls.RESET}"
        module_colored = f"{colors['module']}{module}{cls.RESET}"

        # Special handling for HTTP logs
        if style == LogStyle.HTTP:
            message_colored = cls._colorize_http_message(message)
        else:
            message_colored = (
                f"{colors[level]}{message}{cls.RESET}" if level in colors else message
            )

        line_colored = f"{colors['line']}{line}{cls.RESET}"

        return f"{time_colored} | {service_colored} | {module_colored} | {level_colored} | {message_colored} {line_colored}\n"
