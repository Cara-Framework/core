"""
HTTP Colorizer for the Cara framework.

This module provides HTTP request colorization for logs.
"""

import re


class HttpColorizer:
    """HTTP request colorizer for special formatting."""

    # ANSI color codes
    RESET = "\x1b[0m"
    BOLD = "\x1b[1m"

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
    def colorize_http_message(cls, message: str) -> str:
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
