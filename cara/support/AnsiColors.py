"""Canonical definition of ``AnsiColors``."""

from __future__ import annotations


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
