"""
Console Output Utilities Module.

This module provides console output utilities for the Cara framework, implementing colored output
and command-line formatting with support for different message types and styles.
"""

from __future__ import annotations


class HasColoredOutput:
    """
    Base class for colored console output.

    Outputs through the Log facade when available, falling back to print for
    environments where the logger isn't bootstrapped (e.g. bare drivers).
    """

    def success(self, message):
        try:
            from cara.facades import Log

            Log.info(message)
        except Exception:
            print(f"\033[92m {message} \033[0m")

    def warning(self, message):
        try:
            from cara.facades import Log

            Log.warning(message)
        except Exception:
            print(f"\033[93m {message} \033[0m")

    def danger(self, message):
        try:
            from cara.facades import Log

            Log.error(message)
        except Exception:
            print(f"\033[91m {message} \033[0m")

    def info(self, message):
        return self.success(message)
