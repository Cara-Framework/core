"""
Console Logging Channel for the Cara framework.

This module provides a logging channel that outputs log records to the console.
"""

import sys
from typing import Any


class ConsoleChannel:
    """
    Writes log messages to stdout or stderr.

    Loguru will call write(message) for each record.
    """

    def __init__(self, target: str = "stdout") -> None:
        self._target = target

    def write(self, message: Any) -> None:
        """Called by Loguru—simply write to the selected stream."""
        try:
            if self._target == "stderr":
                sys.stderr.write(message)
                sys.stderr.flush()
            else:
                sys.stdout.write(message)
                sys.stdout.flush()
        except BrokenPipeError:
            # Ignore broken pipe errors (e.g., when output is piped to head/tail)
            pass

    def flush(self) -> None:
        """No special flush logic needed."""
        pass
