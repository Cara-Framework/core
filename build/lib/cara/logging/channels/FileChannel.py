"""
File Channel Logger for the Cara framework.

This module implements a logging channel that writes log messages to files, supporting rotation and
formatting.

A sink wrapper for writing to a file. Loguru will interpret the string returned by __str__() as the
actual file path (including {time} placeholders).
"""

from typing import Any


class FileChannel:
    def __init__(self, filepath_template: str) -> None:
        """
        :param filepath_template: A string, possibly containing Loguru time‐format tokens,
                                  e.g. "storage/logs/app_{time:YYYY-MM-DD}.log"
        """
        self._path = filepath_template

    def write(self, message: Any) -> None:
        """
        This write() method is technically never called by Loguru, because Loguru sees FileChannel
        instance, calls str(channel), and writes directly to that file path.

        We leave write() as a no-op.
        """
        pass

    def __str__(self) -> str:
        """Return the path template for Loguru to interpret as a file sink."""
        return self._path
