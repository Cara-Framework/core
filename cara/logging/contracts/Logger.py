"""
Logger Interface for the Cara framework.

This module defines the contract that any logger class must implement, specifying required methods
for logging operations.
"""

from abc import ABC, abstractmethod
from typing import Optional, Any


class Logger(ABC):
    """
    Contract for the Logger.

    Any concrete logger must implement these methods so that other parts of the application can rely
    on a consistent logging API.
    """

    @abstractmethod
    def debug(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
    ) -> None:
        """
        Log a debug‐level message.

        :param message: The log message template.
        :param args: Any format arguments for the message.
        :param category: Optional category name for filtering.
        """

    @abstractmethod
    def info(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
    ) -> None:
        """
        Log an info‐level message.

        :param message: The log message template.
        :param args: Any format arguments for the message.
        :param category: Optional category name for filtering.
        """

    @abstractmethod
    def warning(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
    ) -> None:
        """
        Log a warning‐level message.

        :param message: The log message template.
        :param args: Any format arguments for the message.
        :param category: Optional category name for filtering.
        """

    @abstractmethod
    def error(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """
        Log an error‐level message.

        :param message: The log message template.
        :param args: Any format arguments for the message.
        :param category: Optional category name for filtering.
        :param exception: Optional exception to capture stack trace.
        """

    @abstractmethod
    def critical(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
    ) -> None:
        """
        Log a critical‐level message.

        :param message: The log message template.
        :param args: Any format arguments for the message.
        :param category: Optional category name for filtering.
        """

    @abstractmethod
    def exception(
        self,
        message: str,
        *args: Any,
        category: Optional[str] = None,
    ) -> None:
        """
        Log an exception‐level message (same as error + backtrace).

        :param message: The log message template.
        :param args: Any format arguments for the message.
        :param category: Optional category name for filtering.
        """
