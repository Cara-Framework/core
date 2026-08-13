"""Canonical definition of ``BaseCast``."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseCast(ABC):
    """
    Abstract base class for all casts.

    All casts must implement get() and set() methods.
    """

    def __init__(self, *args, **kwargs):
        """Initialize cast with parameters."""
        pass

    @abstractmethod
    def get(self, value: Any) -> Any:
        """
        Transform value when retrieving from database.

        Args:
            value: Raw value from database

        Returns:
            Transformed value for application use
        """
        pass

    @abstractmethod
    def set(self, value: Any) -> Any:
        """
        Transform value when storing to database.

        Args:
            value: Application value

        Returns:
            Value ready for database storage
        """
        pass
