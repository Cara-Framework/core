"""
Base Cast System for Cara ORM

Provides the foundation for all cast types with a registry pattern.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type


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


class CastRegistry:
    """
    Registry for managing cast types and their instantiation.

    Supports parametrized casts and provides a clean API.
    """

    def __init__(self):
        self._casts: Dict[str, Type[BaseCast]] = {}

    def register(self, name: str, cast_class: Type[BaseCast]) -> None:
        """Register a cast type."""
        self._casts[name] = cast_class

    def get_cast_instance(self, cast_definition: str) -> Optional[BaseCast]:
        """
        Create cast instance from definition string.

        Supports formats like:
        - "datetime"
        - "datetime:Y-m-d H:i:s"
        - "datetime:Y-m-d H:i:s,Europe/Istanbul"
        - "array:int"
        - "hash:bcrypt"

        Args:
            cast_definition: Cast definition string

        Returns:
            Cast instance or None if not found
        """
        if ":" in cast_definition:
            cast_type, params = cast_definition.split(":", 1)
            return self._create_parametrized_cast(cast_type, params)
        else:
            return self._create_simple_cast(cast_definition)

    def _create_simple_cast(self, cast_type: str) -> Optional[BaseCast]:
        """Create cast without parameters."""
        if cast_type in self._casts:
            return self._casts[cast_type]()
        return None

    def _create_parametrized_cast(
        self, cast_type: str, params: str
    ) -> Optional[BaseCast]:
        """Create cast with parameters."""
        if cast_type not in self._casts:
            return None

        cast_class = self._casts[cast_type]

        # Handle different parameter formats
        if cast_type == "datetime":
            return self._create_datetime_cast(cast_class, params)
        elif cast_type in ["array", "hash"]:
            return cast_class(params)
        else:
            # Generic single parameter
            return cast_class(params)

    def _create_datetime_cast(self, cast_class: Type[BaseCast], params: str) -> BaseCast:
        """Create datetime cast with format and timezone."""
        parts = [p.strip() for p in params.split(",")]
        format_str = parts[0] if parts else None
        timezone = parts[1] if len(parts) > 1 else "UTC"
        return cast_class(format_str, timezone)

    def list_casts(self) -> Dict[str, Type[BaseCast]]:
        """Get all registered casts."""
        return self._casts.copy()


# Global registry instance
cast_registry = CastRegistry()
