"""
Cara ORM Advanced Cast System

Provides powerful, extensible data transformation and validation for model attributes.
Following SOLID principles with clean, simple interfaces.
"""

from __future__ import annotations

from cara._LazyExports import _install_lazy_exports

from .ArrayCast import ArrayCast
from .BoolCast import BoolCast
from .CastRegistry import CastRegistry
from .CollectionCast import CollectionCast
from .DateCast import DateCast
from .DateTimeCast import DateTimeCast
from .DecimalCast import DecimalCast
from .EmailCast import EmailCast
from .EncryptedCast import EncryptedCast
from .EncryptedJsonCast import EncryptedJsonCast
from .FloatCast import FloatCast
from .HashCast import HashCast
from .IntCast import IntCast
from .JsonCast import JsonCast
from .PhoneCast import PhoneCast
from .SlugCast import SlugCast
from .TimeCast import TimeCast
from .TimestampCast import TimestampCast
from .TokenCast import TokenCast
from .URLCast import URLCast
from .UUIDCast import UUIDCast


# Convenience functions
def cast_value(cast_definition: str, value, operation: str = "get"):
    """
    Convenience function to cast a value.

    Args:
        cast_definition: Cast definition string
        value: Value to cast
        operation: 'get' or 'set'

    Returns:
        Casted value
    """
    return cast_registry.cast_value(cast_definition, value, operation)


def register_cast(name: str, cast_class):
    """
    Convenience function to register a custom cast.

    Args:
        name: Name of the cast
        cast_class: Cast class that extends BaseCast
    """
    cast_registry.register(name, cast_class)


def get_cast_instance(cast_definition: str):
    """
    Convenience function to get a cast instance.

    Args:
        cast_definition: Cast definition string

    Returns:
        Cast instance or None
    """
    return cast_registry.get_cast_instance(cast_definition)


# Enhanced cast registry with auto-registration
class EnhancedCastRegistry(CastRegistry):
    """
    Enhanced cast registry with auto-registration and powerful features.
    """

    def __init__(self):
        super().__init__()
        self._auto_register_casts()

    def _auto_register_casts(self):
        """Auto-register all available casts."""
        # Primitive casts
        self.register("bool", BoolCast)
        self.register("boolean", BoolCast)
        self.register("int", IntCast)
        self.register("integer", IntCast)
        self.register("float", FloatCast)
        self.register("decimal", DecimalCast)
        self.register("json", JsonCast)

        # Date/time casts
        self.register("date", DateCast)
        self.register("datetime", DateTimeCast)
        self.register("timestamp", TimestampCast)
        self.register("time", TimeCast)

        # Collection casts
        self.register("array", ArrayCast)
        self.register("collection", CollectionCast)

        # Validation casts
        self.register("email", EmailCast)
        self.register("url", URLCast)
        self.register("uuid", UUIDCast)
        self.register("slug", SlugCast)
        self.register("phone", PhoneCast)

        # Security casts
        self.register("hash", HashCast)
        self.register("encrypted", EncryptedCast)
        self.register("encrypted_json", EncryptedJsonCast)
        self.register("token", TokenCast)

    def cast_value(self, cast_definition: str, value, operation: str = "get"):
        """
        Cast a value using the specified cast definition.

        Args:
            cast_definition: Cast definition string (e.g., 'datetime:Y-m-d')
            value: Value to cast
            operation: 'get' or 'set' operation

        Returns:
            Casted value
        """
        cast_instance = self.get_cast_instance(cast_definition)

        if cast_instance:
            if operation == "set":
                return cast_instance.set(value)
            else:
                return cast_instance.get(value)

        return value

    def validate_cast_definition(self, cast_definition: str) -> bool:
        """Validate if a cast definition is valid."""
        cast_type = (
            cast_definition.split(":")[0] if ":" in cast_definition else cast_definition
        )
        return cast_type in self._casts

    def get_available_casts(self) -> list:
        """Get list of all available cast types."""
        return list(self._casts.keys())


# Create enhanced global registry
cast_registry = EnhancedCastRegistry()


_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BaseCast": (".BaseCast", "BaseCast"),
}

__all__ = [
    "ArrayCast",
    "BaseCast",
    "BoolCast",
    "CastRegistry",
    "CollectionCast",
    "DateCast",
    "DateTimeCast",
    "DecimalCast",
    "EmailCast",
    "EncryptedCast",
    "EncryptedJsonCast",
    "EnhancedCastRegistry",
    "FloatCast",
    "HashCast",
    "IntCast",
    "JsonCast",
    "PhoneCast",
    "SlugCast",
    "TimeCast",
    "TimestampCast",
    "TokenCast",
    "URLCast",
    "UUIDCast",
    "cast_value",
    "get_cast_instance",
    "register_cast",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
