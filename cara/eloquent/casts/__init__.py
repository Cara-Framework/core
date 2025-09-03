"""
Cara ORM Advanced Cast System

Provides powerful, extensible data transformation and validation for model attributes.
Following SOLID principles with clean, simple interfaces.
"""

# Import all base components
from .base import BaseCast, CastRegistry, cast_registry
# Import collection casts
from .collections import ArrayCast, CollectionCast
# Import datetime casts
from .datetime import DateCast, DateTimeCast, TimestampCast, TimeCast
# Import primitive casts
from .primitives import BoolCast, DecimalCast, FloatCast, IntCast, JsonCast
# Import security casts
from .security import EncryptedCast, HashCast, TokenCast
# Import validation casts
from .validation import EmailCast, PhoneCast, SlugCast, URLCast, UUIDCast


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
        self.register('bool', BoolCast)
        self.register('boolean', BoolCast)
        self.register('int', IntCast)
        self.register('integer', IntCast)
        self.register('float', FloatCast)
        self.register('decimal', DecimalCast)
        self.register('json', JsonCast)
        
        # Date/time casts
        self.register('date', DateCast)
        self.register('datetime', DateTimeCast)
        self.register('timestamp', TimestampCast)
        self.register('time', TimeCast)
        
        # Collection casts
        self.register('array', ArrayCast)
        self.register('collection', CollectionCast)
        
        # Validation casts
        self.register('email', EmailCast)
        self.register('url', URLCast)
        self.register('uuid', UUIDCast)
        self.register('slug', SlugCast)
        self.register('phone', PhoneCast)
        
        # Security casts
        self.register('hash', HashCast)
        self.register('encrypted', EncryptedCast)
        self.register('token', TokenCast)
    
    def cast_value(self, cast_definition: str, value, operation: str = 'get'):
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
            if operation == 'set':
                return cast_instance.set(value)
            else:
                return cast_instance.get(value)
        
        return value
    
    def validate_cast_definition(self, cast_definition: str) -> bool:
        """Validate if a cast definition is valid."""
        cast_type = cast_definition.split(':')[0] if ':' in cast_definition else cast_definition
        return cast_type in self._casts
    
    def get_available_casts(self) -> list:
        """Get list of all available cast types."""
        return list(self._casts.keys())


# Create enhanced global registry
cast_registry = EnhancedCastRegistry()

# Convenience functions
def cast_value(cast_definition: str, value, operation: str = 'get'):
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

# Export everything
__all__ = [
    # Base components
    'BaseCast',
    'CastRegistry', 
    'cast_registry',
    'EnhancedCastRegistry',
    
    # Primitive casts
    'BoolCast',
    'IntCast', 
    'FloatCast',
    'DecimalCast',
    'JsonCast',
    
    # Date/time casts
    'DateCast',
    'DateTimeCast', 
    'TimestampCast',
    'TimeCast',
    
    # Collection casts
    'ArrayCast',
    'CollectionCast',
    
    # Validation casts
    'EmailCast',
    'URLCast', 
    'UUIDCast',
    'SlugCast',
    'PhoneCast',
    
    # Security casts
    'HashCast',
    'EncryptedCast',
    'TokenCast',
    
    # Convenience functions
    'cast_value',
    'register_cast',
    'get_cast_instance',
] 