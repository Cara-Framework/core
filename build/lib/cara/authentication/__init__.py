"""
Clean Authentication System for Cara Framework.

Provides authentication guards and managers without string checks.
"""

from .Authentication import Authentication
from .AuthenticationProvider import AuthenticationProvider

__all__ = [
    "Authentication",
    "AuthenticationProvider",
] 