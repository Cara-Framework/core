"""
Clean Authentication System for Cara Framework.

Provides authentication guards, managers, and policy primitives.
"""

from .Authentication import Authentication
from .password import check_password_strength
from .AuthenticationProvider import AuthenticationProvider

__all__ = [
    "Authentication",
    "AuthenticationProvider",
    "check_password_strength",
]
