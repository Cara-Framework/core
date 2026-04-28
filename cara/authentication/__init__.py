"""
Clean Authentication System for Cara Framework.

Provides authentication guards, managers, and policy primitives.
"""

from .Authentication import Authentication
from .AuthenticationProvider import AuthenticationProvider
from .password import check_password_strength

__all__ = [
    "Authentication",
    "AuthenticationProvider",
    "check_password_strength",
]
