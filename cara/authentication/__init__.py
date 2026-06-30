"""
Clean Authentication System for Cara Framework.

Provides authentication guards, managers, and policy primitives.
"""

from .Authentication import Authentication
from .LoginAttemptTracker import LoginAttemptTracker, LoginLocked
from .password import check_password_strength
from .AuthenticationProvider import AuthenticationProvider

__all__ = [
    "Authentication",
    "AuthenticationProvider",
    "LoginAttemptTracker",
    "LoginLocked",
    "check_password_strength",
]
