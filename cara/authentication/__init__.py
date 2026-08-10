"""
Clean Authentication System for Cara Framework.

Provides authentication guards, managers, and policy primitives.
"""

from .Authentication import Authentication
from .LoginAttemptTracker import LoginAttemptTracker, LoginLocked
from .PasswordPolicy import (
    COMMON_PASSWORD_PREFIXES,
    MAX_PASSWORD_BYTES,
    MIN_UNIQUE_CHARS,
    check_password_strength,
    denied_prefixes,
    password_max_bytes,
)
from .SessionPolicy import AUTH_SECURITY_MAX_WINDOW
from .SignInHint import SignInHint
from .TwoFactor import TwoFactor
from .AuthenticationProvider import AuthenticationProvider

__all__ = [
    "AUTH_SECURITY_MAX_WINDOW",
    "COMMON_PASSWORD_PREFIXES",
    "Authentication",
    "AuthenticationProvider",
    "LoginAttemptTracker",
    "LoginLocked",
    "MAX_PASSWORD_BYTES",
    "MIN_UNIQUE_CHARS",
    "SignInHint",
    "TwoFactor",
    "check_password_strength",
    "denied_prefixes",
    "password_max_bytes",
]
