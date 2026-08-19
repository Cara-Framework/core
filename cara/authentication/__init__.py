"""
Clean Authentication System for Cara Framework.

Provides authentication guards, managers, and policy primitives.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "AUTH_REFRESH_REPLAY_WINDOW": (
        ".SessionPolicy",
        "AUTH_REFRESH_REPLAY_WINDOW",
    ),
    "AUTH_SECURITY_MAX_WINDOW": (".SessionPolicy", "AUTH_SECURITY_MAX_WINDOW"),
    "Authenticatable": (".contracts", "Authenticatable"),
    "Authentication": (".Authentication", "Authentication"),
    "AuthenticationProvider": (".AuthenticationProvider", "AuthenticationProvider"),
    "COMMON_PASSWORD_PREFIXES": (".PasswordPolicy", "COMMON_PASSWORD_PREFIXES"),
    "DEFAULT_TTL_DAYS": (".SignInHint", "DEFAULT_TTL_DAYS"),
    "Guard": (".contracts", "Guard"),
    "JWTGuard": (".guards", "JWTGuard"),
    "LoginAttemptTracker": (".LoginAttemptTracker", "LoginAttemptTracker"),
    "LoginLocked": (".LoginLocked", "LoginLocked"),
    "MAX_PASSWORD_BYTES": (".PasswordPolicy", "MAX_PASSWORD_BYTES"),
    "MAX_TOKEN_LENGTH": (".SignInHint", "MAX_TOKEN_LENGTH"),
    "MIN_UNIQUE_CHARS": (".PasswordPolicy", "MIN_UNIQUE_CHARS"),
    "SignInHint": (".SignInHint", "SignInHint"),
    "TOKEN_TYPE_ACCESS": (".guards", "TOKEN_TYPE_ACCESS"),
    "TOKEN_TYPE_REFRESH": (".guards", "TOKEN_TYPE_REFRESH"),
    "TwoFactor": (".TwoFactor", "TwoFactor"),
    "check_password_strength": (".PasswordPolicy", "check_password_strength"),
    "denied_prefixes": (".PasswordPolicy", "denied_prefixes"),
    "password_max_bytes": (".PasswordPolicy", "password_max_bytes"),
}

__all__ = [
    "AUTH_REFRESH_REPLAY_WINDOW",
    "AUTH_SECURITY_MAX_WINDOW",
    "Authenticatable",
    "Authentication",
    "AuthenticationProvider",
    "COMMON_PASSWORD_PREFIXES",
    "DEFAULT_TTL_DAYS",
    "Guard",
    "JWTGuard",
    "LoginAttemptTracker",
    "LoginLocked",
    "MAX_PASSWORD_BYTES",
    "MAX_TOKEN_LENGTH",
    "MIN_UNIQUE_CHARS",
    "SignInHint",
    "TOKEN_TYPE_ACCESS",
    "TOKEN_TYPE_REFRESH",
    "TwoFactor",
    "check_password_strength",
    "denied_prefixes",
    "password_max_bytes",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
