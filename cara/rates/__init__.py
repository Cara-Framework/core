"""Rates — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Limit": (".Limit", "Limit"),
    "RateLimit": (".contracts", "RateLimit"),
    "RateLimitProvider": (".RateLimitProvider", "RateLimitProvider"),
    "RateLimiter": (".RateLimiter", "RateLimiter"),
    "attempt_rate_limit": (".RateLimitAuthority", "attempt_rate_limit"),
}

__all__ = [
    "Limit",
    "RateLimit",
    "RateLimitProvider",
    "RateLimiter",
    "attempt_rate_limit",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
