"""Rates — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Limit": (".Limit", "Limit"),
    "RateLimit": (".contracts", "RateLimit"),
    "RateLimitDecision": (".RateLimitDecision", "RateLimitDecision"),
    "RateLimitProvider": (".RateLimitProvider", "RateLimitProvider"),
    "RateLimiter": (".RateLimiter", "RateLimiter"),
    "attempt_rate_limit": (".RateLimitAuthority", "attempt_rate_limit"),
    "rate_limit_cache_key": (".RateLimitAuthority", "rate_limit_cache_key"),
}

__all__ = [
    "Limit",
    "RateLimit",
    "RateLimitDecision",
    "RateLimitProvider",
    "RateLimiter",
    "attempt_rate_limit",
    "rate_limit_cache_key",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
