"""Queues — layer barrel (generated, DOCTRINE §5.1). — retry subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "DEFAULT_MAX_ATTEMPTS": (".Policy", "DEFAULT_MAX_ATTEMPTS"),
    "DEFAULT_MAX_THROTTLE_ATTEMPTS": (".Policy", "DEFAULT_MAX_THROTTLE_ATTEMPTS"),
    "DEFAULT_RETRY_BACKOFF_SECONDS": (".Policy", "DEFAULT_RETRY_BACKOFF_SECONDS"),
    "DEFAULT_RETRY_JITTER_FRACTION": (".Policy", "DEFAULT_RETRY_JITTER_FRACTION"),
    "MakesRetryable": (".MakesRetryable", "MakesRetryable"),
}

__all__ = [
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_THROTTLE_ATTEMPTS",
    "DEFAULT_RETRY_BACKOFF_SECONDS",
    "DEFAULT_RETRY_JITTER_FRACTION",
    "MakesRetryable",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
