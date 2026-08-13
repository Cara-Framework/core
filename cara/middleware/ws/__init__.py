"""Middleware — layer barrel (generated, DOCTRINE §5.1). — ws subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Authenticate": (".Authenticate", "Authenticate"),
    "LogWSRequests": (".LogWSRequests", "LogWSRequests"),
    "ResetWebSocketAuth": (".ResetWebSocketAuth", "ResetWebSocketAuth"),
    "Throttle": (".Throttle", "Throttle"),
}

__all__ = [
    "Authenticate",
    "LogWSRequests",
    "ResetWebSocketAuth",
    "Throttle",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
