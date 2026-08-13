"""Logging — layer barrel (generated, DOCTRINE §5.1). — channels subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ConsoleChannel": (".ConsoleChannel", "ConsoleChannel"),
    "FileChannel": (".FileChannel", "FileChannel"),
    "SlackChannel": (".SlackChannel", "SlackChannel"),
}

__all__ = [
    "ConsoleChannel",
    "FileChannel",
    "SlackChannel",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
