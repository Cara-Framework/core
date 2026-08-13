"""Eloquent — layer barrel (generated, DOCTRINE §5.1). — schema subpackage. — platforms subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Platform": (".Platform", "Platform"),
    "PostgresPlatform": (".PostgresPlatform", "PostgresPlatform"),
    "SQLitePlatform": (".SQLitePlatform", "SQLitePlatform"),
}

__all__ = [
    "Platform",
    "PostgresPlatform",
    "SQLitePlatform",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
