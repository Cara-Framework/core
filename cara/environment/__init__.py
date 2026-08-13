"""Environment — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "LoadEnvironment": (".LoadEnvironment", "LoadEnvironment"),
    "PathManager": (".PathManager", "PathManager"),
    "env": (".LoadEnvironment", "env"),
}

__all__ = [
    "LoadEnvironment",
    "PathManager",
    "env",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
