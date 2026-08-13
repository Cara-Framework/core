"""Mocking utilities for the Cara testing framework."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Mock": (".Mock", "Mock"),
    "Spy": (".Spy", "Spy"),
}

__all__ = [
    "Mock",
    "Spy",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
