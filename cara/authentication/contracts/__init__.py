"""
Authentication Contracts Package.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Authenticatable": (".Authenticatable", "Authenticatable"),
    "Guard": (".Guard", "Guard"),
}

__all__ = [
    "Authenticatable",
    "Guard",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
