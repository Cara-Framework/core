"""Cara Context Module."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "CENTRAL": (".Tenancy", "CENTRAL"),
    "ExecutionContext": (".ExecutionContext", "ExecutionContext"),
    "Tenancy": (".Tenancy", "Tenancy"),
    "UNSET": (".Tenancy", "UNSET"),
}

__all__ = [
    "CENTRAL",
    "ExecutionContext",
    "Tenancy",
    "UNSET",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
