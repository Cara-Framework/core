"""Durable delayed queue transport."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "DurableDelayedJobStore": (".DurableDelayedJobStore", "DurableDelayedJobStore"),
}

__all__ = [
    "DurableDelayedJobStore",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
