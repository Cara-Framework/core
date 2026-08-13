"""Events — layer barrel (generated, DOCTRINE §5.1). — contracts subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "EventContract": (".EventContract", "EventContract"),
    "Listener": (".Listener", "Listener"),
}

__all__ = [
    "EventContract",
    "Listener",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
