"""Conductors — layer barrel (generated, DOCTRINE §5.1). — lifespan subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "LifespanConductor": (".LifespanConductor", "LifespanConductor"),
    "LifespanConductorProvider": (
        ".LifespanConductorProvider",
        "LifespanConductorProvider",
    ),
}

__all__ = [
    "LifespanConductor",
    "LifespanConductorProvider",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
