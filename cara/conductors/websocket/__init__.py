"""Conductors — layer barrel (generated, DOCTRINE §5.1). — websocket subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "WebsocketConductor": (".WebsocketConductor", "WebsocketConductor"),
    "WebsocketConductorProvider": (
        ".WebsocketConductorProvider",
        "WebsocketConductorProvider",
    ),
}

__all__ = [
    "WebsocketConductor",
    "WebsocketConductorProvider",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
