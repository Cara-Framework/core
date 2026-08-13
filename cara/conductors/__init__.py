"""Cara ASGI conductors — HTTP, WebSocket, and Lifespan protocol handlers."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "HttpConductor": (".http", "HttpConductor"),
    "HttpConductorProvider": (".http", "HttpConductorProvider"),
    "LifespanConductor": (".lifespan", "LifespanConductor"),
    "LifespanConductorProvider": (".lifespan", "LifespanConductorProvider"),
    "WebsocketConductor": (".websocket", "WebsocketConductor"),
    "WebsocketConductorProvider": (".websocket", "WebsocketConductorProvider"),
}

__all__ = [
    "HttpConductor",
    "HttpConductorProvider",
    "LifespanConductor",
    "LifespanConductorProvider",
    "WebsocketConductor",
    "WebsocketConductorProvider",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
