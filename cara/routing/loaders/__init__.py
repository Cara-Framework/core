"""Routing — layer barrel (generated, DOCTRINE §5.1). — loaders subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ControllerRouteLoader": (".ControllerRouteLoader", "ControllerRouteLoader"),
    "ExplicitRouteLoader": (".ExplicitRouteLoader", "ExplicitRouteLoader"),
    "FunctionRouteLoader": (".FunctionRouteLoader", "FunctionRouteLoader"),
}

__all__ = [
    "ControllerRouteLoader",
    "ExplicitRouteLoader",
    "FunctionRouteLoader",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
