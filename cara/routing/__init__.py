"""Routing — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "CompilerRuleMapper": (".CompilerRuleMapper", "CompilerRuleMapper"),
    "ControllerRouteLoader": (".loaders", "ControllerRouteLoader"),
    "ExplicitRouteLoader": (".loaders", "ExplicitRouteLoader"),
    "FunctionRouteLoader": (".loaders", "FunctionRouteLoader"),
    "HTTP_METHODS": (".Router", "HTTP_METHODS"),
    "Route": (".Route", "Route"),
    "RouteCompiler": (".RouteCompiler", "RouteCompiler"),
    "RouteGroup": (".RouteGroup", "RouteGroup"),
    "RouteParameterValidator": (".RouteParameterValidator", "RouteParameterValidator"),
    "RouteProvider": (".RouteProvider", "RouteProvider"),
    "RouteResolver": (".RouteResolver", "RouteResolver"),
    "Router": (".Router", "Router"),
}

__all__ = [
    "CompilerRuleMapper",
    "ControllerRouteLoader",
    "ExplicitRouteLoader",
    "FunctionRouteLoader",
    "HTTP_METHODS",
    "Route",
    "RouteCompiler",
    "RouteGroup",
    "RouteParameterValidator",
    "RouteProvider",
    "RouteResolver",
    "Router",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
