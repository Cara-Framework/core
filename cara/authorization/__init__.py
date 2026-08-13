"""Authorization — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "AuthorizationProvider": (".AuthorizationProvider", "AuthorizationProvider"),
    "AuthorizationResponse": (".AuthorizationResponse", "AuthorizationResponse"),
    "Gate": (".Gate", "Gate"),
    "GateContract": (".contracts", "GateContract"),
    "Policy": (".Policy", "Policy"),
    "PolicyContract": (".contracts", "PolicyContract"),
}

__all__ = [
    "AuthorizationProvider",
    "AuthorizationResponse",
    "Gate",
    "GateContract",
    "Policy",
    "PolicyContract",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
