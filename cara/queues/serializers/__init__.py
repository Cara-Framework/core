"""Authenticated queue wire serializers."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "SignedJsonJobSerializer": (".SignedJsonJobSerializer", "SignedJsonJobSerializer"),
}

__all__ = [
    "SignedJsonJobSerializer",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
