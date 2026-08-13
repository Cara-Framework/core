"""Authentication — layer barrel (generated, DOCTRINE §5.1). — guards subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "JWTGuard": (".JWTGuard", "JWTGuard"),
    "TOKEN_TYPE_ACCESS": (".JWTGuard", "TOKEN_TYPE_ACCESS"),
    "TOKEN_TYPE_REFRESH": (".JWTGuard", "TOKEN_TYPE_REFRESH"),
}

__all__ = [
    "JWTGuard",
    "TOKEN_TYPE_ACCESS",
    "TOKEN_TYPE_REFRESH",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
