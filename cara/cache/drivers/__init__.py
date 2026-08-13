"""Cache — layer barrel (generated, DOCTRINE §5.1). — drivers subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "FileCacheDriver": (".FileCacheDriver", "FileCacheDriver"),
    "RedisCacheDriver": (".RedisCacheDriver", "RedisCacheDriver"),
}

__all__ = [
    "FileCacheDriver",
    "RedisCacheDriver",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
