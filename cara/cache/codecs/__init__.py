"""Cache value codecs."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "JsonCacheCodec": (".JsonCacheCodec", "JsonCacheCodec"),
}

__all__ = [
    "JsonCacheCodec",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
