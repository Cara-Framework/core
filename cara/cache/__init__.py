"""Cache — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Cache": (".Cache", "Cache"),
    "CacheContract": (".contracts", "CacheContract"),
    "CacheLock": (".CacheLock", "CacheLock"),
    "CacheProvider": (".CacheProvider", "CacheProvider"),
    "CacheTaggedStore": (".CacheTaggedStore", "CacheTaggedStore"),
    "FileCacheDriver": (".drivers", "FileCacheDriver"),
    "JsonCacheCodec": (".codecs", "JsonCacheCodec"),
    "RedisCacheDriver": (".drivers", "RedisCacheDriver"),
    "VersionedCache": (".VersionedCache", "VersionedCache"),
    "install_cache_metrics_observer": (".Observer", "install_cache_metrics_observer"),
    "notify_cache_event": (".Observer", "notify_cache_event"),
    "register_cache_scopes": (".Observer", "register_cache_scopes"),
    "scope_for_cache_key": (".Observer", "scope_for_cache_key"),
    "set_cache_observer": (".Observer", "set_cache_observer"),
}

__all__ = [
    "Cache",
    "CacheContract",
    "CacheLock",
    "CacheProvider",
    "CacheTaggedStore",
    "FileCacheDriver",
    "JsonCacheCodec",
    "RedisCacheDriver",
    "VersionedCache",
    "install_cache_metrics_observer",
    "notify_cache_event",
    "register_cache_scopes",
    "scope_for_cache_key",
    "set_cache_observer",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
