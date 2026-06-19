"""
Redis-based Cache Driver for the Cara framework.

This module implements a cache driver that uses Redis as the backend storage,
supporting TTL-based expiration and all standard cache operations.
"""

from __future__ import annotations

import logging

# Payloads above this size emit a one-time warning per key so operators
# notice runaway cache-as-blob patterns (e.g. caching a full search
# response that ballooned past 1 MB). Configurable via the
# ``cache.large_value_bytes`` config key; default 256 KB.
import pickle
from typing import Any

from cara.cache.contracts import Cache
from cara.cache.observer import notify_cache_event
from cara.exceptions import CacheConfigurationException
from cara.facades import Log

_logger = logging.getLogger("cara.cache.redis")

# Resolved lazily on first cache write — NOT at import time. The driver
# module is imported during bootstrap, before ``load_dotenv`` populates
# ``load_dotenv`` from ``.env``, so an import-time read would freeze the
# default and silently ignore ``cache.large_value_bytes``. Reading on
# first use (job-processing time) guarantees the configured value wins.
_LARGE_VALUE_BYTES: int | None = None
_LARGE_VALUE_WARNED: set[str] = set()


def _large_value_threshold() -> int:
    global _LARGE_VALUE_BYTES
    if _LARGE_VALUE_BYTES is None:
        try:
            from cara.configuration import config

            _LARGE_VALUE_BYTES = int(config("cache.large_value_bytes", 262144))
        except (TypeError, ValueError):
            _LARGE_VALUE_BYTES = 262144
    return _LARGE_VALUE_BYTES


class RedisCacheDriver(Cache):
    """
    Stores cache entries in Redis.

    Keys are prefixed for namespacing. Values are pickled; TTL is applied per entry.
    """

    driver_name = "redis"

    def __init__(
        self,
        host: str,
        port: int,
        db: int,
        password: str | None,
        prefix: str = "",
        default_ttl: int = 60,
        *,
        # ── Socket / pool hardening ──────────────────────────────────
        # Bounds for redis-py's internal ConnectionPool — all optional
        # so existing callers stay source-compatible. Defaults are tuned
        # for an ASGI worker: short connect, generous read, periodic
        # health pings so a Redis blip doesn't permanently poison the
        # pool, and a hard ceiling on connection count so a slow
        # downstream can't grow the pool unboundedly under load.
        socket_connect_timeout: float | None = 5.0,
        socket_timeout: float | None = 5.0,
        socket_keepalive: bool = True,
        health_check_interval: int = 30,
        max_connections: int | None = 32,
        retry_on_timeout: bool = True,
    ):
        self._prefix = prefix or ""
        self._default_ttl = default_ttl
        self._validate_connection_params(host, port, db)
        try:
            import redis
        except ImportError as e:
            raise CacheConfigurationException(
                "redis is required for RedisCacheDriver. "
                "Please install it with: pip install redis"
            ) from e
        # ``redis.Redis`` accepts a flat kwargs surface and constructs the
        # underlying ``ConnectionPool`` for us. ``health_check_interval``
        # makes redis-py issue a PING on idle connections older than the
        # interval; without it a stale TCP connection (Redis-side restart,
        # NAT timeout, k8s service rotation) keeps getting handed out to
        # request paths until the first command fails. ``max_connections``
        # caps growth so a burst of slow requests can't exhaust file
        # descriptors. ``retry_on_timeout`` lets a single socket-timeout
        # transparently retry once before bubbling — covers the most
        # common transient network blip.
        redis_kwargs: dict = {
            "host": host,
            "port": port,
            "db": db,
            "password": password,
            "socket_connect_timeout": socket_connect_timeout,
            "socket_timeout": socket_timeout,
            "socket_keepalive": socket_keepalive,
            "health_check_interval": health_check_interval,
            "retry_on_timeout": retry_on_timeout,
        }
        if max_connections is not None:
            redis_kwargs["max_connections"] = max_connections
        # Drop None entries so we don't override redis-py's own defaults
        # (e.g. for ``password``) with explicit None values.
        redis_kwargs = {
            k: v for k, v in redis_kwargs.items() if v is not None or k == "password"
        }
        self._client = redis.Redis(**redis_kwargs)

    def _validate_connection_params(self, host: str, port: int, db: int) -> None:
        if not host or not isinstance(host, str):
            raise CacheConfigurationException(
                "`cache.drivers.redis.host` must be a non‐empty string."
            )
        if not isinstance(port, int) or port <= 0:
            raise CacheConfigurationException(
                "`cache.drivers.redis.port` must be a positive integer."
            )
        if not isinstance(db, int) or db < 0:
            raise CacheConfigurationException(
                "`cache.drivers.redis.db` must be a non‐negative integer."
            )

    def get(self, key: str, default: Any = None) -> Any:
        redis_key = f"{self._prefix}{key}"
        try:
            raw_data = self._client.get(redis_key)
        except Exception as exc:
            Log.warning("[RedisCacheDriver] GET failed for '%s': %s", key, exc, category='cache')
            notify_cache_event("get", "error", key, None)
            return default

        if raw_data is None:
            notify_cache_event("get", "miss", key, None)
            return default

        try:
            value = pickle.loads(raw_data)
            notify_cache_event("get", "hit", key, len(raw_data))
            return value
        except Exception as unpickle_error:
            try:
                value = raw_data.decode("utf-8")
                notify_cache_event("get", "hit", key, len(raw_data))
                return value
            except (OSError, RuntimeError, AttributeError, ConnectionError):
                pass
            # Self-heal a corrupt / pickle-incompatible entry instead of
            # serving the default forever. Without the proactive DELETE
            # every subsequent GET re-fetched, re-failed to unpickle,
            # and re-warned — so the same poison key burned a syscall +
            # log line on every request until the TTL expired (which,
            # for ``BRAND_VERSION_TTL`` and friends, is 30 days). Drop
            # it on first miss so the next caller's ``Cache.remember``
            # repopulates with the current pickle protocol.
            Log.warning("[RedisCacheDriver] unpickle failed for '%s' (corrupt entry, deleting): %s", key, unpickle_error, category='cache')
            try:
                self._client.delete(redis_key)
            except Exception:
                _logger.warning("self-heal delete failed", exc_info=True)
            notify_cache_event("get", "error", key, None)
            return default

    def put(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        *,
        strict: bool = False,
    ) -> None:
        redis_key = f"{self._prefix}{key}"
        try:
            payload = pickle.dumps(value)
        except Exception as e:
            # Silently swallowing serialisation failures was the
            # original bug — callers using ``Cache.add(key, True,
            # ttl)`` as a flight-claim got a silent False back, which
            # they (correctly) read as "another worker won the claim",
            # so the in-flight job never ran. Surface the real cause.
            from cara.exceptions import CacheConfigurationException

            raise CacheConfigurationException(
                f"Cannot pickle value for cache key '{key}' ({type(value).__name__}): {e}"
            ) from e

        ttl_seconds = ttl if (ttl is not None) else self._default_ttl
        payload_size = len(payload)
        _threshold = _large_value_threshold()
        if payload_size > _threshold and key not in _LARGE_VALUE_WARNED:
            _LARGE_VALUE_WARNED.add(key)
            Log.warning("[RedisCacheDriver] large cache value: key='%s' size=%sB threshold=%sB — consider normalising or sharding the payload", key, payload_size, _threshold, category='cache')
        try:
            if ttl_seconds > 0:
                self._client.set(redis_key, payload, ex=ttl_seconds)
            else:
                self._client.set(redis_key, payload)
            notify_cache_event("put", "set", key, payload_size)
        except Exception as e:
            Log.warning("[RedisCacheDriver] set failed: %s", e, category='cache')
            notify_cache_event("put", "error", key, payload_size)
            if strict:
                raise

    def forever(self, key: str, value: Any) -> None:
        self.put(key, value, ttl=0)

    def forget(self, key: str) -> bool:
        redis_key = f"{self._prefix}{key}"
        try:
            deleted = self._client.delete(redis_key) > 0
            notify_cache_event("forget", "deleted" if deleted else "noop", key, None)
            return deleted
        except Exception as e:
            # Don't swallow silently — admin invalidation paths
            # (CacheController, AdminProductRepository.invalidate_*)
            # log the count of forgotten keys, and a returned False
            # is interpreted as "key wasn't there", not "Redis is
            # down". Surface the failure so the caller's audit trail
            # records something operators can act on.
            Log.warning("[RedisCacheDriver] forget failed for '%s': %s", key, e, category='cache')
            notify_cache_event("forget", "error", key, None)
            return False

    def flush(self) -> None:
        """Flush every cache entry under our prefix.

        SECURITY — must NOT call ``flushdb()``: cara namespaces cache
        keys with ``self._prefix`` but Redis databases are typically
        shared with broadcasting state, queue jobs, sessions, rate
        limit counters, etc. Wiping the whole DB on a routine flush
        was wiping co-tenant data. We now SCAN+DEL only keys under
        our prefix.

        When the prefix is empty (config bug), refuse to flush — that
        prevents an accidental "flush all keys in this DB" outage.
        """
        if not self._prefix:
            Log.warning(
                "[RedisCacheDriver] flush() refused: empty cache prefix "
                "would wipe co-tenant Redis keys. Configure "
                "cache.drivers.redis.prefix.",
                category="cache",
            )
            return
        try:
            cursor = 0
            pattern = f"{self._prefix}*"
            while True:
                cursor, keys = self._client.scan(cursor=cursor, match=pattern, count=200)
                if keys:
                    self._client.delete(*keys)
                if cursor == 0:
                    break
        except Exception as e:
            Log.warning("[RedisCacheDriver] flush failed: %s", e, category='cache')

    def has(self, key: str) -> bool:
        """Check if a key exists in cache."""
        redis_key = f"{self._prefix}{key}"
        try:
            return self._client.exists(redis_key) > 0
        except Exception:
            _logger.warning("redis operation failed", exc_info=True)
            return False

    def add(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> bool:
        """Add a value only if the key doesn't exist. Returns True if
        the value was added, False if a value was already present.

        Serialization failures must NOT silently return False — most
        callers use this as a flight-claim primitive; a False return
        is interpreted as "another worker won the slot", so a
        silently-unserialisable payload would skip the work entirely.

        Same reasoning applies when Redis itself is unreachable — return
        ``False`` so callers can distinguish a lost flight-claim from a
        successful acquire.
        """
        redis_key = f"{self._prefix}{key}"
        if isinstance(value, str):
            payload = value.encode("utf-8")
        else:
            try:
                payload = pickle.dumps(value)
            except Exception as e:
                from cara.exceptions import CacheConfigurationException

                raise CacheConfigurationException(
                    f"Cannot pickle flight-claim value for key '{key}': {e}"
                ) from e

        ttl_seconds = ttl if (ttl is not None) else self._default_ttl
        payload_size = len(payload)
        try:
            if ttl_seconds > 0:
                result = self._client.set(redis_key, payload, ex=ttl_seconds, nx=True)
            else:
                result = self._client.set(redis_key, payload, nx=True)
            won = result is not None
            notify_cache_event("add", "set" if won else "noop", key, payload_size)
            return won
        except Exception as e:
            Log.error("[RedisCacheDriver] add() flight-claim failed for '%s': %s", key, e, category='cache', exc_info=True)
            notify_cache_event("add", "error", key, payload_size)
            raise

    def remember(
        self,
        key: str,
        ttl: int,
        callback,
    ) -> Any:
        """
        Get value from cache or execute callback and cache the result.

        If the key exists and hasn't expired, return the cached value.
        Otherwise, execute the callback, cache its result, and return it.
        """
        _MISSING = object()
        cached = self.get(key, _MISSING)
        if cached is not _MISSING:
            return cached

        value = callback()
        self.put(key, value, ttl)
        return value

    # Lua: compare the stored payload against the expected one and
    # delete only on equality. EVAL is single-threaded on the Redis
    # server, which is what makes the CAS atomic — non-atomic
    # ``GET; DEL`` lets a different owner slip in between the two
    # calls and lose its lock.
    #
    # NOTE — for ``forget_if`` we deliberately compare on the *raw*
    # string-coerced expected value, not on the pickle bytes. Pickle
    # output is not deterministic across runs/protocols, so the
    # earlier "compare pickled bytes" approach failed the equality
    # check whenever caller A wrote with one protocol and caller B
    # released with another. Lock owners are already string-typed
    # (``CacheLock.owner``) so plain string CAS is the correct shape.
    _RELEASE_LOCK_LUA = (
        "if redis.call('get', KEYS[1]) == ARGV[1] then "
        "  return redis.call('del', KEYS[1]) "
        "else return 0 end"
    )

    def increment(self, key: str, amount: int = 1, ttl: int | None = None) -> int:
        """Atomically increment via Redis INCRBY.

        If the key holds a non-integer value (e.g. a pickled cache entry
        that collided with a counter key), delete it first and start fresh.
        """
        redis_key = f"{self._prefix}{key}"
        amount = int(amount)
        ttl = int(ttl) if ttl is not None else None
        try:
            new_val = self._client.incrby(redis_key, amount)
            if ttl and ttl > 0:
                # Set TTL on first creation OR refresh TTL if the key
                # previously lost its expiry (e.g. after WRONGTYPE
                # recovery). Checking TTL(-1) avoids resetting the timer
                # on every increment for keys that already have a TTL.
                if new_val == amount or self._client.ttl(redis_key) == -1:
                    self._client.expire(redis_key, ttl)
            return new_val
        except Exception:
            # Key likely holds non-integer data — nuke and reinitialise.
            try:
                pipe = self._client.pipeline(transaction=True)
                pipe.delete(redis_key)
                if ttl and ttl > 0:
                    pipe.set(redis_key, str(amount), ex=ttl)
                else:
                    pipe.set(redis_key, str(amount))
                pipe.execute()
                return amount
            except Exception as fallback_error:
                Log.warning("[RedisCacheDriver] increment fallback failed for '%s': %s", key, fallback_error, category='cache')
            return amount

    def forget_if(self, key: str, expected_value: Any) -> bool:
        redis_key = f"{self._prefix}{key}"
        owner_token = str(expected_value).encode("utf-8")
        try:
            result = self._client.eval(self._RELEASE_LOCK_LUA, 1, redis_key, owner_token)
            return bool(result) and int(result) > 0
        except Exception as e:
            Log.warning("[RedisCacheDriver] forget_if failed: %s", e, category='cache')
            return False

    def ttl(self, key: str) -> int | None:
        """Remaining time-to-live for ``key`` in seconds.

        Returns ``None`` when the key doesn't exist or has no expiry,
        and a non-negative int otherwise. Lets rate limiters /
        throttle middleware report an accurate ``Retry-After`` instead
        of the full window. Wraps Redis ``TTL`` which returns -2 (no
        such key) and -1 (no expiry); both map to ``None`` here.
        """
        redis_key = f"{self._prefix}{key}"
        try:
            t = self._client.ttl(redis_key)
        except Exception:
            _logger.warning("redis operation failed", exc_info=True)
            return None
        if t is None or t < 0:
            return None
        return int(t)

    def forget_pattern(self, pattern: str) -> int:
        """
        Delete multiple keys matching a glob pattern.

        Uses Redis SCAN to find keys matching the pattern, then DEL to remove them.
        This is non-blocking and safe for large key sets.

        Args:
            pattern: Glob pattern (e.g., "home:*", "products:featured:*")

        Returns:
            Number of keys deleted
        """
        prefixed_pattern = f"{self._prefix}{pattern}"
        deleted_count = 0

        try:
            cursor = 0
            while True:
                cursor, keys = self._client.scan(
                    cursor=cursor,
                    match=prefixed_pattern,
                    count=100,
                )

                if keys:
                    deleted_count += self._client.delete(*keys)

                if cursor == 0:
                    break

            return deleted_count
        except Exception:
            _logger.warning("redis operation failed", exc_info=True)
            return 0
