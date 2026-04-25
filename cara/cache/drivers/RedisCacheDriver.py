"""
Redis-based Cache Driver for the Cara framework.

This module implements a cache driver that uses Redis as the backend storage,
supporting TTL-based expiration and all standard cache operations.
"""

from cara.facades import Log
import pickle
from typing import Any, Optional

from cara.cache.contracts import Cache
from cara.exceptions import CacheConfigurationException


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
        password: Optional[str],
        prefix: str = "",
        default_ttl: int = 60,
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
        self._client = redis.Redis(host=host, port=port, db=db, password=password)

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
        except Exception:
            return default

        if raw_data is None:
            return default

        try:
            return pickle.loads(raw_data)
        except Exception:
            return default

    def put(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> None:
        redis_key = f"{self._prefix}{key}"
        try:
            payload = pickle.dumps(value)
        except Exception:
            return

        ttl_seconds = ttl if (ttl is not None) else self._default_ttl
        try:
            if ttl_seconds > 0:
                self._client.set(redis_key, payload, ex=ttl_seconds)
            else:
                self._client.set(redis_key, payload)
        except Exception as e:
            Log.debug(f"[RedisCacheDriver] set failed: {e}", category="cache")

    def forever(self, key: str, value: Any) -> None:
        self.put(key, value, ttl=0)

    def forget(self, key: str) -> bool:
        redis_key = f"{self._prefix}{key}"
        try:
            return self._client.delete(redis_key) > 0
        except Exception:
            return False

    def flush(self) -> None:
        try:
            self._client.flushdb()
        except Exception as e:
            Log.debug(f"[RedisCacheDriver] flush failed: {e}", category="cache")

    def has(self, key: str) -> bool:
        """Check if a key exists in cache."""
        redis_key = f"{self._prefix}{key}"
        try:
            return self._client.exists(redis_key) > 0
        except Exception:
            return False

    def add(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """Add a value only if key doesn't exist. Returns True if added."""
        redis_key = f"{self._prefix}{key}"
        try:
            payload = pickle.dumps(value)
        except Exception:
            return False

        ttl_seconds = ttl if (ttl is not None) else self._default_ttl
        try:
            # Use Redis SET with NX (only if not exists)
            if ttl_seconds > 0:
                result = self._client.set(redis_key, payload, ex=ttl_seconds, nx=True)
            else:
                result = self._client.set(redis_key, payload, nx=True)
            return result is not None
        except Exception:
            return False

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

    # Lua: compare the stored pickled payload against the expected one and
    # delete only on equality. EVAL is single-threaded on the Redis server,
    # which is what makes the CAS atomic — non-atomic ``GET; DEL`` lets a
    # different owner slip in between the two calls and lose its lock.
    _RELEASE_LOCK_LUA = (
        "if redis.call('get', KEYS[1]) == ARGV[1] then "
        "  return redis.call('del', KEYS[1]) "
        "else return 0 end"
    )

    def forget_if(self, key: str, expected_value: Any) -> bool:
        redis_key = f"{self._prefix}{key}"
        try:
            payload = pickle.dumps(expected_value)
        except Exception:
            return False
        try:
            result = self._client.eval(self._RELEASE_LOCK_LUA, 1, redis_key, payload)
            return bool(result) and int(result) > 0
        except Exception as e:
            Log.debug(f"[RedisCacheDriver] forget_if failed: {e}", category="cache")
            return False

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
            return 0
