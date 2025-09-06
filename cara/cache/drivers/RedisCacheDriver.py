"""
Redis-based Cache Driver for the Cara framework.

This module implements a cache driver that uses Redis as the backend storage,
supporting TTL-based expiration and all standard cache operations.
"""

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
            global redis
            import redis
        except ImportError:
            raise CacheConfigurationException(
                "redis is required for RedisCacheDriver. "
                "Please install it with: pip install redis"
            )
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
        except Exception:
            pass

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
        except Exception:
            pass

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
