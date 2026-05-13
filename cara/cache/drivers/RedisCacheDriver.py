"""
Redis-based Cache Driver for the Cara framework.

This module implements a cache driver that uses Redis as the backend storage,
supporting TTL-based expiration and all standard cache operations.
"""

from __future__ import annotations

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
        except Exception as exc:
            Log.warning(
                f"[RedisCacheDriver] GET failed for '{key}': {exc}",
                category="cache",
            )
            return default

        if raw_data is None:
            return default

        try:
            return pickle.loads(raw_data)
        except Exception as exc:
            # Self-heal a corrupt / pickle-incompatible entry instead of
            # serving the default forever. Without the proactive DELETE
            # every subsequent GET re-fetched, re-failed to unpickle,
            # and re-warned — so the same poison key burned a syscall +
            # log line on every request until the TTL expired (which,
            # for ``BRAND_VERSION_TTL`` and friends, is 30 days). Drop
            # it on first miss so the next caller's ``Cache.remember``
            # repopulates with the current pickle protocol.
            Log.warning(
                f"[RedisCacheDriver] unpickle failed for '{key}' "
                f"(corrupt entry, deleting): {exc}",
                category="cache",
            )
            try:
                self._client.delete(redis_key)
            except Exception:
                pass
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
        except Exception as e:
            # Silently swallowing serialisation failures was the
            # original bug — callers using ``Cache.add(key, True,
            # ttl)`` as a flight-claim got a silent False back, which
            # they (correctly) read as "another worker won the claim",
            # so the in-flight job never ran. Surface the real cause.
            from cara.exceptions import CacheConfigurationException

            raise CacheConfigurationException(
                f"Cannot pickle value for cache key '{key}' "
                f"({type(value).__name__}): {e}"
            ) from e

        ttl_seconds = ttl if (ttl is not None) else self._default_ttl
        try:
            if ttl_seconds > 0:
                self._client.set(redis_key, payload, ex=ttl_seconds)
            else:
                self._client.set(redis_key, payload)
        except Exception as e:
            Log.warning(f"[RedisCacheDriver] set failed: {e}", category="cache")

    def forever(self, key: str, value: Any) -> None:
        self.put(key, value, ttl=0)

    def forget(self, key: str) -> bool:
        redis_key = f"{self._prefix}{key}"
        try:
            return self._client.delete(redis_key) > 0
        except Exception as e:
            # Don't swallow silently — admin invalidation paths
            # (CacheController, AdminProductRepository.invalidate_*)
            # log the count of forgotten keys, and a returned False
            # is interpreted as "key wasn't there", not "Redis is
            # down". Surface the failure so the caller's audit trail
            # records something operators can act on.
            Log.warning(
                f"[RedisCacheDriver] forget failed for '{key}': {e}",
                category="cache",
            )
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
            Log.warning(f"[RedisCacheDriver] flush failed: {e}", category="cache")

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
        """Add a value only if the key doesn't exist. Returns True if
        the value was added, False if a value was already present.

        Serialization failures must NOT silently return False — most
        callers use this as a flight-claim primitive; a False return
        is interpreted as "another worker won the slot", so a
        silently-unserialisable payload would skip the work entirely.

        Same reasoning applies when Redis itself is unreachable. The
        previous `except Exception: return False` swallowed connection
        errors and made every worker think it had lost the flight
        claim — so during a Redis blip nothing executed at all. Now we
        fail-OPEN: log loudly and return True so the work runs. At-most-
        once becomes at-least-once during the outage, which downstream
        idempotency wrappers (`wrap_with_idempotency`) already tolerate.
        Total starvation, by contrast, isn't recoverable until someone
        notices the queue backlog.
        """
        redis_key = f"{self._prefix}{key}"
        try:
            payload = pickle.dumps(value)
        except Exception as e:
            from cara.exceptions import CacheConfigurationException

            raise CacheConfigurationException(
                f"Cannot pickle flight-claim value for key '{key}': {e}"
            ) from e

        ttl_seconds = ttl if (ttl is not None) else self._default_ttl
        try:
            if ttl_seconds > 0:
                result = self._client.set(redis_key, payload, ex=ttl_seconds, nx=True)
            else:
                result = self._client.set(redis_key, payload, nx=True)
            return result is not None
        except Exception as e:
            Log.error(
                f"[RedisCacheDriver] add() flight-claim failed for '{key}' "
                f"(Redis unreachable?): {e} — failing OPEN so caller proceeds",
                category="cache",
            )
            return True

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
            if ttl and ttl > 0 and new_val == amount:
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
                Log.warning(
                    f"[RedisCacheDriver] increment fallback failed for '{key}': "
                    f"{fallback_error}",
                    category="cache",
                )
            return amount

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
            Log.warning(f"[RedisCacheDriver] forget_if failed: {e}", category="cache")
            return False

    def ttl(self, key: str) -> Optional[int]:
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
            return 0
