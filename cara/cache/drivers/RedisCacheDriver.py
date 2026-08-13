"""
Redis-based Cache Driver for the Cara framework.

This module implements a cache driver that uses Redis as the backend storage,
supporting TTL-based expiration and all standard cache operations.
"""

from __future__ import annotations

import logging

# Payloads above the configured driver threshold emit a one-time warning per
# key so operators notice runaway cache-as-blob patterns.
from typing import Any

from cara.cache.codecs import JsonCacheCodec
from cara.cache.contracts import CacheContract
from cara.cache.Observer import notify_cache_event
from cara.exceptions import CacheConfigurationException
from cara.facades import Log

_logger = logging.getLogger("cara.cache.redis")


class RedisCacheDriver(CacheContract):
    """
    Stores cache entries in Redis.

    Keys use codec-versioned, type-separated namespaces. Values are canonical
    tagged JSON with HMAC integrity. Redis-native integer counters remain raw
    for INCRBY, but live under a separate counter prefix so an attacker with
    Redis write access cannot substitute an unsigned integer for an arbitrary
    authenticated cache value.
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
        ssl: bool = False,
        ssl_ca_certs: str | None = None,
        ssl_certfile: str | None = None,
        ssl_keyfile: str | None = None,
        ssl_cert_reqs: str = "required",
        signing_key: str | bytes | None = None,
        max_nodes: int | None = None,
        large_value_bytes: int = 262144,
    ):
        if (
            isinstance(large_value_bytes, bool)
            or not isinstance(large_value_bytes, int)
            or large_value_bytes < 1
        ):
            raise CacheConfigurationException(
                "cache.large_value_bytes must be a positive integer"
            )
        self._base_prefix = prefix or ""
        self._codec = JsonCacheCodec(
            self._resolve_signing_key(signing_key), max_nodes=max_nodes
        )
        separator = (
            "" if not self._base_prefix or self._base_prefix.endswith(":") else ":"
        )
        self._prefix = f"{self._base_prefix}{separator}{self._codec.NAMESPACE}:"
        self._value_prefix = f"{self._prefix}v:"
        self._counter_prefix = f"{self._prefix}c:"
        self._default_ttl = self._resolve_ttl(None, default_ttl)
        self._large_value_bytes = large_value_bytes
        self._large_value_warned: set[str] = set()
        self._validate_connection_params(host, port, db)
        try:
            import redis  # local: heavy optional dep
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
        # descriptors. redis-py 6+ retries timeout errors through its built-in
        # retry policy; the deprecated ``retry_on_timeout`` argument is
        # intentionally not passed.
        redis_kwargs: dict = {
            "host": host,
            "port": port,
            "db": db,
            "password": password,
            "socket_connect_timeout": socket_connect_timeout,
            "socket_timeout": socket_timeout,
            "socket_keepalive": socket_keepalive,
            "health_check_interval": health_check_interval,
            "ssl": ssl,
        }
        if ssl:
            redis_kwargs.update(
                {
                    "ssl_ca_certs": ssl_ca_certs,
                    "ssl_certfile": ssl_certfile,
                    "ssl_keyfile": ssl_keyfile,
                    "ssl_cert_reqs": ssl_cert_reqs,
                }
            )
        if max_connections is not None:
            redis_kwargs["max_connections"] = max_connections
        # Drop None entries so we don't override redis-py's own defaults
        # (e.g. for ``password``) with explicit None values.
        redis_kwargs = {
            k: v for k, v in redis_kwargs.items() if v is not None or k == "password"
        }
        self._client = redis.Redis(**redis_kwargs)

    def connection(self):
        """The raw redis-py client this driver writes through.

        The ONE supported way for a primitive that needs Redis server-side
        atomicity — a Lua ``EVAL``, a sorted-set semaphore — to reach the
        connection. ``ConcurrencyLimited`` used to duck-type five private
        attribute names looking for it (``_redis``, ``store._redis``,
        ``store.redis``, ``connection()``, ``redis``); this driver has never
        had any of them, so the probe returned ``None`` on every call and a
        hard concurrency ceiling silently enforced nothing for its entire
        life. A named accessor makes that class of miss impossible: it
        either exists or the caller fails loudly.

        Callers must NOT use this for ordinary get/put/forget work — those
        go through the codec-versioned, HMAC-authenticated methods above,
        and bypassing them writes values this driver will later refuse to
        decode.
        """
        return self._client

    @staticmethod
    def _resolve_signing_key(explicit: str | bytes | None) -> str | bytes:
        if explicit:
            return explicit
        raise CacheConfigurationException(
            "CACHE_SIGNING_KEY is required for the Redis cache driver."
        )

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

    def _value_key(self, key: str) -> str:
        return f"{self._value_prefix}{key}"

    def _counter_key(self, key: str) -> str:
        return f"{self._counter_prefix}{key}"

    def get(self, key: str, default: Any = None, *, strict: bool = True) -> Any:
        redis_key = self._value_key(key)
        try:
            raw_data = self._client.get(redis_key)
        except Exception as exc:
            Log.warning(
                "[RedisCacheDriver] GET failed for '%s': %s", key, exc, category="cache"
            )
            notify_cache_event("get", "error", key, None)
            if strict:
                raise
            return default

        if raw_data is None:
            notify_cache_event("get", "miss", key, None)
            return default

        try:
            value = self._codec.decode(raw_data)
            notify_cache_event("get", "hit", key, len(raw_data))
            return value
        except CacheConfigurationException as decode_error:
            # Corrupt/tampered envelopes are never deserialized. Delete on
            # first read so remember() can repopulate clean state.
            Log.warning(
                "[RedisCacheDriver] codec validation failed for '%s' (entry deleted): %s",
                key,
                decode_error,
                category="cache",
            )
            try:
                self._client.delete(redis_key)
            except Exception:
                _logger.warning("self-heal delete failed", exc_info=True)
            notify_cache_event("get", "error", key, None)
            if not strict:
                return default
            raise CacheConfigurationException(
                f"Corrupt cache value for key '{key}'"
            ) from decode_error

    def put(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        *,
        strict: bool = True,
    ) -> None:
        redis_key = self._value_key(key)
        try:
            payload = self._codec.encode(value)
        except CacheConfigurationException as e:
            if not strict:
                Log.warning(
                    "[RedisCacheDriver] disposable value for '%s' is not cacheable: %s",
                    key,
                    e,
                    category="cache",
                )
                notify_cache_event("put", "error", key, None)
                return
            raise CacheConfigurationException(
                f"Cannot encode value for cache key '{key}' ({type(value).__name__}): {e}"
            ) from e

        ttl_seconds = self._resolve_ttl(ttl, self._default_ttl)
        payload_size = len(payload)
        if payload_size > self._large_value_bytes and key not in self._large_value_warned:
            self._large_value_warned.add(key)
            Log.warning(
                "[RedisCacheDriver] large cache value: key='%s' size=%sB threshold=%sB — consider normalising or sharding the payload",
                key,
                payload_size,
                self._large_value_bytes,
                category="cache",
            )
        try:
            if ttl_seconds > 0:
                self._client.set(redis_key, payload, ex=ttl_seconds)
            else:
                self._client.set(redis_key, payload)
            notify_cache_event("put", "set", key, payload_size)
        except Exception as e:
            Log.warning("[RedisCacheDriver] set failed: %s", e, category="cache")
            notify_cache_event("put", "error", key, payload_size)
            if strict:
                raise

    def forever(self, key: str, value: Any) -> None:
        self.put(key, value, ttl=0)

    def forget(self, key: str) -> bool:
        redis_keys = (self._value_key(key), self._counter_key(key))
        try:
            deleted = self._client.delete(*redis_keys) > 0
            notify_cache_event("forget", "deleted" if deleted else "noop", key, None)
            return deleted
        except Exception as e:
            # Don't swallow silently — admin invalidation paths
            # (CacheController, AdminProductRepository.invalidate_*)
            # log the count of forgotten keys, and a returned False
            # is interpreted as "key wasn't there", not "Redis is
            # down". Surface the failure so the caller's audit trail
            # records something operators can act on.
            Log.warning(
                "[RedisCacheDriver] forget failed for '%s': %s", key, e, category="cache"
            )
            notify_cache_event("forget", "error", key, None)
            raise

    def pull(self, key: str, default: Any = None) -> Any:
        """Atomically return and delete ``key`` using Redis ``GETDEL``."""
        redis_key = self._value_key(key)
        try:
            raw_data = self._client.getdel(redis_key)
        except Exception as exc:
            Log.error(
                "[RedisCacheDriver] GETDEL failed for '%s': %s",
                key,
                exc,
                category="cache",
                exc_info=True,
            )
            notify_cache_event("pull", "error", key, None)
            raise

        if raw_data is None:
            notify_cache_event("pull", "miss", key, None)
            return default

        try:
            value = self._codec.decode(raw_data)
        except CacheConfigurationException as exc:
            Log.warning(
                "[RedisCacheDriver] pulled corrupt value for '%s'",
                key,
                category="cache",
            )
            notify_cache_event("pull", "error", key, len(raw_data))
            raise CacheConfigurationException(
                f"Corrupt one-time cache value for key '{key}'"
            ) from exc

        notify_cache_event("pull", "hit", key, len(raw_data))
        return value

    def flush(self) -> None:
        """Flush every cache entry under our prefix.

        SECURITY — must NOT call ``flushdb()``: cara namespaces cache
        keys with ``self._prefix`` but Redis databases are typically
        shared with broadcasting state, queue jobs, sessions, rate
        limit counters, etc. Wiping the whole DB on a routine flush
        was wiping co-tenant data. We now SCAN+DEL only keys under
        our prefix.

        The codec namespace is always non-empty, even when the configured base
        prefix is empty, so the scan can never expand to every key in the DB.
        """
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
            Log.warning("[RedisCacheDriver] flush failed: %s", e, category="cache")
            raise

    def has(self, key: str) -> bool:
        """Check if a key exists in cache."""
        return self._client.exists(self._value_key(key), self._counter_key(key)) > 0

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

        When Redis itself is unreachable this method RAISES (it does NOT
        return False). That is the correct fail-CLOSED posture for the
        lock/flight-claim use case: the exception propagates through
        ``CacheLock.acquire`` so a caller that couldn't reach Redis does not
        falsely believe it acquired the slot. (Earlier docs claimed a False
        return here — the raising behaviour is intentional; don't "fix" it to
        return False without auditing every lock call site.)
        """
        redis_key = self._value_key(key)
        try:
            payload = self._codec.encode(value)
        except CacheConfigurationException as e:
            raise CacheConfigurationException(
                f"Cannot encode flight-claim value for key '{key}': {e}"
            ) from e

        ttl_seconds = self._resolve_ttl(ttl, self._default_ttl)
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
            Log.error(
                "[RedisCacheDriver] add() flight-claim failed for '%s': %s",
                key,
                e,
                category="cache",
                exc_info=True,
            )
            notify_cache_event("add", "error", key, payload_size)
            raise

    # Lua: compare the stored payload against the expected one and
    # delete only on equality. EVAL is single-threaded on the Redis
    # server, which is what makes the CAS atomic — non-atomic
    # ``GET; DEL`` lets a different owner slip in between the two
    # calls and lose its lock.
    #
    # Canonical tagged JSON is byte-deterministic, so the release CAS compares
    # the exact authenticated encoding written by add().
    _RELEASE_LOCK_LUA = (
        "if redis.call('get', KEYS[1]) == ARGV[1] then "
        "  return redis.call('del', KEYS[1]) "
        "else return 0 end"
    )

    def increment(self, key: str, amount: int = 1, ttl: int | None = None) -> int:
        """Atomically increment via Redis INCRBY.

        Counters use their own codec-versioned namespace. Invalid input,
        corrupt state and backend failures raise; resetting a damaged
        security counter would weaken every limit that consumes it.
        """
        redis_key = self._counter_key(key)
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise TypeError("Cache counter amount must be an integer")
        if ttl is None:
            raise CacheConfigurationException(
                "Cache counters require a positive expiration"
            )
        ttl = self._resolve_ttl(ttl, self._default_ttl)
        if ttl == 0:
            raise CacheConfigurationException(
                "Cache counters require a positive expiration"
            )
        try:
            new_val = self._client.incrby(redis_key, amount)
            # Set TTL on first creation OR refresh TTL if the key
            # previously lost its expiry (e.g. after WRONGTYPE
            # recovery). Checking TTL(-1) avoids resetting the timer
            # on every increment for keys that already have a TTL.
            if (
                ttl
                and ttl > 0
                and (new_val == amount or self._client.ttl(redis_key) == -1)
            ):
                self._client.expire(redis_key, ttl)
            return new_val
        except Exception as exc:
            Log.error(
                "[RedisCacheDriver] counter increment failed for '%s': %s",
                key,
                exc,
                category="cache",
                exc_info=True,
            )
            raise

    def forget_if(self, key: str, expected_value: Any) -> bool:
        redis_key = self._value_key(key)
        owner_token = self._codec.encode(expected_value)
        result = self._client.eval(self._RELEASE_LOCK_LUA, 1, redis_key, owner_token)
        return bool(result) and int(result) > 0

    def ttl(self, key: str) -> int | None:
        """Remaining time-to-live for ``key`` in seconds.

        Returns ``None`` when the key doesn't exist or has no expiry,
        and a non-negative int otherwise. Lets rate limiters /
        throttle middleware report an accurate ``Retry-After`` instead
        of the full window. Wraps Redis ``TTL`` which returns -2 (no
        such key) and -1 (no expiry); both map to ``None`` here.
        """
        counter_key = self._counter_key(key)
        value_key = self._value_key(key)
        t = self._client.ttl(counter_key)
        if t == -2:
            t = self._client.ttl(value_key)
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
        deleted_count = 0

        for namespace_prefix in (self._value_prefix, self._counter_prefix):
            cursor = 0
            while True:
                cursor, keys = self._client.scan(
                    cursor=cursor,
                    match=f"{namespace_prefix}{pattern}",
                    count=100,
                )

                if keys:
                    deleted_count += self._client.delete(*keys)

                if cursor == 0:
                    break

        return deleted_count
