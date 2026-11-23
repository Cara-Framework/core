"""Bulk key removal of ``RedisCacheDriver``: ``flush`` and ``forget_pattern``.

Kept beside the driver (bound back onto the class) so the driver module stays
under the framework's file-size limit.

Removal uses ``UNLINK``: the keyspace entry goes at once and the memory is
reclaimed on a background thread. ``DEL`` frees every value inline on the
single Redis thread, so a flush of a large prefix (or one big value) stalled
every other client of the shared server — queues, sessions, rate limits —
for the duration. Servers without ``UNLINK`` (Redis < 4.0) fall back to
``DEL``.
"""

from __future__ import annotations

from cara.facades import Log


def _is_unknown_command(exc: Exception) -> bool:
    try:
        from redis.exceptions import ResponseError  # local: heavy optional dep
    except ImportError:
        return False
    return isinstance(exc, ResponseError) and "unknown command" in str(exc).lower()


def _remove_keys(self, keys) -> int:
    """``UNLINK`` ``keys`` (``DEL`` where the server lacks it); the count removed."""
    if getattr(self, "_unlink_unsupported", False):
        return int(self._client.delete(*keys))
    unlink = getattr(self._client, "unlink", None)
    if unlink is not None:
        try:
            return int(unlink(*keys))
        except Exception as exc:
            if not _is_unknown_command(exc):
                raise
            Log.warning(
                "[RedisCacheDriver] server has no UNLINK; key removal falls back to DEL",
                category="cache",
            )
    # Remembered per driver so later removals skip the failing round trip.
    self._unlink_unsupported = True
    return int(self._client.delete(*keys))


def _flush(self) -> None:
    """Flush every cache entry under our prefix.

    SECURITY — must NOT call ``flushdb()``: cara namespaces cache
    keys with ``self._prefix`` but Redis databases are typically
    shared with broadcasting state, queue jobs, sessions, rate
    limit counters, etc. Wiping the whole DB on a routine flush
    was wiping co-tenant data. We now SCAN+UNLINK only keys under
    our prefix.

    The codec namespace is always non-empty, even when the configured base
    prefix is empty, so the scan can never expand to every key in the DB.
    """
    try:
        cursor = 0
        pattern = f"{self._prefix}*"
        while True:
            cursor, keys = self._client.scan(
                cursor=cursor, match=pattern, count=self._SCAN_COUNT
            )
            if keys:
                _remove_keys(self, keys)
            if cursor == 0:
                break
    except Exception as e:
        Log.warning("[RedisCacheDriver] flush failed: %s", e, category="cache")
        raise


def _forget_pattern(self, pattern: str) -> int:
    """
    Delete multiple keys matching a glob pattern.

    Uses Redis SCAN to find keys matching the pattern, then UNLINK to remove
    them. This is non-blocking and safe for large key sets.

    Args:
        pattern: Glob pattern (e.g., "home:*", "products:featured:*")

    Returns:
        Number of keys deleted
    """
    deleted_count = 0

    # ONE walk for both namespaces: ``[vc]`` is the value / counter
    # marker that follows the prefix. Two walks, one per namespace, each
    # crossed the entire shared keyspace again for the same pattern.
    match = f"{self._prefix}[vc]:{pattern}"
    cursor = 0
    while True:
        cursor, keys = self._client.scan(
            cursor=cursor, match=match, count=self._SCAN_COUNT
        )

        if keys:
            deleted_count += _remove_keys(self, keys)

        if cursor == 0:
            break

    return deleted_count
