"""CacheTaggedStore."""

from __future__ import annotations

from typing import Any


class CacheTaggedStore:
    """
    Tagged cache operations.

    Allows grouping cache entries by tags for bulk invalidation.
    """

    def __init__(self, cache, tags: list[str]):
        """
        Initialize tagged cache store.

        Args:
            cache: Cache driver instance
            tags: List of tags to apply to operations
        """
        self.cache = cache
        self.tags = tags

    def _build_tagged_key(self, key: str) -> str:
        """Build a key with tag prefix."""
        tag_prefix = ":".join(self.tags)
        return f"{tag_prefix}:{key}"

    def get(self, key: str, default: Any = None, *, strict: bool = True) -> Any:
        """Get value from tagged cache."""
        return self.cache.get(self._build_tagged_key(key), default, strict=strict)

    def put(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        *,
        strict: bool = True,
    ) -> None:
        """Store value in tagged cache."""
        self.cache.put(self._build_tagged_key(key), value, ttl, strict=strict)

    def forever(self, key: str, value: Any) -> None:
        """Store value permanently in tagged cache."""
        self.cache.forever(self._build_tagged_key(key), value)

    def forget(self, key: str) -> bool:
        """Remove value from tagged cache."""
        return self.cache.forget(self._build_tagged_key(key))

    def pull(self, key: str, default: Any = None) -> Any:
        """Atomically return and remove a tagged value."""
        return self.cache.pull(self._build_tagged_key(key), default)

    def flush(self) -> int:
        """Flush all entries with these tags."""
        # Flush all keys matching tag pattern
        pattern = f"{':'.join(self.tags)}:*"
        return self.cache.forget_pattern(pattern)
