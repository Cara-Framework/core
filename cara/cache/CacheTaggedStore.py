"""CacheTaggedStore."""

from __future__ import annotations

import hashlib
import uuid
from typing import Any

from .CacheBackendFaults import cache_backend_fault_types

#: Prefix of the per-tag version id and of every tagged entry. Distinct from
#: any plain key a caller would choose, so a tagged entry never collides with
#: an untagged one.
_TAG_VERSION_PREFIX = "cara:tag:"
_TAGGED_PREFIX = "cara:tagged:"


class CacheTaggedStore:
    """
    Tagged cache operations.

    Allows grouping cache entries by tags for bulk invalidation.

    Every tag owns a version id (a random token stored without expiry). An
    entry's key folds in the CURRENT version id of each of its tags, and
    flushing a tag replaces that tag's id: every entry carrying the tag —
    whatever other tags it also carries — is unreachable at once, with no key
    scan. The orphaned entries expire on their own TTL (an orphaned
    ``forever`` entry stays until the backend evicts it, as in Laravel).

    Before, a tag was only a key prefix: ``tags("posts", "featured")`` wrote
    ``posts:featured:<key>``, so ``tags("featured").flush()`` (pattern
    ``featured:*``) left it alive, and ``tags("posts").flush()`` also deleted
    any UNTAGGED key that merely began with ``posts:``.
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

    @staticmethod
    def _version_key(tag: str) -> str:
        return f"{_TAG_VERSION_PREFIX}{tag}:version"

    def _tag_versions(self, *, strict: bool = True) -> list[str]:
        """The current version id of each tag, minting the missing ones.

        A missing id is minted with ``add`` (set-if-absent): two workers that
        both find a tag unversioned agree on the winner's id instead of each
        writing its own and splitting the namespace.
        """
        keys = [self._version_key(tag) for tag in self.tags]
        versions = self.cache.many(keys, None, strict=strict)
        resolved: list[str] = []
        for key in keys:
            version = versions.get(key)
            if version is None:
                self.cache.add(key, uuid.uuid4().hex, 0)
                version = self.cache.get(key, None, strict=strict)
                if version is None:
                    raise LookupError(f"cache tag version '{key}' could not be minted")
            resolved.append(str(version))
        return resolved

    def _build_tagged_key(self, key: str, *, strict: bool = True) -> str:
        """The key of ``key`` under the current version of every tag."""
        versions = self._tag_versions(strict=strict)
        namespace = hashlib.sha1(
            "|".join(versions).encode("utf-8"), usedforsecurity=False
        ).hexdigest()
        return f"{_TAGGED_PREFIX}{namespace}:{key}"

    def get(self, key: str, default: Any = None, *, strict: bool = True) -> Any:
        """Get value from tagged cache."""
        try:
            tagged_key = self._build_tagged_key(key, strict=strict)
        except (LookupError, *cache_backend_fault_types()):
            # A disposable read (``strict=False``) treats unreadable tag
            # versions as a miss, exactly as it treats an unreadable value.
            if strict:
                raise
            return default
        return self.cache.get(tagged_key, default, strict=strict)

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
        """Invalidate every entry carrying any of these tags.

        Returns the number of tags reset. Each tag gets a fresh version id, so
        the namespace of every entry that carries it changes.
        """
        for tag in self.tags:
            self.cache.forever(self._version_key(tag), uuid.uuid4().hex)
        return len(self.tags)
