"""SaltedHashKeyFinding."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SaltedHashKeyFinding:
    """One cache call whose key is built from the builtin ``hash()``."""

    path: str
    line: int
    method: str

    def __str__(self) -> str:
        return (
            f"{self.path}:{self.line}: Cache.{self.method} key uses the builtin "
            f"hash() — it is salted per process (PYTHONHASHSEED), so every worker "
            f"writes its own key and the cache silently never hits; use a stable "
            f"digest (hashlib.sha256)"
        )
