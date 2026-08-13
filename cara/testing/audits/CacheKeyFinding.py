"""CacheKeyFinding."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CacheKeyFinding:
    """One constant key in a parameterized function."""

    path: str
    line: int
    key: str
    function: str
    parameters: tuple[str, ...]

    def __str__(self) -> str:
        return (
            f"{self.path}:{self.line}: {self.function}({', '.join(self.parameters)}) "
            f"caches under the constant key {self.key!r} — every argument serves "
            f"the first caller's answer"
        )
