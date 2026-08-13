"""DispatchFinding."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DispatchFinding:
    """One discarded coroutine, reported against a product-relative path."""

    path: str
    line: int
    call: str

    def __str__(self) -> str:
        return (
            f"{self.path}:{self.line}: `{self.call}(...)` is a statement with no "
            f"`await` — the coroutine is discarded and the work never runs"
        )
