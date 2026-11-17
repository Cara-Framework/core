"""MixinOverrideFinding."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MixinOverrideFinding:
    """A class body redefining a method one of its own ``*Mixin`` slices defines."""

    path: str
    line: int
    cls: str
    method: str
    mixin: str

    def __str__(self) -> str:
        return (
            f"{self.path}:{self.line}: {self.cls}.{self.method} redefines "
            f"{self.mixin}.{self.method}"
        )
