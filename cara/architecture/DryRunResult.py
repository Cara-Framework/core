"""DryRunResult."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class DryRunResult:
    """Outcome of one deployable's dry-run: what ran, and what broke."""

    deployable: str
    steps: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures

    def __str__(self) -> str:
        head = f"vendor dry-run [{self.deployable}]: " + ("OK" if self.ok else "FAILED")
        return "\n".join([head, *(f"  - {line}" for line in self.steps + self.failures)])
