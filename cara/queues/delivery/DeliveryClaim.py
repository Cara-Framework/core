"""Execution lease claim result."""

from dataclasses import dataclass


@dataclass(frozen=True)
class DeliveryClaim:
    outcome: str
    lease_token: str | None = None
    reclaimed: bool = False
    terminal_reason: str | None = None
