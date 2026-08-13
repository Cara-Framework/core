"""Terminal-hook lease claim result."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TerminalHookClaim:
    outcome: str
    lease_token: str | None = None
    signed_envelope: bytes | None = None
    status: str | None = None
    terminal_reason: str | None = None
