"""Execution lease claim result."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class DeliveryClaim:
    outcome: str
    lease_token: str | None = None
    reclaimed: bool = False
    terminal_reason: str | None = None
    #: The latest the heartbeat may carry the lease to: claim time plus the
    #: job's timeout and the execution grace — the whole budget a run can
    #: legitimately take (``QueueJobDeliveryStore.renew_execution``).
    lease_deadline: datetime | None = None
