"""Replay admission view for a durable delivery."""

from dataclasses import dataclass

import pendulum

_ACCEPTED_TERMINAL_STATUSES = frozenset({"completed", "retry_scheduled"})
_ACCEPTED_OPEN_STATUSES = frozenset({"pending", "processing"})


@dataclass(frozen=True)
class ReplayDelivery:
    job_id: str
    status: str
    publish_status: str
    expires_at: pendulum.DateTime | None

    def is_accepted(self, now: pendulum.DateTime | None = None) -> bool:
        current = now or pendulum.now("UTC")
        if self.status in _ACCEPTED_TERMINAL_STATUSES:
            return True
        return (
            self.status in _ACCEPTED_OPEN_STATUSES
            and self.expires_at is not None
            and self.expires_at > current
        )
