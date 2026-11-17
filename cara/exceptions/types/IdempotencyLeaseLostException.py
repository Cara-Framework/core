"""IdempotencyLeaseLostException."""

from __future__ import annotations

from .QueueException import QueueException


class IdempotencyLeaseLostException(QueueException):
    """A run's ``job_lock:`` lease lapsed and was taken while it still ran.

    Another run of the same key may already be executing, so the stale run
    was cancelled at its next ``await`` (``LeaseHeartbeat`` fence) rather than
    let it commit beside the new owner. A queue throttle, not an execution
    failure: the job is redelivered behind the new owner without spending its
    failure budget, and its result was never cached.
    """

    is_throttle = True
    #: Every message starts with this; a dead-letter ledger keeps only the text.
    REASON = "Idempotency lease was lost while the job ran"

    def __init__(self, message: str, retry_after: int | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after
