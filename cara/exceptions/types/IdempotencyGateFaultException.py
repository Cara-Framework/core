"""IdempotencyGateFaultException."""

from __future__ import annotations

from .QueueException import QueueException


class IdempotencyGateFaultException(QueueException):
    """The idempotency gate could not ask its cache; the callback did not run.

    A queue throttle, not an execution failure: the worker redelivers without
    spending the job's failure budget. Deliberately not a
    ``JobThrottledException``, which ``Bus`` settles as a skip: an inline
    dispatch during a cache outage must fail at its caller, never report a
    quiet no-op.
    """

    is_throttle = True
    #: Every message starts with this. A dead-letter ledger keeps only the text,
    #: and this is how its triage tells an outage from a refusal.
    REASON = "Idempotency gate could not reach the cache"

    def __init__(self, message: str, retry_after: int | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after
