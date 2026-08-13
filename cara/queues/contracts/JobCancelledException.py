"""Canonical definition of ``JobCancelledException``."""

from __future__ import annotations

from cara.exceptions import QueueException


class JobCancelledException(QueueException):
    """Exception raised when a job is cancelled during execution.

    ``do_not_retry`` is the load-bearing signal, read by
    ``JobProcessor._route_failed_message``. A cancellation is a DECISION, not
    a fault: retrying it re-runs work an operator deliberately stopped.
    Pre-fix this exception declared nothing, so a cancelled job burned the
    full 1s/5s/30s retry schedule — re-executing the cancelled work twice
    more — before dead-lettering with ``Log.error`` tracebacks that read like
    a crash.
    """

    do_not_retry: bool = True

    def __init__(
        self, message: str = "Job was cancelled", tracking_id: str | None = None
    ):
        super().__init__(message)
        self.tracking_id = tracking_id
