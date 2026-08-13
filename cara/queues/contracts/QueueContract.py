"""
Queue driver contract for the Cara framework.

This module defines the contract that any queue driver must implement, specifying required methods
for queue operations.
"""

from __future__ import annotations

from typing import Any, Protocol


class QueueContract(Protocol):
    """Protocol that any Queue driver must implement."""

    def ping(self, timeout_ms: int = 1000) -> None:
        """Verify that the driver's backing queue is reachable.

        Implementations must perform a real round-trip when the driver has an
        external dependency and raise when it cannot be reached.
        """

    def push(self, *jobs: Any, options: dict[str, Any]) -> str | list[str]:
        """Push one or more job objects onto the queue with given options. Returns job ID(s)."""

    def chain(self, jobs: list[Any], options: dict[str, Any]) -> None:
        """Enqueue a sequence of jobs so that each runs only after its predecessor succeeds."""

    def batch(self, *jobs: Any, options: dict[str, Any]) -> None:
        """
        Enqueue multiple jobs as a batch.

        They can be processed in parallel but tracked together.
        """

    def schedule(self, job: Any, when: Any, options: dict[str, Any]) -> None:
        """
        Schedule a single job to run at a specific time or after a delay.

        'when' can be a datetime, pendulum.Duration, or human-readable string.
        """
