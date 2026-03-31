"""
Queue Interface for the Cara framework.

This module defines the contract that any queue driver must implement, specifying required methods
for queue operations.
"""

from typing import Any, Dict, List, Protocol, Union


class Queue(Protocol):
    """Protocol that any Queue driver must implement."""

    def push(self, *jobs: Any, options: Dict[str, Any]) -> Union[str, List[str]]:
        """Push one or more job objects onto the queue with given options. Returns job ID(s)."""

    def consume(self, options: Dict[str, Any]) -> None:
        """Start consuming jobs from the queue based on options."""

    def retry(self, options: Dict[str, Any]) -> None:
        """Retry failed jobs based on options."""

    def chain(self, jobs: List[Any], options: Dict[str, Any]) -> None:
        """Enqueue a sequence of jobs so that each runs only after its predecessor succeeds."""

    def batch(self, *jobs: Any, options: Dict[str, Any]) -> None:
        """
        Enqueue multiple jobs as a batch.

        They can be processed in parallel but tracked together.
        """

    def schedule(self, job: Any, when: Any, options: Dict[str, Any]) -> None:
        """
        Schedule a single job to run at a specific time or after a delay.

        'when' can be a datetime, pendulum.Duration, or human-readable string.
        """
