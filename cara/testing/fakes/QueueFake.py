"""In-memory fake for the ``Queue`` facade.

Captures ``push``/``later``/``dispatch`` calls so tests can assert that
a particular job was enqueued, without an actual broker. Mirrors
Laravel's ``Queue::fake()``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, List, Optional


@dataclass
class QueuedJob:
    job: Any
    queue: Optional[str] = None
    delay: Optional[float] = None
    payload: Optional[dict] = None


class QueueFake:
    """Drop-in replacement for the ``Queue`` facade in tests."""

    def __init__(self) -> None:
        self.jobs: List[QueuedJob] = []

    # Production-side surface — accept any kwargs we don't model so a
    # caller using a ``priority=`` or ``options=`` flag still works.
    def push(self, job: Any, queue: Optional[str] = None, **kwargs: Any) -> None:
        self.jobs.append(QueuedJob(job=job, queue=queue, payload=kwargs or None))

    def later(
        self, delay: float, job: Any, queue: Optional[str] = None, **kwargs: Any
    ) -> None:
        self.jobs.append(
            QueuedJob(job=job, queue=queue, delay=delay, payload=kwargs or None)
        )

    def dispatch(self, job: Any, **kwargs: Any) -> None:
        self.push(job, **kwargs)

    # ── Assertions ───────────────────────────────────────────────────

    def all(self) -> List[QueuedJob]:
        return list(self.jobs)

    def count(self) -> int:
        return len(self.jobs)

    def pushed(self, predicate: Callable[[QueuedJob], bool]) -> List[QueuedJob]:
        return [j for j in self.jobs if predicate(j)]

    def assert_pushed(
        self,
        of_type: Optional[type] = None,
        *,
        where: Optional[Callable[[QueuedJob], bool]] = None,
        times: Optional[int] = None,
    ) -> None:
        matches = self.jobs
        if of_type is not None:
            matches = [j for j in matches if isinstance(j.job, of_type)]
        if where is not None:
            matches = [j for j in matches if where(j)]
        if times is not None and len(matches) != times:
            label = of_type.__name__ if of_type else "any job"
            raise AssertionError(
                f"Expected {times} pushes of {label}, got {len(matches)}"
            )
        if times is None and not matches:
            label = of_type.__name__ if of_type else "any job"
            raise AssertionError(f"Expected at least one push of {label}, got 0")

    def assert_nothing_pushed(self) -> None:
        if self.jobs:
            raise AssertionError(f"Expected no jobs queued, got {len(self.jobs)}")

    def clear(self) -> None:
        self.jobs.clear()
