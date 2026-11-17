"""Bounded drain of a batched write (retention prunes, redactions, sweeps).

A retention sweep deletes in bounded batches so no statement holds a long
lock; it stops when a batch comes back partial. Written as ``while True`` the
loop has no ceiling: a backlog (the first run after an outage, a table that
never had a sweep) keeps the job running past its execution budget, the
worker kills it mid-batch, and the next tick starts the same race again.

``drain_batches`` caps the run at ``max_batches``; whatever is left waits for
the next tick. A job's timeout can then be written as arithmetic
(``max_batches`` x the worst batch time) instead of a guess.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

__all__ = ["drain_batches"]


def drain_batches(
    step: Callable[[], Any],
    *,
    batch_size: int,
    max_batches: int,
) -> int:
    """Run ``step`` until it affects fewer than ``batch_size`` rows or the cap.

    ``step`` performs ONE bounded batch and returns its affected-row count
    (``None`` reads as 0). Returns the total. An exception from ``step``
    propagates unchanged: batches already committed stay committed and the
    caller decides how to log the failure.
    """
    if int(batch_size) < 1:
        raise ValueError("drain_batches: batch_size must be >= 1")
    if int(max_batches) < 1:
        raise ValueError("drain_batches: max_batches must be >= 1")
    total = 0
    for _ in range(int(max_batches)):
        affected = int(step() or 0)
        total += affected
        if affected < int(batch_size):
            break
    return total
