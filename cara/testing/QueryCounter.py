"""Count the database statements a block executes — the N+1 pin.

Every concrete connection runs its SQL through the inherited
:meth:`cara.eloquent.connections.BaseConnection.statement`: the ORM path
(``QueryBuilder`` → ``new_connection().query``) and every raw ``DB.select`` /
``DB.statement`` facade call end there. Wrapping that one chokepoint counts
exactly the round trips that reach the driver cursor — ORM and raw SQL alike,
eager-load sub-queries included — which a stub on ``DB.select`` never sees.

Usage (inside a live-database test)::

    from cara.testing import assert_query_count, count_queries

    with assert_query_count(maximum=6, label="product list"):
        service.list_products(payload)

    with count_queries() as counter:
        service.list_products(payload)
    assert counter.count == baseline  # does not grow with the rows

The wrapper is installed once while any counter is active (reference
counted under a lock) and restored when the last one exits, even if the
block raises. Nested blocks each tally only their own statements.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field

from cara.eloquent import BaseConnection

_ACTIVE: list[QueryCounter] = []
_LOCK = threading.Lock()
_ORIGINAL_STATEMENT = None


@dataclass
class QueryCounter:
    """The statements executed while this counter was active.

    ``queries`` keeps each SQL text whitespace-collapsed and truncated, so a
    failed ceiling shows WHICH query repeated once per row.
    """

    count: int = 0
    queries: list[str] = field(default_factory=list)

    def record(self, query: str) -> None:
        self.count += 1
        compact = " ".join(str(query).split())
        self.queries.append(compact[:160] + ("…" if len(compact) > 160 else ""))

    def report(self) -> str:
        """A numbered dump of every captured query, for assertion messages."""
        return "\n".join(f"  {i + 1:>2}: {q}" for i, q in enumerate(self.queries))


def _install_patch() -> None:
    global _ORIGINAL_STATEMENT
    if _ORIGINAL_STATEMENT is not None:
        return
    original = BaseConnection.statement
    _ORIGINAL_STATEMENT = original

    def _counting_statement(self, query, bindings=()):
        for counter in _ACTIVE:
            counter.record(query)
        return original(self, query, bindings)

    BaseConnection.statement = _counting_statement  # type: ignore[method-assign]


def _uninstall_patch() -> None:
    global _ORIGINAL_STATEMENT
    if _ORIGINAL_STATEMENT is None:
        return
    BaseConnection.statement = _ORIGINAL_STATEMENT  # type: ignore[method-assign]
    _ORIGINAL_STATEMENT = None


@contextmanager
def count_queries() -> Iterator[QueryCounter]:
    """Count the statements executed inside the block."""
    counter = QueryCounter()
    with _LOCK:
        _install_patch()
        _ACTIVE.append(counter)
    try:
        yield counter
    finally:
        with _LOCK:
            with suppress(ValueError):
                _ACTIVE.remove(counter)
            if not _ACTIVE:
                _uninstall_patch()


@contextmanager
def assert_query_count(*, maximum: int, label: str = "block") -> Iterator[QueryCounter]:
    """Fail unless the block executes at most ``maximum`` statements.

    Pick ``maximum`` from the measured count: a per-row loop scales with the
    seeded rows and always blows past it.
    """
    with count_queries() as counter:
        yield counter
    assert counter.count <= maximum, (
        f"{label}: executed {counter.count} DB statements, ceiling is "
        f"{maximum}. A count above the ceiling usually means an N+1 — a query "
        f"issued once per row instead of once per request. Captured queries:\n"
        f"{counter.report()}"
    )
