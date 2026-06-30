"""Per-test database isolation — wrap a test in a transaction that is ALWAYS
rolled back, so live-DB integration tests leave no trace.

Generic framework primitive (no app/domain assumptions): an app wires a pytest
fixture around :meth:`DatabaseTransactions.rolled_back`. Every query issued
inside the block joins the connection pinned in Cara's per-context transaction
registry (``ConnectionResolver._ACTIVE_CONNECTIONS``), so writes are visible
WITHIN the test but never durably committed — teardown rolls the outer
transaction back, discarding everything (including nested ``atomic()``
SAVEPOINTs).

Contract: the test body must not COMMIT past the fixture's outer ``BEGIN``
(i.e. don't drain the transaction to level 0). Repository ``atomic()`` blocks
are safe — under an open outer transaction they become SAVEPOINTs and unwind
symmetrically, so their "commit" only RELEASEs a savepoint and never persists.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

from cara.facades import DB

_logger = logging.getLogger("cara.testing.database")


class DatabaseTransactions:
    """Transactional test-isolation helper. See the module docstring."""

    @staticmethod
    @contextmanager
    def rolled_back(connection: str = "app") -> Iterator[None]:
        """Open an outer transaction, yield, and ALWAYS roll it back.

        Opening the transaction pins the connection in the context registry so
        every query in the block rides it; the teardown rollback discards all
        writes. Safe to use against a shared dev database — nothing persists.
        """
        DB.begin_transaction(connection)
        try:
            yield
        finally:
            try:
                DB.rollback(connection)
            except Exception:  # noqa: BLE001 — log misuse, never mask the test result
                # No active transaction at teardown means the test body
                # committed past the fixture's outer BEGIN, so writes may have
                # PERSISTED (isolation lost for this test). Surface it loudly
                # but do not raise — a teardown error must not swallow a test
                # assertion/exception already propagating.
                _logger.warning(
                    "DatabaseTransactions.rolled_back: no active transaction on "
                    "%r at teardown — the test drained its outer transaction, so "
                    "writes may have persisted (isolation lost).",
                    connection,
                    exc_info=True,
                )
