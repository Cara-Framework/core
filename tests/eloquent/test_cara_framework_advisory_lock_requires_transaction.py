"""``DB.advisory_xact_lock`` outside a transaction is refused, not a no-op.

A transaction-scoped advisory lock taken in autocommit is released when its
own ``SELECT`` ends, so the caller's next statements run unserialized while the
code reads as if they were guarded. A repository helper called from a path
that forgot ``DB.transaction()`` lost its fence silently.
"""

from __future__ import annotations

import pytest

from cara.eloquent.DatabaseManager import DatabaseManager


class _Recording(DatabaseManager):
    def __init__(self, level: int) -> None:
        self.level = level
        self.calls: list[str] = []

    def transaction_level(self, connection=None) -> int:
        return self.level

    def select(self, query, bindings=(), connection=None):
        self.calls.append(query)
        return []

    def select_one(self, query, bindings=(), connection=None):
        self.calls.append(query)
        return None


@pytest.mark.parametrize("keys", [("one",), ("one", "two")])
def test_outside_a_transaction_the_lock_is_refused(keys):
    db = _Recording(level=0)

    with pytest.raises(RuntimeError, match="needs an open transaction"):
        db.advisory_xact_lock(*keys)

    assert db.calls == [], "no statement may run for a lock that guards nothing"


def test_inside_a_transaction_the_lock_is_taken():
    db = _Recording(level=1)

    db.advisory_xact_lock("one")

    assert len(db.calls) == 1


def test_no_keys_never_needs_a_transaction():
    db = _Recording(level=0)

    db.advisory_xact_lock()

    assert db.calls == []
