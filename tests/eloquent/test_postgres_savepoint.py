"""Regression tests for the PostgresConnection savepoint fix.

Background
~~~~~~~~~~
Pre-fix, ``PostgresConnection.savepoint()`` toggled
``self._connection.autocommit`` mid-transaction. psycopg2 forbids that
— ``set_session cannot be used inside a transaction`` — so any nested
``begin()`` (which is implemented as a savepoint) crashed with an
opaque driver error and the surrounding caller's outer transaction
was left in an unknown state. The fix simply removes the toggle:
``begin()`` already set ``autocommit=False`` for the outer level when
the transaction opened, so the savepoint just inherits it.

These tests pin:
    1. ``savepoint()`` never touches ``autocommit``.
    2. The savepoint name validator still blocks SQL injection.
    3. Outer begin/commit toggles autocommit correctly.
    4. Nested begin/rollback/commit go through SAVEPOINT / RELEASE /
       ROLLBACK TO SAVEPOINT without re-toggling autocommit.
    5. ``transaction_level`` accounting stays balanced across deep
       nesting.
    6. Concurrent connection instances each get their own
       ``transaction_level`` counter (no shared state).

The driver is mocked — we are not asserting psycopg2's behavior,
we are asserting OUR contract with it. The pre-fix bug surfaced
because the contract was wrong; if we restore it, these tests fail.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from cara.eloquent.connections import PostgresConnection


def _make_conn() -> PostgresConnection:
    """Build a PostgresConnection wired to a MagicMock psycopg2 conn.

    The mock starts with ``autocommit=True`` (matches the default
    state ``make_connection`` would set after a successful connect).
    """
    pc = PostgresConnection(host="x", database="x", user="x", port=5432, password="x")
    fake = MagicMock(name="psycopg2_connection")
    fake.autocommit = True  # post-connect default
    fake.closed = False
    pc._connection = fake
    return pc


# ── savepoint() must not touch autocommit ─────────────────────────────


def test_savepoint_does_not_toggle_autocommit():
    """The actual fix: ``savepoint()`` issues ``SAVEPOINT name`` and
    nothing else. Touching autocommit here is what raised
    ``set_session cannot be used inside a transaction`` pre-fix."""
    pc = _make_conn()
    pc.begin()  # opens outer tx, sets autocommit=False
    pc._connection.autocommit = False  # confirm pre-condition

    pc.savepoint("sp_test")

    # Cursor used SAVEPOINT, autocommit untouched by savepoint().
    issued = [
        c.args[0] for c in pc._connection.cursor.return_value.execute.call_args_list
    ]
    assert any("SAVEPOINT sp_test" in s for s in issued)
    assert pc._connection.autocommit is False


def test_savepoint_increments_transaction_level():
    pc = _make_conn()
    pc.begin()
    assert pc.transaction_level == 1
    pc.savepoint("sp_1")
    assert pc.transaction_level == 2


# ── identifier validator (SQL injection guard) ────────────────────────


@pytest.mark.parametrize(
    "bad",
    [
        "sp_1; DROP TABLE users",  # statement injection
        "1bad",  # leading digit
        "bad name",  # space
        "",  # empty
        "name-with-dash",
        "name.with.dot",
        "name'quote",
        'name"dquote',
    ],
)
def test_savepoint_name_validator_rejects_injection(bad):
    pc = _make_conn()
    pc.begin()
    with pytest.raises(ValueError, match="Invalid savepoint name"):
        pc.savepoint(bad)


@pytest.mark.parametrize(
    "good",
    ["sp_1", "Savepoint1", "_underscore_lead", "x", "X" * 32],
)
def test_savepoint_name_validator_accepts_legal_identifiers(good):
    pc = _make_conn()
    pc.begin()
    pc.savepoint(good)  # should not raise


# ── begin() / commit() at outer level toggle autocommit ──────────────


def test_outer_begin_disables_autocommit_and_increments_level():
    pc = _make_conn()
    assert pc.transaction_level == 0
    assert pc._connection.autocommit is True

    pc.begin()

    assert pc.transaction_level == 1
    assert pc._connection.autocommit is False


def test_outer_commit_calls_commit_and_restores_autocommit():
    pc = _make_conn()
    pc.begin()
    pc.commit()

    pc._connection.commit.assert_called_once()
    assert pc._connection.autocommit is True
    assert pc.transaction_level == 0


def test_outer_rollback_calls_rollback_and_restores_autocommit():
    pc = _make_conn()
    pc.begin()
    pc.rollback()

    pc._connection.rollback.assert_called_once()
    assert pc._connection.autocommit is True
    assert pc.transaction_level == 0


def test_rollback_with_no_active_transaction_is_noop():
    pc = _make_conn()
    pc.rollback()  # must not raise, must not touch driver
    pc._connection.rollback.assert_not_called()
    assert pc.transaction_level == 0


# ── nested begin/commit cycles use savepoints, leave autocommit ──────


def test_nested_begin_uses_savepoint_and_leaves_autocommit_off():
    """begin() at level >0 must NOT toggle autocommit — the savepoint
    only exists inside the outer tx, where autocommit is already
    False."""
    pc = _make_conn()
    pc.begin()  # outer
    pc.begin()  # nested

    issued = [
        c.args[0] for c in pc._connection.cursor.return_value.execute.call_args_list
    ]
    assert any("SAVEPOINT sp_1" in s for s in issued)
    assert pc._connection.autocommit is False
    assert pc.transaction_level == 2


def test_nested_commit_releases_savepoint_without_touching_autocommit():
    pc = _make_conn()
    pc.begin()
    pc.begin()  # nested at level 2

    pc.commit()  # should RELEASE SAVEPOINT sp_1, not commit the tx

    issued = [
        c.args[0] for c in pc._connection.cursor.return_value.execute.call_args_list
    ]
    assert any("RELEASE SAVEPOINT sp_1" in s for s in issued)
    pc._connection.commit.assert_not_called()  # outer tx stays open
    assert pc._connection.autocommit is False
    assert pc.transaction_level == 1


def test_nested_rollback_rolls_back_to_savepoint_without_touching_autocommit():
    pc = _make_conn()
    pc.begin()
    pc.begin()  # nested at level 2

    pc.rollback()  # should ROLLBACK TO SAVEPOINT sp_1

    issued = [
        c.args[0] for c in pc._connection.cursor.return_value.execute.call_args_list
    ]
    assert any("ROLLBACK TO SAVEPOINT sp_1" in s for s in issued)
    pc._connection.rollback.assert_not_called()  # outer tx stays open
    assert pc._connection.autocommit is False
    assert pc.transaction_level == 1


def test_deep_nesting_keeps_transaction_level_balanced():
    """5 begin()s, 5 commit()s — counter returns to 0, outer tx
    commits exactly once, no autocommit thrash mid-stack."""
    pc = _make_conn()
    for _ in range(5):
        pc.begin()
    assert pc.transaction_level == 5
    assert pc._connection.autocommit is False

    for _ in range(5):
        pc.commit()
    assert pc.transaction_level == 0
    pc._connection.commit.assert_called_once()  # outer-most only
    assert pc._connection.autocommit is True


def test_deep_nesting_then_rollback_at_each_level_balances_counter():
    """5 begin()s then 5 rollback()s — counter returns to 0, outer
    tx rolls back exactly once."""
    pc = _make_conn()
    for _ in range(5):
        pc.begin()
    for _ in range(5):
        pc.rollback()
    assert pc.transaction_level == 0
    pc._connection.rollback.assert_called_once()  # outer-most only


def test_nested_commit_does_not_double_decrement_level():
    """release_savepoint() decrements transaction_level itself. Pre-
    fix release-path commits used to decrement twice, leaving the
    counter negative after one nested commit."""
    pc = _make_conn()
    pc.begin()  # level=1
    pc.begin()  # level=2
    pc.commit()  # release sp_1 → level=1, NOT 0
    assert pc.transaction_level == 1
    # Outer commit still possible:
    pc.commit()
    assert pc.transaction_level == 0


def test_nested_rollback_does_not_double_decrement_level():
    pc = _make_conn()
    pc.begin()  # level=1
    pc.begin()  # level=2
    pc.rollback()  # rollback_to_savepoint(sp_1) → level=1, NOT 0
    assert pc.transaction_level == 1
    pc.rollback()
    assert pc.transaction_level == 0


# ── per-instance state isolation (concurrent connections) ─────────────


def test_transaction_level_is_per_instance_not_shared():
    """Each PostgresConnection wraps its own psycopg2 connection;
    transaction_level lives on the instance and must not bleed
    between connections used from different threads."""
    a = _make_conn()
    b = _make_conn()

    a.begin()
    a.begin()  # a at level 2

    assert a.transaction_level == 2
    assert b.transaction_level == 0  # untouched

    b.begin()
    assert b.transaction_level == 1
    assert a.transaction_level == 2


def test_concurrent_threads_each_run_nested_tx_without_crosstalk():
    """Two threads each open their own PostgresConnection, do begin →
    nested begin → commit → commit. Their counters must remain
    independent, no exception raised, and each outer connection
    commits exactly once."""
    barrier = threading.Barrier(8)
    errors: list[BaseException] = []
    results: list[tuple[int, int]] = []

    def worker():
        try:
            pc = _make_conn()
            barrier.wait()  # maximize interleaving
            pc.begin()
            pc.begin()
            pc.commit()  # release sp_1
            pc.commit()  # commit outer
            results.append((pc.transaction_level, pc._connection.commit.call_count))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert not errors, f"Concurrent nested tx raised: {errors!r}"
    assert len(results) == 8
    for level, commits in results:
        assert level == 0
        assert commits == 1


def test_savepoint_name_validator_runs_before_any_sql_is_issued():
    """The validator must be the first thing savepoint() does — if a
    bad name slipped through to the cursor, it would already have
    been interpolated into the SQL string."""
    pc = _make_conn()
    pc.begin()
    pc._connection.cursor.reset_mock()  # clear begin()'s prior calls

    with pytest.raises(ValueError):
        pc.savepoint("bad; DROP TABLE x")

    # cursor() may be unused or the execute must NOT have happened.
    execute = pc._connection.cursor.return_value.execute
    for call in execute.call_args_list:
        assert "DROP TABLE" not in call.args[0]
