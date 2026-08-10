"""``cara.eloquent.Integrity.is_unique_violation`` — must see through the ORM
``QueryException`` wrapper.

``PostgresConnection.query`` re-raises every driver error as
``raise QueryException(str(e)) from e``, so a unique-violation race that reaches
application code is NOT a top-level ``psycopg2.IntegrityError`` — the real
``UniqueViolation`` (SQLSTATE 23505) hangs off ``__cause__``. The pre-fix
detector only inspected the top-level exception, so it returned ``False`` for
every ORM-raised duplicate and silently defeated the SAVEPOINT/re-select race
guards that call it (container consolidation, listing matching, listing
persist). These tests pin the ``__cause__`` / ``__context__`` unwrap so the
guards actually fire.
"""

from __future__ import annotations

import sqlite3

import psycopg2

from cara.eloquent.Integrity import is_unique_violation, sqlstate_of


class _UniqueViolation(psycopg2.IntegrityError):
    """A psycopg2 IntegrityError shaped like a 23505 (pgcode is a read-only
    driver attribute, so pin it at class level)."""

    pgcode = "23505"


class _ForeignKeyViolation(psycopg2.IntegrityError):
    pgcode = "23503"  # foreign_key_violation — must NOT be treated as unique


class _QueryException(Exception):
    """Stand-in for the ORM's wrapper (``QueryException(str(e)) from e``)."""


# ── direct (unwrapped) ───────────────────────────────────────────────────


def test_direct_unique_violation_detected() -> None:
    assert is_unique_violation(_UniqueViolation("duplicate key")) is True


def test_direct_non_unique_integrity_error_not_detected() -> None:
    assert is_unique_violation(_ForeignKeyViolation("fk")) is False


def test_unrelated_exception_not_detected() -> None:
    assert is_unique_violation(ValueError("not a db error")) is False


# ── wrapped (the fix) ────────────────────────────────────────────────────


def test_unique_violation_wrapped_in_query_exception_via_cause() -> None:
    """The production shape: ``raise QueryException(...) from unique_violation``."""
    inner = _UniqueViolation("duplicate key value violates unique constraint")
    wrapper = _QueryException("insert failed")
    wrapper.__cause__ = inner
    assert is_unique_violation(wrapper) is True


def test_unique_violation_wrapped_via_context() -> None:
    """Implicit chaining (``except: raise QueryException(...)`` with no ``from``)
    stores the original under ``__context__`` — unwrap that too."""
    inner = _UniqueViolation("duplicate key")
    wrapper = _QueryException("insert failed")
    wrapper.__context__ = inner
    assert is_unique_violation(wrapper) is True


def test_non_unique_violation_wrapped_not_detected() -> None:
    """A wrapped FK violation must still propagate — only 23505 is a race."""
    wrapper = _QueryException("insert failed")
    wrapper.__cause__ = _ForeignKeyViolation("fk")
    assert is_unique_violation(wrapper) is False


def test_double_wrapped_unique_violation_detected() -> None:
    inner = _UniqueViolation("dup")
    mid = _QueryException("mid")
    mid.__cause__ = inner
    outer = _QueryException("outer")
    outer.__cause__ = mid
    assert is_unique_violation(outer) is True


# ── scoping still works through the wrapper ──────────────────────────────


def test_constraint_scope_matches_through_wrapper() -> None:
    class _Diag:
        constraint_name = "widget_container_container_signature_unique"
        message_detail = "Key (container_signature)=(region-a:X1) already exists."

    # ``diag`` (like ``pgcode``) is a read-only driver attribute on instances,
    # so pin it at class level.
    class _UniqueViolationWithDiag(psycopg2.IntegrityError):
        pgcode = "23505"
        diag = _Diag()

    inner = _UniqueViolationWithDiag("dup")
    wrapper = _QueryException("insert failed")
    wrapper.__cause__ = inner

    assert is_unique_violation(wrapper, constraint="container_signature") is True
    assert is_unique_violation(wrapper, column="container_signature") is True
    assert is_unique_violation(wrapper, constraint="some_other_index") is False


# ── robustness ───────────────────────────────────────────────────────────


def test_cyclic_cause_chain_terminates_and_returns_false() -> None:
    a = _QueryException("a")
    b = _QueryException("b")
    a.__cause__ = b
    b.__cause__ = a  # cycle — must not spin
    assert is_unique_violation(a) is False


# ── sqlstate_of: one owner of "get a SQLSTATE out of a wrapped error" ─────


def test_sqlstate_of_reads_through_the_wrapper() -> None:
    """``Transactions._is_retriable_error`` used to hand-roll this and drop
    the unwrap, so it read ``None`` off every wrapped driver error and
    ``atomic(attempts=N)`` retried zero times."""
    inner = _UniqueViolation("dup")
    wrapper = _QueryException("insert failed")
    wrapper.__cause__ = inner

    assert sqlstate_of(wrapper) == "23505"


def test_sqlstate_of_needs_no_driver_class() -> None:
    """Deliberately structural: any exception publishing ``pgcode`` or the
    psycopg3 spelling ``sqlstate`` answers, so the helper works for wrapped
    and future driver shapes without an isinstance ladder."""

    class _Psycopg3Shaped(Exception):
        sqlstate = "40P01"

    wrapper = _QueryException("deadlock")
    wrapper.__cause__ = _Psycopg3Shaped("deadlock detected")

    assert sqlstate_of(wrapper) == "40P01"


def test_sqlstate_of_returns_none_for_a_plain_error() -> None:
    """Unknown stays unknown — never an invented code."""
    assert sqlstate_of(ValueError("not a db error")) is None
    assert sqlstate_of(None) is None


def test_sqlstate_of_terminates_on_a_cyclic_chain() -> None:
    a = _QueryException("a")
    b = _QueryException("b")
    a.__cause__ = b
    b.__cause__ = a
    assert sqlstate_of(a) is None


# ── SQLite: the one driver with no SQLSTATE ──────────────────────────────


def test_sqlite_unique_violation_detected_through_the_wrapper() -> None:
    """SQLite publishes no SQLSTATE and no constraint name, so its message is
    the only signal. Reading it is confined to ``Integrity`` — the point is
    that callers keep asking one structured question, not that no driver
    ever needs a concession."""
    wrapper = _QueryException("insert failed")
    wrapper.__cause__ = sqlite3.IntegrityError("UNIQUE constraint failed: users.email")

    assert is_unique_violation(wrapper) is True
    assert is_unique_violation(wrapper, column="email") is True
    assert is_unique_violation(wrapper, column="public_id") is False
    # `constraint="users"` used to answer True here, purely because the
    # constraint argument was substring-matched against the `table.column`
    # string `users.email`. That is not a constraint match — it is the table
    # name colliding with a substring test. SQLite names no constraint, so the
    # honest answer is a refusal (see the warning test below), not a
    # coincidence that happens to look right for this one table.
    assert is_unique_violation(wrapper, constraint="users") is False


def test_sqlite_other_integrity_errors_are_not_unique_violations() -> None:
    wrapper = _QueryException("insert failed")
    wrapper.__cause__ = sqlite3.IntegrityError("NOT NULL constraint failed: users.email")

    assert is_unique_violation(wrapper) is False


# ── scoping must NARROW, never widen (DOCTRINE §9) ───────────────────────────


class _Diag:
    """The structured ``diag`` psycopg exposes on an integrity error."""

    def __init__(self, constraint_name: str = "", message_detail: str = "") -> None:
        self.constraint_name = constraint_name
        self.message_detail = message_detail


class _ScopedViolation(Exception):
    """A 23505 that carries the ``diag`` fields the scope arguments read.

    Deliberately NOT a ``psycopg2.IntegrityError`` subclass: that class's
    ``diag`` is a read-only driver attribute, so a fake cannot populate it.
    The detector is structural now — it asks for the SQLSTATE and reads
    whatever ``diag`` the carrier exposes — so a plain object with the right
    shape is exactly what it must recognise.
    """

    pgcode = "23505"

    def __init__(self, *, constraint_name: str, message_detail: str) -> None:
        super().__init__(message_detail)
        self.diag = _Diag(constraint_name, message_detail)


def _pg_duplicate(*, constraint: str, columns: str) -> Exception:
    detail = f"Key ({columns})=(1) already exists."
    return _ScopedViolation(constraint_name=constraint, message_detail=detail)


def _sqlite_duplicate(targets: str) -> Exception:
    """A SQLite duplicate as the ORM delivers it — driver error under a wrapper."""
    driver_error = sqlite3.IntegrityError(f"UNIQUE constraint failed: {targets}")
    try:
        raise _QueryException(str(driver_error)) from driver_error
    except _QueryException as wrapped:
        return wrapped


def test_column_scope_is_a_token_not_a_substring_on_postgres():
    """``column="name"`` must not match ``display_name``.

    The scope argument exists to NARROW a race guard. Matched as a substring it
    did the opposite: an unrelated duplicate on ``display_name`` was swallowed
    as "our race" by a guard scoped to ``name``.
    """
    duplicate = _pg_duplicate(constraint="users_display_name_key", columns="display_name")
    assert is_unique_violation(duplicate, column="display_name") is True
    assert is_unique_violation(duplicate, column="name") is False


def test_column_scope_is_a_token_not_a_substring_on_sqlite():
    """The SQLite path was strictly more permissive than the Postgres one:
    ``column="e"`` matched ``users.email`` because it tested ``in``."""
    duplicate = _sqlite_duplicate("users.email, users.tenant_id")
    assert is_unique_violation(duplicate, column="email") is True
    assert is_unique_violation(duplicate, column="tenant_id") is True
    assert is_unique_violation(duplicate, column="e") is False
    assert is_unique_violation(duplicate, column="mail") is False


def test_multi_column_postgres_detail_is_parsed_into_tokens():
    duplicate = _pg_duplicate(
        constraint="supplier_tenant_name_unique", columns="tenant_id, name"
    )
    assert is_unique_violation(duplicate, column="tenant_id") is True
    assert is_unique_violation(duplicate, column="name") is True
    assert is_unique_violation(duplicate, column="id") is False


def test_constraint_scope_on_sqlite_warns_instead_of_answering_falsely(caplog):
    """SQLite names no constraint, so a ``constraint=`` scope is unanswerable.

    It used to be matched against ``table.column`` strings, which can never be
    equal — so every constraint-scoped guard silently returned False under the
    test connection and re-raised on a legitimate race, with a green suite.
    """
    duplicate = _sqlite_duplicate("suppliers.tenant_id, suppliers.name")
    with caplog.at_level("WARNING"):
        assert (
            is_unique_violation(duplicate, constraint="supplier_tenant_name_unique")
            is False
        )
    assert "cannot be evaluated on SQLite" in caplog.text

    # …and when the caller also scopes by column, that answerable half decides.
    assert (
        is_unique_violation(
            duplicate, constraint="supplier_tenant_name_unique", column="name"
        )
        is True
    )


def test_psycopg3_shaped_unique_violation_is_recognised():
    """psycopg3 spells the code ``sqlstate`` and is not a psycopg2 class.

    The detector used to gate on ``isinstance(candidate, psycopg2.IntegrityError)``
    while ``sqlstate_of`` right beside it was already driver-agnostic — so the
    module could see the code and still answered "not a unique violation".
    """

    class _Psycopg3UniqueViolation(Exception):
        sqlstate = "23505"

    driver_error = _Psycopg3UniqueViolation(
        "duplicate key value violates unique constraint"
    )
    try:
        raise _QueryException(str(driver_error)) from driver_error
    except _QueryException as wrapped:
        assert is_unique_violation(wrapped) is True
        assert sqlstate_of(wrapped) == "23505"
