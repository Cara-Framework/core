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

import psycopg2

from cara.eloquent.Integrity import is_unique_violation


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
        constraint_name = "product_container_container_signature_unique"
        message_detail = "Key (container_signature)=(amazon-us:B0X) already exists."

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
