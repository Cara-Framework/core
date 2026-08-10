"""Database-integrity helpers — recognise unique-violation races without string matching.

Three different services were each rolling their own way to spot
a unique-constraint violation:

* matching ``pgcode`` plus ``diag.constraint_name`` substring;
* a bare ``getattr(exc, "pgcode", None) == "23505"`` check;
* substring-matching ``str(exc).lower()`` against
  ``("unique", "duplicate", "integrity", …)`` — brittle, since any
  future Postgres error whose message happens to contain
  "duplicate" would misclassify.

This module collapses all three into one well-defined helper that
uses ``pgcode`` + structured ``diag`` fields, never message text,
and lets the caller scope the match to a specific constraint name
or column when they care (auth-email vs. wishlist-row vs.
review-author-product).

Generic — no domain assumptions. Apps pass their own constraint /
column names; the helper just knows the Postgres error shape.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from collections.abc import Iterator

_logger = logging.getLogger(__name__)

# Postgres error code for ``unique_violation`` — the only kind of
# IntegrityError these helpers care about. Other 23xxx codes
# (foreign-key, check, not-null) are different bugs and should
# propagate up rather than being swallowed by a "duplicate" handler.
_PG_UNIQUE_VIOLATION = "23505"

# Cap the ``__cause__``/``__context__`` walk so a pathological cyclic
# chain can never spin. Real driver→ORM wrapping is one or two links deep.
_MAX_CAUSE_DEPTH = 8

# SQLite has no SQLSTATE — the driver exposes nothing but the message. This
# is the ONE place in the framework allowed to read error text, and only for
# this driver: keeping the concession here means the rest of the codebase can
# keep asking a structured question. Format:
# ``UNIQUE constraint failed: users.email, users.tenant_id``.
_SQLITE_UNIQUE_PREFIX = "unique constraint failed:"

# Postgres reports the offending columns in ``diag.message_detail`` as
# ``Key (tenant_id, name)=(1, acme) already exists.`` — parse the parenthesised
# column list rather than searching the whole sentence for a substring.
_PG_DETAIL_COLUMNS = re.compile(r"key \(([^)]*)\)=")


def _cause_chain(exc: BaseException | None) -> Iterator[BaseException]:
    """Yield ``exc`` and every exception it wraps, outermost first.

    The ORM wraps every driver error in a ``QueryException(str(e)) from e``
    (see ``PostgresConnection.query``), so the driver exception a violation
    raises is NOT the top-level exception — it hangs off ``__cause__``.
    Checking only ``exc`` therefore misclassified every ORM-raised duplicate
    as "not a unique violation", silently defeating the SAVEPOINT/re-select
    race guards that call these helpers. Walk ``__cause__`` (explicit
    ``raise … from``) then ``__context__`` (implicit re-raise) so both the
    raw-driver and wrapped forms are recognised.

    One traversal, used by every predicate here. A second hand-rolled walk is
    how the classification drifted apart in the first place.
    """
    seen: set[int] = set()
    cur: BaseException | None = exc
    depth = 0
    while cur is not None and depth < _MAX_CAUSE_DEPTH and id(cur) not in seen:
        yield cur
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
        depth += 1


def sqlstate_of(exc: BaseException | None) -> str | None:
    """Return the SQLSTATE carried anywhere in ``exc``'s cause chain.

    Deliberately structural and driver-agnostic: it reads the ``pgcode``
    (psycopg2) / ``sqlstate`` (psycopg3) attribute wherever it appears rather
    than testing ``isinstance`` against a driver class, so it needs no import
    of the driver and sees the code through the ORM's wrapper.

    The unwrap is the whole point. ``PostgresConnection.query`` re-raises a
    deadlock as ``DatabaseUnavailableException(str(e)) from e``, and that
    wrapper carries no ``pgcode`` — so ``getattr(exc, "pgcode", None)`` on the
    exception a caller actually catches is always ``None``, and
    ``atomic(attempts=N)`` retried zero times in production while its unit
    tests, which raised a bare object with a ``pgcode``, stayed green.
    """
    for candidate in _cause_chain(exc):
        for attribute in ("pgcode", "sqlstate"):
            code = getattr(candidate, attribute, None)
            if code:
                return str(code)
    return None


def _unique_violation_carrier(exc: BaseException | None) -> BaseException | None:
    """Return the exception in the chain that CARRIES the unique-violation code.

    Structural, like :func:`sqlstate_of`: it asks for the SQLSTATE rather than
    testing ``isinstance`` against a driver class, so psycopg2 (``pgcode``) and
    psycopg3 (``sqlstate``) are both recognised. The previous version matched
    ``isinstance(candidate, psycopg2.IntegrityError)``, which meant a psycopg3
    ``UniqueViolation`` — same 23505, different class — was classified as "not a
    unique violation" even though the module already had the machinery to see it.

    The carrier is returned rather than a verdict because the caller needs its
    structured ``diag`` fields to scope the match.
    """
    for candidate in _cause_chain(exc):
        for attribute in ("pgcode", "sqlstate"):
            code = getattr(candidate, attribute, None)
            if code and str(code) == _PG_UNIQUE_VIOLATION:
                return candidate
    return None


def _detail_columns(detail: str) -> list[str]:
    """Column names Postgres names in ``Key (a, b)=(1, 2) already exists.``"""
    match = _PG_DETAIL_COLUMNS.search(detail)
    if match is None:
        return []
    return [part.strip().strip('"') for part in match.group(1).split(",") if part.strip()]


def _sqlite_unique_columns(exc: BaseException | None) -> list[str] | None:
    """Return the ``table.column`` names of a SQLite unique violation, if any.

    ``None`` means "not a SQLite unique violation"; a list means it is, and
    carries the columns so ``column=`` scoping still answers something truthful
    on this driver. SQLite names no constraint at all, so ``constraint=`` is
    unanswerable here — see :func:`is_unique_violation`.
    """
    for candidate in _cause_chain(exc):
        if not isinstance(candidate, sqlite3.IntegrityError):
            continue
        message = str(candidate).strip().lower()
        if not message.startswith(_SQLITE_UNIQUE_PREFIX):
            return None
        targets = message[len(_SQLITE_UNIQUE_PREFIX) :]
        return [part.strip() for part in targets.split(",") if part.strip()]
    return None


def _names_column(candidates: list[str], column: str) -> bool:
    """True if ``column`` is one of ``candidates``, compared as a whole token.

    ``candidates`` are either bare Postgres column names or SQLite's
    ``table.column`` pairs; the table qualifier is dropped before comparing.

    This is a token match, never a substring, and that is the whole point.
    Substring scoping is strictly MORE permissive than no scoping at all:
    ``column="name"`` matched ``display_name`` and ``template_name``, and
    ``column="e"`` matched ``users.email`` — so a filter callers added to
    NARROW a race guard quietly widened it, and an unrelated duplicate got
    swallowed as "our race". DOCTRINE §9: a gate that widens on the unclear
    case is the failure mode, not the safe default.
    """
    wanted = column.strip().lower()
    return any(candidate.rsplit(".", 1)[-1].strip() == wanted for candidate in candidates)


def is_unique_violation(
    exc: Exception,
    *,
    constraint: str | None = None,
    column: str | None = None,
) -> bool:
    """Return True if ``exc`` is a unique-constraint violation.

    Args:
        exc: The raised exception. The check unwraps the ``__cause__`` /
            ``__context__`` chain, so a driver error wrapped in the ORM's
            ``QueryException`` is recognised too. Anything whose chain carries
            no unique-violation code returns False — including the other 23xxx
            integrity errors (FK, check, not-null), which are different bugs
            and must propagate rather than be swallowed by a race guard.
        constraint: When given, additionally require the violation's
            ``diag.constraint_name`` to contain this substring (case-
            insensitive). Use this to scope the match — e.g.
            ``constraint="email"`` matches ``users_email_key`` and
            ``users_lower_email_idx`` but not ``users_public_id_key``.
            **Unanswerable on SQLite** — see below.
        column: When given, additionally require the violation to name this
            column, compared as a WHOLE TOKEN. Postgres reports it as
            ``"Key (<col>)=(<val>) already exists."``, SQLite as
            ``table.column``; both are parsed into column names and matched
            exactly. ``column="name"`` therefore does NOT match
            ``display_name`` — it used to, and a filter added to narrow a race
            guard silently widened it instead.

    If both ``constraint`` and ``column`` are given, *either* match is
    sufficient — handy when the constraint name varies by migration but the
    column is stable.

    **SQLite and ``constraint=``.** Both products run their suites on SQLite,
    and that driver publishes no SQLSTATE and no constraint name — only
    ``UNIQUE constraint failed: table.column``. Reading that message is the
    single documented exception to "never classify on error text", confined to
    this module. But it means a ``constraint=`` scope cannot be evaluated here:
    the previous code matched the Postgres constraint NAME against those
    ``table.column`` strings, which can never be equal, so every
    ``constraint=``-scoped guard silently returned False under SQLite and
    re-raised on a legitimate race — the exact failure the guard exists to
    prevent, hidden behind a green suite. Rather than invent an answer in
    either direction, the question is now refused loudly: a WARNING naming the
    limitation, and the ``column=`` scope alone decides. Scope by ``column=``
    (which SQLite can answer) whenever the guard must also hold in tests.
    """
    carrier = _unique_violation_carrier(exc)
    if carrier is not None:
        diag = getattr(carrier, "diag", None)
        columns = _detail_columns((getattr(diag, "message_detail", "") or "").lower())
        constraint_name = (getattr(diag, "constraint_name", "") or "").lower()
        return _scoped(
            constraint=constraint,
            column=column,
            constraint_name=constraint_name,
            columns=columns,
        )

    columns = _sqlite_unique_columns(exc)
    if columns is None:
        return False
    if constraint is None and column is None:
        return True
    if constraint is not None:
        _logger.warning(
            "is_unique_violation(constraint=%r) cannot be evaluated on SQLite: the "
            "driver names no constraint, only %s. Scope by column= instead.",
            constraint,
            ", ".join(columns) or "nothing",
        )
    # The constraint half is unanswerable, so only an explicit column scope can
    # say yes. Refusing (False → the caller re-raises) is the fail-closed
    # direction: an unrecognised duplicate surfaces instead of being silently
    # absorbed by a guard that was never able to check what it claimed to check.
    return column is not None and _names_column(columns, column)


def _scoped(
    *,
    constraint: str | None,
    column: str | None,
    constraint_name: str,
    columns: list[str],
) -> bool:
    """Apply the caller's ``constraint=``/``column=`` scope to a known violation.

    One implementation for both drivers: the drivers differ in how they REPORT
    a violation, not in what scoping means. Keeping the scope rule in two
    branches is how the two paths drifted into disagreeing about what
    ``column=`` matches.
    """
    if constraint is None and column is None:
        return True  # caller does not care which constraint — any will do
    if constraint is not None and constraint.lower() in constraint_name:
        return True
    return column is not None and _names_column(columns, column)


__all__ = ["is_unique_violation", "sqlstate_of"]
