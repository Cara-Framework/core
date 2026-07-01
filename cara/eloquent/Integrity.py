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

import psycopg2

# Postgres error code for ``unique_violation`` — the only kind of
# IntegrityError these helpers care about. Other 23xxx codes
# (foreign-key, check, not-null) are different bugs and should
# propagate up rather than being swallowed by a "duplicate" handler.
_PG_UNIQUE_VIOLATION = "23505"

# Cap the ``__cause__``/``__context__`` walk so a pathological cyclic
# chain can never spin. Real driver→ORM wrapping is one or two links deep.
_MAX_CAUSE_DEPTH = 8


def _find_integrity_error(exc: BaseException | None) -> psycopg2.IntegrityError | None:
    """Return the first ``psycopg2.IntegrityError`` in the exception's cause chain.

    The ORM wraps every driver error in a ``QueryException(str(e)) from e``
    (see ``PostgresConnection.query``), so the ``psycopg2.IntegrityError`` a
    unique-violation race raises is NOT the top-level exception — it hangs off
    ``__cause__``. Checking only ``exc`` therefore misclassified every
    ORM-raised duplicate as "not a unique violation", silently defeating the
    SAVEPOINT/re-select race guards that call this helper. Walk the
    ``__cause__`` (explicit ``raise … from``) then ``__context__`` (implicit
    re-raise) chain so both the raw-driver and wrapped forms are recognised.
    """
    seen: set[int] = set()
    cur: BaseException | None = exc
    depth = 0
    while cur is not None and depth < _MAX_CAUSE_DEPTH and id(cur) not in seen:
        if isinstance(cur, psycopg2.IntegrityError):
            return cur
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
        depth += 1
    return None


def is_unique_violation(
    exc: Exception,
    *,
    constraint: str | None = None,
    column: str | None = None,
) -> bool:
    """Return True if ``exc`` is a Postgres unique-constraint violation.

    Args:
        exc: The raised exception. The check unwraps the ``__cause__`` /
            ``__context__`` chain, so a ``psycopg2.IntegrityError`` wrapped in
            the ORM's ``QueryException`` is recognised too. Anything whose
            chain holds no ``psycopg2.IntegrityError`` returns False —
            including other 23xxx integrity errors (FK, check) which should
            propagate.
        constraint: When given, additionally require the violation's
            ``diag.constraint_name`` to contain this substring (case-
            insensitive). Use this to scope the match — e.g.
            ``constraint="email"`` matches ``users_email_key`` and
            ``users_lower_email_idx`` but not
            ``users_public_id_key``.
        column: When given, additionally require the violation's
            ``diag.message_detail`` to mention this column (case-
            insensitive). Postgres formats the detail as
            ``"Key (<col>)=(<val>) already exists."``; matching on
            that is slightly more robust to constraint renames than
            the ``constraint`` arg.

    If both ``constraint`` and ``column`` are given, *either* match
    is sufficient — handy when the constraint name varies by
    migration but the column is stable.
    """
    integrity_exc = _find_integrity_error(exc)
    if integrity_exc is None:
        return False
    if getattr(integrity_exc, "pgcode", None) != _PG_UNIQUE_VIOLATION:
        return False

    # Bare unique-violation check — caller doesn't care which.
    if constraint is None and column is None:
        return True

    diag = getattr(integrity_exc, "diag", None)
    detail = (getattr(diag, "message_detail", "") or "").lower()
    constraint_name = (getattr(diag, "constraint_name", "") or "").lower()

    if constraint is not None and constraint.lower() in constraint_name:
        return True
    if column is not None and column.lower() in detail:
        return True
    return False


__all__ = ["is_unique_violation"]
