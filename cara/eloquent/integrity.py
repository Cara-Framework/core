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

from typing import Optional

import psycopg2


# Postgres error code for ``unique_violation`` — the only kind of
# IntegrityError these helpers care about. Other 23xxx codes
# (foreign-key, check, not-null) are different bugs and should
# propagate up rather than being swallowed by a "duplicate" handler.
_PG_UNIQUE_VIOLATION = "23505"


def is_unique_violation(
    exc: Exception,
    *,
    constraint: Optional[str] = None,
    column: Optional[str] = None,
) -> bool:
    """Return True if ``exc`` is a Postgres unique-constraint violation.

    Args:
        exc: The raised exception. Anything that isn't a
            ``psycopg2.IntegrityError`` returns False — including
            other 23xxx integrity errors (FK, check) which should
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
    if not isinstance(exc, psycopg2.IntegrityError):
        return False
    if getattr(exc, "pgcode", None) != _PG_UNIQUE_VIOLATION:
        return False

    # Bare unique-violation check — caller doesn't care which.
    if constraint is None and column is None:
        return True

    diag = getattr(exc, "diag", None)
    detail = (getattr(diag, "message_detail", "") or "").lower()
    constraint_name = (getattr(diag, "constraint_name", "") or "").lower()

    if constraint is not None and constraint.lower() in constraint_name:
        return True
    if column is not None and column.lower() in detail:
        return True
    return False


__all__ = ["is_unique_violation"]
