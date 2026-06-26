"""SQL LIKE/ILIKE meta-character escaping.

Postgres' ``LIKE`` / ``ILIKE`` operators interpret ``%`` (any string)
and ``_`` (any single char) as wildcards. When a user's free-text
search term is interpolated raw into the parameterised value
(``LIKE %s`` with bound value ``"%"``), the user's ``%`` becomes a
wildcard at evaluation time — *not* a SQL injection (the parameter
is still bound), but a **wildcard injection** that silently widens
the match to every row.

Without this escape, every public search box that just does
``f"%{user_input}%"`` is exploitable as a "show me everything"
shortcut by typing a single ``%`` (or ``_``). That breaks the
intended scoping of in-record search, scoped search, etc., and
in adversarial use can be abused to harvest the full dataset while
appearing to send a normal search request.

Two helpers are exposed:

* :func:`escape_like` returns the user's value with the LIKE
  meta-characters (``\\``, ``%``, ``_``) prefixed with the escape
  character (``\\``). Caller composes the surrounding pattern.

* :func:`like_contains` is the most common shape — wraps the
  escaped value in ``%...%`` for substring containment.

Both expect the consuming SQL to declare the escape character
explicitly via ``LIKE %s ESCAPE '\\'`` (or use the orator builder's
``where(col, 'like', value)`` which generates the equivalent under
``standard_conforming_strings=on``, the Postgres default since 9.1).

Established prior art in this repo:
``app/repositories/SearchRepository.py::_escape_ilike`` and
``app/repositories/SmartCompletionRepository.py::_escape_like``
both do the same thing inline; this module is the canonical
single-source-of-truth so future search call-sites don't have to
re-derive the escape rule (or, more realistically, forget to).
"""

from __future__ import annotations

__all__ = ["escape_like", "like_contains", "LIKE_ESCAPE_CHAR"]


# We use ``\\`` as the escape char for parity with the existing
# in-repo helpers and with the Postgres documented default. Any
# single non-meta character would work; consistency with prior art
# matters more than cleverness.
LIKE_ESCAPE_CHAR = "\\"


def escape_like(value: str) -> str:
    """Escape SQL LIKE meta-characters in ``value``.

    Backslash MUST be escaped first — otherwise the backslashes we
    insert in front of ``%`` and ``_`` would themselves be doubled
    on a second pass.

    Returns the escaped string verbatim (no surrounding wildcards).
    Pair with ``ESCAPE '\\'`` in the SQL clause.
    """
    if not isinstance(value, str):
        return value  # mypy belt-and-braces; callers should pre-coerce
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def like_contains(value: str) -> str:
    """Return ``%escaped%`` — the canonical substring-LIKE pattern."""
    return f"%{escape_like(value)}%"
