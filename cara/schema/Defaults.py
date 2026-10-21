"""A column's DEFAULT — declared and deployed — in one comparable form.

A model declares a default as a Python value (``.default("pending")``,
``.default(0)``, ``.use_current()``); a from-zero migration renders it into
DDL; ``information_schema.columns.column_default`` reports what the server
stored, decorated with casts (``'pending'::character varying``,
``'-1'::integer``). ``schema:check`` reports a difference and ``schema:plan``
repairs it, so both read defaults through these functions: a default the
checker calls equal and the planner calls different is a plan that never
empties, and the reverse is a drift gate that stays green over a database
whose INSERTs fail.

Nothing here guesses. A declared default that is a Python expression rather
than a value has no SQL spelling the planner could derive, so it is refused,
not rendered.
"""

from __future__ import annotations

import ast
import re
from decimal import Decimal, InvalidOperation

from cara.exceptions import SchemaPlanRefused

#: The words a from-zero migration renders as an expression rather than a
#: quoted literal — mirrors ``PostgresPlatform.premapped_defaults``, so the
#: same declaration means the same DEFAULT whichever tool rendered it.
_PREMAPPED_DEFAULTS = {"current": "CURRENT_TIMESTAMP", "now": "NOW()", "null": None}

#: One trailing ``::type`` cast as the server prints it, multi-word and
#: parameterised types included (``::character varying``,
#: ``::numeric(10,2)``, ``::text[]``).
_CAST_SUFFIX = re.compile(
    r"::\s*\"?[a-z_][\w ]*\"?(?:\(\s*\d+(?:\s*,\s*\d+)?\s*\))?(?:\[\])*\s*$",
    re.IGNORECASE,
)

#: Expressions Postgres treats as the same "now" — a column created with
#: ``DEFAULT now()`` and one with ``DEFAULT CURRENT_TIMESTAMP`` stamp alike.
_NOW_EXPRESSIONS = frozenset({"current_timestamp", "now()"})


def default_literal(value) -> str:
    """``value`` as a SQL literal: booleans and numbers bare, the rest quoted."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def declared_default_sql(params: dict | None) -> str | None:
    """The DEFAULT a model's field declaration puts in DDL, or None for none.

    Raises :class:`SchemaPlanRefused` for a default the discoverer kept as
    Python source (``default_is_raw``) that is not a plain value — there is no
    honest SQL rendering of an arbitrary expression.
    """
    params = params or {}
    # ``use_current`` overrides a declared value, as it does in the Blueprint.
    if params.get("use_current"):
        return "CURRENT_TIMESTAMP"
    value = params.get("default")
    if value is None:
        return None
    if params.get("default_is_raw"):
        value = _raw_default_value(value)
    if isinstance(value, str) and value in _PREMAPPED_DEFAULTS:
        return _PREMAPPED_DEFAULTS[value]
    return default_literal(value)


def live_default_sql(column_default: str | None) -> str | None:
    """A deployed ``column_default`` with the server's decoration removed.

    None for no default, and for ``nextval(...)``: a serial column's sequence
    belongs to its type (``increments``), never to a model's ``.default()``.
    """
    if column_default is None:
        return None
    text = str(column_default).strip()
    if text.lower().startswith("nextval("):
        return None
    while True:
        stripped = _CAST_SUFFIX.sub("", text).strip()
        if _wrapped_in_parentheses(stripped):
            stripped = stripped[1:-1].strip()
        if stripped == text:
            break
        text = stripped
    return None if text.upper() == "NULL" else text


def defaults_match(declared: str | None, live: str | None) -> bool:
    """Whether a declared and a deployed default mean the same value.

    Both sides are compared as values, not spellings: ``'-1'`` (how Postgres
    prints a negative integer default) equals ``-1``, ``1.0`` equals ``1``,
    ``FALSE`` equals ``false`` and ``now()`` equals ``CURRENT_TIMESTAMP``.
    """
    if declared is None or live is None:
        return declared is None and live is None
    return _canonical(declared) == _canonical(live)


def _raw_default_value(source):
    try:
        value = ast.literal_eval(str(source))
    except ValueError, SyntaxError, TypeError, MemoryError, RecursionError:
        value = None
    if isinstance(value, (bool, int, float, str)):
        return value
    raise SchemaPlanRefused(
        f"default {source!r} is a Python expression, not a value — it has no "
        "SQL spelling to derive. Declare a literal default or .use_current()"
    )


def _wrapped_in_parentheses(text: str) -> bool:
    """Whether one pair of parentheses encloses the WHOLE expression."""
    if not (text.startswith("(") and text.endswith(")")):
        return False
    depth = 0
    quoted = False
    for position, char in enumerate(text):
        if char == "'":
            quoted = not quoted
        elif not quoted and char == "(":
            depth += 1
        elif not quoted and char == ")":
            depth -= 1
            if depth == 0 and position != len(text) - 1:
                return False
    return depth == 0


def _number(text: str) -> Decimal | None:
    try:
        number = Decimal(text)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _canonical(sql: str):
    text = live_default_sql(sql) or ""
    if len(text) >= 2 and text.startswith("'") and text.endswith("'"):
        value = text[1:-1].replace("''", "'")
        number = _number(value)
        return value if number is None else number
    lowered = text.lower()
    if lowered in _NOW_EXPRESSIONS:
        return ("expression", "now()")
    if lowered in ("true", "false"):
        return lowered == "true"
    number = _number(text)
    if number is not None:
        return number
    return ("expression", lowered)


__all__ = [
    "declared_default_sql",
    "default_literal",
    "defaults_match",
    "live_default_sql",
]
