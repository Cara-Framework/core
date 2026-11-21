"""BuiltRelation — a relation of a read scope that is built ONCE, not per statement.

A scope's relation (``cara.eloquent.ReadScope``) is by default INLINE: every
statement that names it carries its SQL in front (``WITH name AS NOT
MATERIALIZED (<sql>)``), so the relation's own work is done again by each
statement — and again by each reference inside one. That is right for a
relation that is nearly the table itself (a filter the planner pushes into
the table's indexes) and wrong for one that costs real work to derive (a
table as it stood at a past instant, folded back from a log): a page of
forty statements derives it forty times.

A ``BuiltRelation`` is derived once per session of the block's snapshot
(``read_snapshot()``): the first statement that names it builds it as a
temporary table inside that session's transaction — with the lookup keys of
the table it stands for — and every later statement of the session reads
the built rows. Outside ``read_snapshot()`` there is no session to keep it
in, and it is read inline like any other relation: the same rows, derived
per statement.

``bound`` names the columns every row of the relation holds the same value
in (the workspace it was derived for): they lead no lookup key.

Generic: the SQL, its parameters and the column names are the caller's.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class BuiltRelation:
    """``(sql, params)`` of a scope relation to build once per session."""

    sql: str
    params: Sequence[Any] = ()
    bound: tuple[str, ...] = ()


__all__ = ["BuiltRelation"]
