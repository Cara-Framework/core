"""Tables the database holds and no model declares — reported, never dropped.

Moved out of :mod:`cara.schema.Objects` (which plans the named objects a model
OWNS) because this answers the opposite question and plans nothing.
"""

from __future__ import annotations

from cara.schema.LiveSchema import LiveSchema


def orphaned_tables(model_tables: set[str], live: LiveSchema) -> list[str]:
    """Tables in the database that no model declares — REPORTED, never dropped.

    A DROP TABLE derived from a diff is where an autogenerating tool does its
    worst damage, because the diff cannot distinguish an abandoned table from
    a partition child, a table an extension owns, or the framework's own
    migration tracker. So this returns prose for a human, not an operation.
    """
    ignored = {"migrations"}
    # BASE TABLES only. A VIEW is never a table a model declares, so every
    # view in the schema matched this rule and was reported as an orphan
    # inviting a DROP — advice about a relation the application reads.
    orphans = sorted(live.base_table_names() - model_tables - ignored)
    return [
        f"table '{name}' exists in the database and no model declares it — "
        f"if it is obsolete, drop it by hand; the planner will not guess"
        for name in orphans
    ]


__all__ = ["orphaned_tables"]
