"""Partitions in a live catalogue: read them, then hide them behind their parent.

A partition is storage of its parent, never a table a model owns. Left in the
introspection it would be an orphan table, and every index Postgres attached
to it an orphan index. ``cara.schema.LiveSchema.introspect`` therefore reads
the partitioning here (parent -> ``{"key": pg_get_partkeydef, "children":
{partition: pg_get_expr(relpartbound)}}``) and drops every partition from the
per-table maps; ``cara.schema.PartitionDrift`` compares the shape it keeps
with the model's ``__partition__``.
"""

from __future__ import annotations

import dataclasses

#: The ``LiveSchema`` maps keyed by table name.
_PER_TABLE = (
    "tables",
    "checks",
    "indexes",
    "constraint_indexes",
    "constraints",
    "triggers",
    "relation_kinds",
    "constraint_definitions",
    "unique_constraints",
    "index_definitions",
    "invalid_indexes",
    "column_statistics",
)


def read_partitioning(run, schema_literal: str) -> dict[str, dict]:
    """Every partitioned parent in the schema, with its partitions' bounds."""
    partitioning: dict[str, dict] = {}
    for row in (
        run(
            "SELECT c.relname AS table_name, pg_get_partkeydef(c.oid) AS key_def "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{schema_literal}' AND c.relkind = 'p'"
        )
        or []
    ):
        partitioning[row["table_name"]] = {"key": row["key_def"], "children": {}}
    for row in (
        run(
            "SELECT p.relname AS parent_name, c.relname AS partition_name, "
            "pg_get_expr(c.relpartbound, c.oid) AS bound "
            "FROM pg_inherits i "
            "JOIN pg_class c ON c.oid = i.inhrelid "
            "JOIN pg_class p ON p.oid = i.inhparent "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{schema_literal}' AND c.relispartition"
        )
        or []
    ):
        partitioning.setdefault(row["parent_name"], {"key": None, "children": {}})[
            "children"
        ][row["partition_name"]] = row["bound"]
    return partitioning


def without_partitions(live, partitioning: dict[str, dict]):
    """``live`` with ``partitioning`` attached and every partition hidden."""
    partitions = {child for shape in partitioning.values() for child in shape["children"]}
    hidden = {
        name: {
            table: value
            for table, value in (getattr(live, name) or {}).items()
            if table not in partitions
        }
        for name in _PER_TABLE
    }
    return dataclasses.replace(live, partitioning=partitioning, **hidden)


__all__ = ["read_partitioning", "without_partitions"]
