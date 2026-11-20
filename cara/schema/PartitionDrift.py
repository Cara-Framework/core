"""Partitioning drift: a live table's partitioning against its model's ``__partition__``.

``LiveSchema`` hides every partition behind its parent, so the column, index
and orphan rules see one table. What they cannot see is the storage itself —
whether the parent is partitioned the way the model says, into exactly the
declared partitions. This module answers that, once, for ``schema:check``
(reported as drift) and ``schema:plan`` (a refusal: repartitioning an
existing table is a rebuild, never a derivable statement).

It also names the one planner limit partitioning brings: Postgres refuses
``CREATE INDEX CONCURRENTLY`` / ``DROP INDEX CONCURRENTLY`` on a partitioned
parent, and the planner's index operations are built concurrently. An index
change on a deployed partitioned table is therefore refused with that reason
instead of planned into a statement the server rejects.
"""

from __future__ import annotations

from cara.eloquent import partitioning_from_declaration
from cara.exceptions import PartitionDeclarationError
from cara.schema.LiveSchema import LiveSchema


def partition_drift(model: dict, table: str, live: LiveSchema) -> list[str]:
    """Human-readable differences; empty when storage matches the model."""
    try:
        spec = partitioning_from_declaration(model.get("partition"))
    except PartitionDeclarationError as exc:
        return [f"__partition__ is invalid: {exc}"]
    actual = (live.partitioning or {}).get(table)

    if spec is None:
        if actual is None:
            return []
        return [
            f"table is partitioned in the database ({actual.get('key')}) but the "
            f"model declares no __partition__"
        ]
    if actual is None:
        return [
            f"model declares {spec.key_definition()} partitioning into "
            f"{spec.partitions} partitions, but the database table is not partitioned"
        ]

    issues: list[str] = []
    if actual.get("key") != spec.key_definition():
        issues.append(
            f"partition key in database is {actual.get('key')}, model declares "
            f"{spec.key_definition()}"
        )
    declared = spec.children(table)
    present = actual.get("children") or {}
    for name in sorted(set(declared) - set(present)):
        issues.append(f"partition '{name}' declared but MISSING in database")
    for name in sorted(set(present) - set(declared)):
        issues.append(
            f"partition '{name}' ({present[name]}) present in database but NOT "
            f"declared by __partition__"
        )
    for name in sorted(set(declared) & set(present)):
        if declared[name] != present[name]:
            issues.append(
                f"partition '{name}' bound is {present[name]}, model declares "
                f"{declared[name]}"
            )
    return issues


def partition_refusals(planned, live: LiveSchema, operations, refusals: list[str]):
    """The plan's operations minus what a partitioned table refuses.

    Two refusals, appended to ``refusals`` so the plan reads as incomplete:
    a deployed table whose partitioning differs from its model (a rebuild —
    new table, copy, swap — is never a statement derived from a diff), and an
    index operation built ``CONCURRENTLY`` on a partitioned parent, which
    Postgres rejects (it needs a per-partition build and ``ATTACH``).
    """
    for model in planned:
        table = model["table"]
        if table in live.tables:
            refusals.extend(
                f"{table}: {issue} — repartitioning is a rebuild, not a plan step"
                for issue in partition_drift(model, table, live)
            )
    kept = []
    for operation in operations:
        if refuses_concurrent_build(operation.table, operation.forward_sql, live):
            refusals.append(
                f"{operation.key}: {operation.kind} on partitioned table "
                f"'{operation.table}' needs a per-partition build and ATTACH, "
                f"which the planner does not derive"
            )
        else:
            kept.append(operation)
    return kept


def is_partitioned(table: str, live: LiveSchema) -> bool:
    """Whether the deployed ``table`` is a partitioned parent."""
    return table in (live.partitioning or {})


def refuses_concurrent_build(table: str, forward_sql: str | None, live) -> bool:
    """A concurrent index statement on a deployed partitioned parent."""
    return is_partitioned(table, live) and "CONCURRENTLY" in (forward_sql or "").upper()


__all__ = [
    "is_partitioned",
    "partition_drift",
    "partition_refusals",
    "refuses_concurrent_build",
]
