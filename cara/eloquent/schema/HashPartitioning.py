"""Declarative table partitioning — the one description of a partitioned table.

A model declares it beside ``__table__``::

    __partition__ = {"by": "hash", "key": "tenant_id", "partitions": 64}

and every consumer reads it from here: the migration renderer emits
``table.partition_by_hash(...)`` into the table's one generated migration,
the Postgres platform compiles it into ``PARTITION BY HASH`` plus one
``CREATE TABLE ... PARTITION OF`` per partition, and the schema gates compare
the live table with :meth:`HashPartitioning.live_shape`.

Only HASH exists. A hash partitioning is total: every key value has a
partition from the moment the table is created, so there is no "next
partition" to create and no maintenance job. RANGE / LIST bring that job with
them, and arrive with the first model that needs them — not before.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass

from cara.exceptions import PartitionDeclarationError

#: Postgres caps nothing near this; the bound keeps a typo (6400) from
#: creating thousands of relations.
MAX_PARTITIONS = 1024


@dataclass(frozen=True)
class HashPartitioning:
    """``PARTITION BY HASH (key)`` into ``partitions`` partitions."""

    key: str
    partitions: int

    method = "hash"

    def __post_init__(self):
        if not isinstance(self.key, str) or not self.key.isidentifier():
            raise PartitionDeclarationError(
                f"partition key must name one column, got {self.key!r}"
            )
        if (
            isinstance(self.partitions, bool)
            or not isinstance(self.partitions, int)
            or not 2 <= self.partitions <= MAX_PARTITIONS
        ):
            raise PartitionDeclarationError(
                f"hash partitions must be an int in 2..{MAX_PARTITIONS}, "
                f"got {self.partitions!r}"
            )

    # --- names and DDL -----------------------------------------------------

    def child_name(self, table: str, remainder: int) -> str:
        """``<table>_p<NN>``, zero-padded to the width of the last remainder."""
        width = len(str(self.partitions - 1))
        return f"{table}_p{remainder:0{width}d}"

    def bound(self, remainder: int) -> str:
        """The partition bound exactly as ``pg_get_expr(relpartbound)`` reads."""
        return f"FOR VALUES WITH (modulus {self.partitions}, remainder {remainder})"

    def children(self, table: str) -> dict[str, str]:
        """Every partition of ``table``: name -> bound."""
        return {
            self.child_name(table, remainder): self.bound(remainder)
            for remainder in range(self.partitions)
        }

    def clause(self) -> str:
        """The ``CREATE TABLE`` suffix."""
        return f'PARTITION BY HASH ("{self.key}")'

    def child_statements(self, table: str) -> list[str]:
        """``CREATE TABLE <child> PARTITION OF <table> <bound>``, in order."""
        return [
            f'CREATE TABLE "{child}" PARTITION OF "{table}" {bound}'
            for child, bound in self.children(table).items()
        ]

    def blueprint_line(self) -> str:
        """The generated migration's Blueprint call."""
        return f'table.partition_by_hash("{self.key}", {self.partitions})'

    # --- rules -------------------------------------------------------------

    def key_definition(self) -> str:
        """``pg_get_partkeydef`` of the declared parent."""
        return f"HASH ({self.key})"

    def live_shape(self, table: str) -> dict:
        """What the live catalogue holds for a table built from this."""
        return {"key": self.key_definition(), "children": self.children(table)}

    def require_key_in(self, kind: str, name: str, columns) -> None:
        """A partitioned table's PRIMARY KEY / UNIQUE must contain the key."""
        if self.key not in list(columns):
            raise PartitionDeclarationError(
                f"{kind} {name} ({', '.join(columns)}) omits the partition key "
                f"'{self.key}': on a partitioned table every PRIMARY KEY and "
                f"UNIQUE must contain it"
            )


def partitioning_from_declaration(declaration) -> HashPartitioning | None:
    """Parse a model's ``__partition__`` value; ``None`` for no declaration."""
    if declaration is None:
        return None
    if not isinstance(declaration, dict):
        raise PartitionDeclarationError(
            f"__partition__ must be a dict literal, got {declaration!r}"
        )
    unknown = set(declaration) - {"by", "key", "partitions"}
    if unknown:
        raise PartitionDeclarationError(
            f"__partition__ has unknown keys {sorted(unknown)}"
        )
    method = declaration.get("by")
    if method != "hash":
        raise PartitionDeclarationError(
            f"__partition__ 'by' must be 'hash', got {method!r} (RANGE and LIST "
            f"are not built: they need a partition-maintenance job no model "
            f"needs yet)"
        )
    return HashPartitioning(
        key=declaration.get("key"), partitions=declaration.get("partitions")
    )


def _modifier(field: dict, name: str) -> bool:
    """A chained modifier (``.nullable()``, ``.unique()``) of a discovered field."""
    return bool(field.get(name) or (field.get("params") or {}).get(name))


def validate_model_partitioning(model_info: dict) -> HashPartitioning | None:
    """The model's partitioning, checked against its own declarations.

    Called by the migration renderer, so a declaration Postgres would refuse
    fails ``make:migration`` rather than the first ``migrate``.
    """
    spec = partitioning_from_declaration(model_info.get("partition"))
    if spec is None:
        return None
    table = model_info.get("table")
    field = (model_info.get("fields") or {}).get(spec.key)
    if field is None:
        raise PartitionDeclarationError(
            f"{table}: partition key '{spec.key}' is not a declared field"
        )
    if _modifier(field, "nullable"):
        raise PartitionDeclarationError(
            f"{table}: partition key '{spec.key}' must be NOT NULL"
        )
    for declaration in model_info.get("composite_uniques", []):
        columns = declaration.get("columns") or []
        spec.require_key_in("UNIQUE", declaration.get("name") or "(unnamed)", columns)
    for name, definition in (model_info.get("fields") or {}).items():
        if _modifier(definition, "unique") and name != spec.key:
            spec.require_key_in("UNIQUE", name, [name])
    return spec


def migration_partition_drift(source: str, model_info: dict) -> str | None:
    """How a generated migration's partitioning differs from its model, or None.

    The column comparator cannot see storage, so a model that gained, lost or
    changed ``__partition__`` would read as in sync while every database built
    from the file got the old storage. An invalid declaration is reported,
    never raised: this runs inside the drift gates.
    """
    try:
        spec = validate_model_partitioning(model_info)
    except PartitionDeclarationError as exc:
        return f"invalid __partition__: {exc}"
    declared = [spec.blueprint_line()] if spec else []
    found = [
        ast.unparse(node).replace("'", '"')
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and getattr(node.func, "attr", None) == "partition_by_hash"
    ]
    if found == declared:
        return None
    return f"file has {', '.join(found) or 'no partitioning'}, model declares " + (
        declared[0] if declared else "none"
    )


__all__ = [
    "HashPartitioning",
    "MAX_PARTITIONS",
    "migration_partition_drift",
    "partitioning_from_declaration",
    "validate_model_partitioning",
]
