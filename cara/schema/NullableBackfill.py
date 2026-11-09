"""The declared fill of a nullable column, as a batched operation.

A column's NOT NULL recipe fills in one UPDATE because ``SET NOT NULL`` needs
every row at once. Nothing later depends on a nullable column's fill, so it can
give the table back between slices. A whole-table UPDATE of 150,000 closed
orders held a writer of one of them for 5.5 s.
"""

from __future__ import annotations

from cara.exceptions import SchemaPlanRefused
from cara.schema.Operation import LOCKING, Operation

#: The column types a batched backfill can walk.
_SERIAL_TYPES = frozenset({"increments", "big_increments", "tiny_increments"})


def fills_nullable(declared: dict) -> bool:
    params = declared.get("params") or {}
    return bool(declared["nullable"]) and params.get("backfill_from") is not None


def refuse_partial_fill(params: dict) -> None:
    if params.get("backfill_where") is not None:
        raise SchemaPlanRefused(
            "NOT NULL with a backfill limited by where= — the rows it skips "
            "would still be NULL. Fill every row, or keep the column nullable"
        )


def batched_backfill(table: str, name: str, columns: dict[str, dict]) -> Operation:
    """Fill ``name`` from its ``backfill_from``, in slices of the table's key."""
    walk = [column for column, spec in columns.items() if spec["type"] in _SERIAL_TYPES]
    if len(walk) != 1:
        raise SchemaPlanRefused(
            "backfill_from on a nullable column is filled in batches over an "
            "auto-increment key, and this table has none to walk. Fill it "
            "with an explicit script and remove the declaration"
        )
    params = columns[name]["params"]
    if params.get("backfill_where_is_raw"):
        raise SchemaPlanRefused(
            "backfill_from's where= must be a string literal: discovery reads "
            "the model's source and cannot tell which rows this one limits to"
        )
    where = params.get("backfill_where")
    limited = f" AND ({where})" if where is not None else ""
    return Operation(
        kind="backfill_column",
        table=table,
        key=f"{table}.{name}:backfill",
        forward_sql=(
            f'UPDATE "{table}" SET "{name}" = {params["backfill_from"]} '
            f'WHERE "{name}" IS NULL{limited}'
        ),
        reverse_sql=None,
        safety=LOCKING,
        reason="fill existing rows from the declared backfill",
        restores_data=False,
        transactional=False,
        batch_column=walk[0],
        notes=(
            f"runs in short transactions over {walk[0]}; a stopped run "
            "re-walks and fills what is still NULL",
        ),
    )
