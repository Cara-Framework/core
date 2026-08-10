"""The rename emitter's type map is checked against the field-type SSOT.

``_RENAME_SQL_TYPE`` is the fourth place in the migration tool-chain that has
to know what a model field means in SQL. The other three now derive their
vocabulary from ``cara.eloquent.schema.Schema``; this one still has to spell
out a SQL type per field type, so what it can be held to is COVERAGE — every
type the DSL accepts is either mapped or explicitly declared column-less.

It mattered: an unmapped type used to fall back to ``"varchar"``, and SQLite
performs a rename by rebuilding the table, where that fallback resolved to an
EMPTY data type. The rebuilt column came out typeless and the generator
reported success.
"""

from __future__ import annotations

import pytest

from cara.eloquent.migrations.MigrationGenerator import (
    _NON_COLUMN_FIELD_TYPES,
    _RENAME_SQL_TYPE,
)
from cara.eloquent.schema.Schema import (
    FIELD_TYPES_WITH_NAMES,
    FIELD_TYPES_WITHOUT_NAMES,
)

DSL_FIELD_TYPES = FIELD_TYPES_WITH_NAMES | FIELD_TYPES_WITHOUT_NAMES


def test_every_dsl_field_type_is_mapped_or_declared_column_less() -> None:
    unaccounted = DSL_FIELD_TYPES - set(_RENAME_SQL_TYPE) - _NON_COLUMN_FIELD_TYPES

    assert not unaccounted, (
        f"these field types would emit a rename with no SQL type: {sorted(unaccounted)}"
    )


def test_the_two_sets_do_not_overlap() -> None:
    """A type is either a column or it is not — never both."""
    assert not (set(_RENAME_SQL_TYPE) & _NON_COLUMN_FIELD_TYPES)


def test_the_map_holds_no_type_the_dsl_cannot_produce() -> None:
    """A stale entry is drift in the other direction."""
    assert not set(_RENAME_SQL_TYPE) - DSL_FIELD_TYPES


@pytest.mark.parametrize(
    ("field_type", "sql_type"),
    [
        # ``datetime`` is tz-AWARE; emitting bare ``timestamp`` here silently
        # downgraded a renamed column to naive.
        ("datetime", "timestamptz"),
        ("timestamp", "timestamp"),
        ("increments", "serial"),
        ("big_increments", "bigserial"),
        ("binary", "bytea"),
    ],
)
def test_load_bearing_mappings(field_type: str, sql_type: str) -> None:
    assert _RENAME_SQL_TYPE[field_type] == sql_type
