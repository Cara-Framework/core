"""UTC everywhere: the framework's own timestamp columns must be tz-AWARE.

``timestamps()`` and ``soft_deletes()`` used to build created_at / updated_at /
deleted_at through the naive ``timestamp()`` builder (Postgres TIMESTAMP), while
every hand-declared ``datetime()`` column compiled to TIMESTAMPTZ. Mixing the two
inside one expression makes Postgres insert a session-timezone-dependent cast,
which is NOT IMMUTABLE — so an index over e.g. ``COALESCE(last_seen_at,
created_at)`` cannot be built and a from-scratch ``craft migrate`` dies partway
through.

Separately, introspection mapped BOTH live TIMESTAMP variants onto ``datetime``,
so naive-vs-aware drift was structurally invisible and the bug above stayed
hidden across a whole database.
"""

from __future__ import annotations

import pytest

from cara.eloquent.schema.Blueprint import Blueprint
from cara.eloquent.schema.platforms.PostgresPlatform import PostgresPlatform


@pytest.fixture
def platform() -> PostgresPlatform:
    return PostgresPlatform()


def _create_sql(platform, build) -> str:
    blueprint = Blueprint(None, table="widget", platform=platform, action="create")
    blueprint.increments("id")
    build(blueprint)
    return blueprint.to_sql()[0]


# --- the framework columns compile tz-aware ---------------------------------


def test_timestamps_compile_to_timestamptz(platform):
    sql = _create_sql(platform, lambda b: b.timestamps())
    assert '"created_at" TIMESTAMPTZ' in sql
    assert '"updated_at" TIMESTAMPTZ' in sql
    # The naive spelling must be gone entirely — a bare "TIMESTAMP " would be
    # TIMESTAMP WITHOUT TIME ZONE.
    assert '"created_at" TIMESTAMP ' not in sql
    assert '"updated_at" TIMESTAMP ' not in sql


def test_soft_deletes_compiles_to_timestamptz(platform):
    sql = _create_sql(platform, lambda b: b.soft_deletes())
    assert '"deleted_at" TIMESTAMPTZ' in sql
    assert '"deleted_at" TIMESTAMP ' not in sql


def test_framework_and_declared_datetime_columns_share_one_type(platform):
    # The whole point: an index over COALESCE(last_seen_at, created_at) is only
    # buildable when both sides are the same tz-aware type.
    sql = _create_sql(
        platform,
        lambda b: (b.datetime("last_seen_at", nullable=True), b.timestamps()),
    )
    assert sql.count("TIMESTAMPTZ") == 3
    # ...and no column landed on the naive type behind their backs.
    assert sql.count("TIMESTAMP") == sql.count("TIMESTAMPTZ") + sql.count(
        "CURRENT_TIMESTAMP"
    )


def test_naive_timestamp_builder_still_available(platform):
    # ``timestamp()`` remains the escape hatch for a genuinely naive wall-clock
    # column — it just is no longer what the framework hands out by default.
    sql = _create_sql(platform, lambda b: b.timestamp("wall_clock", nullable=True))
    assert '"wall_clock" TIMESTAMP ' in sql
    assert '"wall_clock" TIMESTAMPTZ' not in sql


# --- introspection distinguishes naive from aware ---------------------------


def test_table_info_map_separates_naive_and_aware():
    # Collapsing both onto "datetime" is what made the drift invisible.
    assert PostgresPlatform.table_info_map["TIMESTAMP WITH TIME ZONE"] == "datetime"
    assert PostgresPlatform.table_info_map["TIMESTAMP WITHOUT TIME ZONE"] == "timestamp"


def test_introspection_reads_back_the_two_variants_as_different_types(platform):
    class _Connection:
        def query(self, sql, bindings):
            return [
                {
                    "column_name": "created_at",
                    "data_type": "timestamp with time zone",
                    "column_default": None,
                },
                {
                    "column_name": "wall_clock",
                    "data_type": "timestamp without time zone",
                    "column_default": None,
                },
            ]

    table = platform.get_current_schema(_Connection(), "widget")
    types = {name: column.column_type for name, column in table.added_columns.items()}
    assert types == {"created_at": "datetime", "wall_clock": "timestamp"}
