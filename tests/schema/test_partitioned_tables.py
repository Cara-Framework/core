"""Declaratively partitioned tables: the DSL, the generated DDL, the gates.

``__partition__ = {"by": "hash", "key": ..., "partitions": n}`` is the one
description; the renderer, the Postgres platform, ``migrations:check``,
``schema:check`` and ``schema:plan`` all read it from
``cara.eloquent.schema.HashPartitioning``. The last test runs the generated DDL on
a real scratch database and proves the live catalogue reads back as exactly
what the model declares: no orphan table, no orphan index, no drift.
"""

from __future__ import annotations

import ast
import os
import uuid

import pytest

from cara.commands.core.MigrationsCheckCommand import audit_migrations
from cara.eloquent.migrations.MigrationGenerator import MigrationGenerator
from cara.eloquent.migrations.ModelDiscoverer import ModelDiscoverer
from cara.eloquent.schema import (
    Blueprint,
    HashPartitioning,
    PostgresPlatform,
    partitioning_from_declaration,
)
from cara.exceptions import PartitionDeclarationError
from cara.schema import (
    LiveSchema,
    introspect,
    orphaned_indexes,
    orphaned_tables,
    partition_drift,
)
from cara.schema.Planner import plan

_MODEL_SOURCE = '''"""Doc."""
from cara.eloquent.models import Model
from cara.eloquent.schema import Schema


class StreamRow(Model):
    __table__ = "stream_row"
    __partition__ = {"by": "hash", "key": "tenant_id", "partitions": 4}

    @property
    def fields(self):
        """Doc."""
        return Schema.build(
            lambda field: (
                field.big_increments("id"),
                field.big_integer("tenant_id"),
                field.string("stream", 40),
                field.integer("seq"),
                field.unique(["tenant_id", "stream", "seq"], name="stream_row_seq_unique"),
                field.index(["tenant_id", "seq"], name="stream_row_tenant_seq_idx"),
            )
        )
'''


def _discovered(tmp_path, source=_MODEL_SOURCE) -> dict:
    (tmp_path / "StreamRow.py").write_text(source, encoding="utf-8")
    return next(
        m
        for m in ModelDiscoverer()._scan_path_for_models(tmp_path, max_depth=1)
        if m["name"] == "StreamRow"
    )


def _create_sql(model: dict) -> list[str]:
    """Execute the generated migration's create block against a Blueprint."""
    source = MigrationGenerator().generate_create_migration(model)
    tree = ast.parse(source)
    body = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.With)
        and "schema.create" in ast.unparse(node.items[0].context_expr)
    ).body
    table = Blueprint(None, model["table"], platform=PostgresPlatform(), action="create")
    for statement in body:
        eval(
            compile(ast.Expression(statement.value), "<migration>", "eval"),
            {"table": table},
        )
    return table.to_sql()


# --- the DSL ---------------------------------------------------------------


def test_declaration_parses_into_a_hash_partitioning():
    spec = partitioning_from_declaration(
        {"by": "hash", "key": "tenant_id", "partitions": 64}
    )

    assert spec == HashPartitioning("tenant_id", 64)
    assert spec.child_name("event_log", 0) == "event_log_p00"
    assert spec.child_name("event_log", 63) == "event_log_p63"
    assert spec.bound(5) == "FOR VALUES WITH (modulus 64, remainder 5)"
    assert partitioning_from_declaration(None) is None


@pytest.mark.parametrize(
    "declaration",
    [
        {"by": "range", "key": "recorded_at", "partitions": 4},
        {"by": "list", "key": "tenant_id", "partitions": 4},
        {"by": "hash", "key": "tenant_id", "partitions": 1},
        {"by": "hash", "key": "tenant_id", "partitions": 5000},
        {"by": "hash", "key": "tenant_id", "partitions": True},
        {"by": "hash", "key": "tenant id", "partitions": 4},
        {"by": "hash", "key": "tenant_id", "partitions": 4, "extra": 1},
        "hash(tenant_id)",
    ],
)
def test_declarations_the_framework_cannot_build_are_refused(declaration):
    with pytest.raises(PartitionDeclarationError):
        partitioning_from_declaration(declaration)


# --- generated DDL ---------------------------------------------------------


def test_discovery_reads_the_declaration_and_the_migration_carries_it(tmp_path):
    model = _discovered(tmp_path)

    assert model["partition"] == {"by": "hash", "key": "tenant_id", "partitions": 4}
    source = MigrationGenerator().generate_create_migration(model)
    assert 'table.partition_by_hash("tenant_id", 4)' in source


def test_create_sql_partitions_the_table_and_keys_it_on_the_partition_key(tmp_path):
    sql = _create_sql(_discovered(tmp_path))

    create = sql[0]
    assert create.endswith('PARTITION BY HASH ("tenant_id")')
    assert '"id" BIGSERIAL NOT NULL' in create  # no column UNIQUE / PRIMARY KEY
    assert "CONSTRAINT stream_row_pkey PRIMARY KEY (tenant_id, id)" in create
    assert sql[1:5] == [
        f'CREATE TABLE "stream_row_p{r}" PARTITION OF "stream_row" '
        f"FOR VALUES WITH (modulus 4, remainder {r})"
        for r in range(4)
    ]


def test_an_unpartitioned_table_is_rendered_exactly_as_before():
    table = Blueprint(None, "plain", platform=PostgresPlatform(), action="create")
    table.big_increments("id")
    table.big_integer("tenant_id")

    assert table.to_sql() == [
        'CREATE TABLE "plain" ("id" BIGSERIAL UNIQUE NOT NULL PRIMARY KEY, '
        '"tenant_id" BIGINT NOT NULL)'
    ]


def test_a_unique_without_the_partition_key_fails_the_generator(tmp_path):
    source = _MODEL_SOURCE.replace(
        'field.unique(["tenant_id", "stream", "seq"]', 'field.unique(["stream", "seq"]'
    )
    model = _discovered(tmp_path, source)

    with pytest.raises(PartitionDeclarationError, match="omits the partition key"):
        MigrationGenerator().generate_create_migration(model)


def test_a_nullable_partition_key_fails_the_generator(tmp_path):
    source = _MODEL_SOURCE.replace(
        'field.big_integer("tenant_id")', 'field.big_integer("tenant_id").nullable()'
    )

    with pytest.raises(PartitionDeclarationError, match="NOT NULL"):
        MigrationGenerator().generate_create_migration(_discovered(tmp_path, source))


# --- migrations:check ------------------------------------------------------


def _write_migration(directory, model):
    path = directory / "0001_01_01_000001_create_stream_row_table.py"
    path.write_text(MigrationGenerator().generate_create_migration(model), "utf-8")
    return path


def _audit(directory, model):
    return audit_migrations(
        directory,
        {"stream_row": {"stream_row_tenant_seq_idx"}},
        model_infos=[model],
    )


def test_the_rendered_partitioned_file_is_clean(tmp_path):
    model = _discovered(tmp_path)
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    _write_migration(migrations, model)

    assert _audit(migrations, model) == []


@pytest.mark.parametrize(
    "edit",
    [
        lambda text: text.replace('table.partition_by_hash("tenant_id", 4)', ""),
        lambda text: text.replace(
            'partition_by_hash("tenant_id", 4)', 'partition_by_hash("tenant_id", 8)'
        ),
    ],
)
def test_a_file_whose_partitioning_differs_from_the_model_is_drift(tmp_path, edit):
    model = _discovered(tmp_path)
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    path = _write_migration(migrations, model)
    path.write_text(edit(path.read_text("utf-8")), "utf-8")

    drift = [v for v in _audit(migrations, model) if v.rule == "model-schema-drift"]

    assert drift and "__partition__" in drift[0].message


def test_a_file_partitioned_under_a_model_that_is_not_is_drift(tmp_path):
    model = _discovered(tmp_path)
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    _write_migration(migrations, model)
    unpartitioned = {k: v for k, v in model.items() if k != "partition"}

    drift = [
        v for v in _audit(migrations, unpartitioned) if v.rule == "model-schema-drift"
    ]

    assert drift and "__partition__" in drift[0].message


# --- schema:check / schema:plan over a live catalogue ------------------------


def _live(model, *, partitioning) -> LiveSchema:
    columns = {
        name: {"data_type": "bigint", "is_nullable": False}
        for name in ("id", "tenant_id")
    }
    return LiveSchema(
        tables={"stream_row": columns},
        checks={},
        indexes={"stream_row": {"stream_row_pkey", "stream_row_tenant_seq_idx"}},
        constraint_indexes={"stream_row": {"stream_row_pkey"}},
        partitioning=partitioning,
    )


def _shape(spec=HashPartitioning("tenant_id", 4)):
    return {"stream_row": spec.live_shape("stream_row")}


def test_storage_as_declared_is_not_drift(tmp_path):
    model = _discovered(tmp_path)

    assert partition_drift(model, "stream_row", _live(model, partitioning=_shape())) == []


def test_each_storage_difference_is_named(tmp_path):
    model = _discovered(tmp_path)
    shape = _shape()
    children = shape["stream_row"]["children"]
    del children["stream_row_p3"]
    children["stream_row_p9"] = "FOR VALUES WITH (modulus 16, remainder 9)"
    children["stream_row_p0"] = "FOR VALUES WITH (modulus 8, remainder 0)"
    shape["stream_row"]["key"] = "HASH (stream)"

    issues = partition_drift(model, "stream_row", _live(model, partitioning=shape))

    assert len(issues) == 4, issues
    assert any("partition key" in i for i in issues)
    assert any("'stream_row_p3' declared but MISSING" in i for i in issues)
    assert any("'stream_row_p9'" in i and "NOT declared" in i for i in issues)
    assert any("'stream_row_p0' bound" in i for i in issues)


def test_partitioned_in_one_place_only_is_drift_both_ways(tmp_path):
    model = _discovered(tmp_path)
    plain = {k: v for k, v in model.items() if k != "partition"}

    assert partition_drift(model, "stream_row", _live(model, partitioning={}))
    assert partition_drift(plain, "stream_row", _live(plain, partitioning=_shape()))


def test_the_planner_refuses_to_repartition_and_to_build_concurrently(tmp_path):
    model = _discovered(tmp_path)
    live = _live(model, partitioning=_shape(HashPartitioning("tenant_id", 8)))

    operations, refusals, _ = plan([model], live)

    assert any("repartitioning is a rebuild" in r for r in refusals), refusals
    # The missing seq-unique would be a CONCURRENTLY build on the parent.
    assert any("partitioned table 'stream_row'" in r for r in refusals), refusals
    assert not [o for o in operations if "CONCURRENTLY" in (o.forward_sql or "")]


# --- a real database -------------------------------------------------------

_INTEGRATION = os.getenv("CARA_RUN_INTEGRATION") == "1" and os.getenv(
    "DB_DATABASE", ""
).startswith("synk_scratch_")


@pytest.mark.skipif(
    not _INTEGRATION,
    reason="set CARA_RUN_INTEGRATION=1 and DB_DATABASE named synk_scratch_*",
)
def test_generated_ddl_reads_back_from_postgres_as_exactly_the_model(tmp_path):
    import psycopg2
    import psycopg2.extras

    model = _discovered(tmp_path)
    schema = f"part_{uuid.uuid4().hex[:8]}"
    connection = psycopg2.connect(
        dbname=os.environ["DB_DATABASE"],
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USERNAME") or None,
        password=os.getenv("DB_PASSWORD") or None,
    )
    connection.autocommit = True

    class _Executor:
        def get_query_result(self, sql):
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                return [dict(row) for row in cur.fetchall()]

    class _Schema:
        query_executor = _Executor()

        def get_schema(self):
            return schema

    try:
        with connection.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}"')
            for statement in _create_sql(model):
                cur.execute(statement)
            # A raw INSERT routes to its partition; the parent holds the row.
            cur.execute(
                "INSERT INTO stream_row (tenant_id, stream, seq) VALUES (7, 's', 1)"
            )
            cur.execute("SELECT count(*) FROM stream_row")
            assert cur.fetchone()[0] == 1

        live = introspect(_Schema())

        assert set(live.tables) == {"stream_row"}
        assert partition_drift(model, "stream_row", live) == []
        assert orphaned_tables({"stream_row"}, live) == []
        assert orphaned_indexes(model, "stream_row", live) == []
        assert live.partitioning["stream_row"]["key"] == "HASH (tenant_id)"
    finally:
        with connection.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        connection.close()
