"""End-to-end tests for COMPOSITE (multi-column) foreign keys in the
model-first migration generator.

A composite FK is declared on a model as::

    field.foreign(["a", "b"]).references(["x", "y"]).on("t").on_delete("CASCADE")

i.e. the local-column and referenced-column arguments are LISTS instead of
single strings. No production model uses this yet — these tests exercise the
capability synthetically across all three layers:

  * ModelDiscoverer  — parses the list-form into ``composite_foreign_keys``
    and registers the referenced table as a dependency,
  * MigrationGenerator — emits ``table.foreign([...]).references([...])...``,
  * schema Blueprint/Platform — the emitted line re-parses into a correct
    ``FOREIGN KEY (a, b) REFERENCES t (x, y)`` SQL constraint.

They also PIN the scalar FK path so the single-column behaviour stays
byte-identical (a ``str`` argument is unchanged end-to-end).
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from cara.eloquent.migrations.MigrationGenerator import MigrationGenerator
from cara.eloquent.migrations.ModelDiscoverer import ModelDiscoverer
from cara.eloquent.schema.Blueprint import Blueprint
from cara.eloquent.schema.platforms.PostgresPlatform import PostgresPlatform


def _write_model(tmp_path: Path, filename: str, source: str) -> Path:
    path = tmp_path / filename
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    return path


@pytest.fixture
def discoverer() -> ModelDiscoverer:
    return ModelDiscoverer()


@pytest.fixture
def generator() -> MigrationGenerator:
    # __new__ avoids the app-config bootstrap in __init__ (paths("migrations")).
    # The emit path under test only reads the __file__-relative stub.
    return MigrationGenerator.__new__(MigrationGenerator)


_COMPOSITE_MODEL_SRC = '''
    from cara.eloquent.schema import Schema

    class OrderLine(Model):
        __table__ = "order_line"

        @property
        def fields(self):
            return Schema.build(
                lambda field: (
                    field.big_increments("id"),
                    field.unsigned_big_integer("order_id"),
                    field.string("tenant_id", 50),
                    field.foreign(["order_id", "tenant_id"])
                    .references(["id", "tenant_id"])
                    .on("orders")
                    .on_delete("CASCADE"),
                )
            )
'''


# --------------------------------------------------------------------------
# Discoverer
# --------------------------------------------------------------------------


def test_discoverer_parses_composite_fk_into_its_own_collection(discoverer, tmp_path):
    model_path = _write_model(tmp_path, "OrderLine.py", _COMPOSITE_MODEL_SRC)
    info = discoverer._parse_model_file(model_path)

    assert info is not None
    # The composite FK lands in the dedicated top-level collection (the local
    # side is a list, so there is no single ``fields`` entry to hang it off).
    assert info["composite_foreign_keys"] == [
        {
            "columns": ["order_id", "tenant_id"],
            "references": ["id", "tenant_id"],
            "on": "orders",
            "on_delete": "CASCADE",
            "on_update": None,
        }
    ]
    # And it must NOT leak into any scalar ``fields[...]['foreign_key']`` slot.
    assert all("foreign_key" not in fi for fi in info["fields"].values())


def test_discoverer_registers_composite_fk_as_dependency(discoverer, tmp_path):
    """The referenced table must be an ordering dependency so the CREATE TABLE
    that adds the composite constraint runs after its target exists."""
    order_line_path = _write_model(tmp_path, "OrderLine.py", _COMPOSITE_MODEL_SRC)
    # A minimal ``orders`` parent so the dependency resolves to a real table.
    orders_path = _write_model(
        tmp_path,
        "Orders.py",
        '''
        from cara.eloquent.schema import Schema

        class Orders(Model):
            __table__ = "orders"

            @property
            def fields(self):
                return Schema.build(
                    lambda field: (
                        field.big_increments("id"),
                        field.string("tenant_id", 50),
                    )
                )
        ''',
    )

    models = [
        discoverer._parse_model_file(order_line_path),
        discoverer._parse_model_file(orders_path),
    ]
    ordered = discoverer.resolve_dependency_order(models)
    by_table = {m["table"]: m for m in ordered}

    # orders is emitted before order_line (FK-respecting order).
    tables = [m["table"] for m in ordered]
    assert tables.index("orders") < tables.index("order_line")

    # The composite FK shows up in the model's resolved foreign_keys, with the
    # local columns preserved as a list.
    fks = by_table["order_line"]["foreign_keys"]
    assert any(
        fk["references_table"] == "orders"
        and fk["field"] == ["order_id", "tenant_id"]
        for fk in fks
    )


def test_discoverer_skips_malformed_composite_fk(discoverer, tmp_path):
    """A composite local side with a mismatched ``references`` count is a
    malformed declaration — it must be dropped, not emitted broken."""
    src = '''
        from cara.eloquent.schema import Schema

        class Bad(Model):
            __table__ = "bad"

            @property
            def fields(self):
                return Schema.build(
                    lambda field: (
                        field.big_increments("id"),
                        field.unsigned_big_integer("a"),
                        field.unsigned_big_integer("b"),
                        field.foreign(["a", "b"]).references(["x"]).on("t"),
                    )
                )
    '''
    model_path = _write_model(tmp_path, "Bad.py", src)
    info = discoverer._parse_model_file(model_path)
    assert info["composite_foreign_keys"] == []


# --------------------------------------------------------------------------
# Generator
# --------------------------------------------------------------------------


def test_generator_emits_composite_fk_line(generator):
    fk_info = {
        "columns": ["order_id", "tenant_id"],
        "references": ["id", "tenant_id"],
        "on": "orders",
        "on_delete": "CASCADE",
        "on_update": None,
    }
    line = generator._generate_foreign_key_line(fk_info)
    assert line == (
        'table.foreign(["order_id", "tenant_id"])'
        '.references(["id", "tenant_id"]).on("orders").on_delete("CASCADE")'
    )


def test_generator_scalar_fk_line_unchanged(generator):
    """The scalar FK shape must emit exactly the historical single-column line."""
    fk_info = {
        "field": "user_id",
        "references": "id",
        "on": "users",
        "on_delete": "CASCADE",
        "on_update": None,
    }
    line = generator._generate_foreign_key_line(fk_info)
    assert line == (
        'table.foreign("user_id").references("id").on("users").on_delete("CASCADE")'
    )


def test_create_migration_contains_composite_fk_line(discoverer, generator, tmp_path):
    model_path = _write_model(tmp_path, "OrderLine.py", _COMPOSITE_MODEL_SRC)
    info = discoverer._parse_model_file(model_path)
    migration = generator._generate_blueprint_create_migration(info)

    assert (
        'table.foreign(["order_id", "tenant_id"])'
        '.references(["id", "tenant_id"]).on("orders").on_delete("CASCADE")'
    ) in migration


# --------------------------------------------------------------------------
# Schema layer: the emitted line re-parses into composite SQL
# --------------------------------------------------------------------------


def test_emitted_composite_line_renders_composite_sql(discoverer, generator, tmp_path):
    """Discover → emit → re-execute the emitted Blueprint call → assert SQL.

    Closes the loop: the line the generator writes is valid Cara schema syntax
    that produces a real multi-column FOREIGN KEY constraint.
    """
    model_path = _write_model(tmp_path, "OrderLine.py", _COMPOSITE_MODEL_SRC)
    info = discoverer._parse_model_file(model_path)
    assert info["composite_foreign_keys"], "composite FK was not discovered"

    # Re-run the emitted Blueprint chain against a fresh table named
    # "order_line" (table is the 2nd ctor arg; the 1st is the grammar).
    blueprint = Blueprint(None, table="order_line")
    blueprint.foreign(["order_id", "tenant_id"]).references(
        ["id", "tenant_id"]
    ).on("orders").on_delete("CASCADE")

    platform = PostgresPlatform()
    sql = platform.foreign_key_constraintize(
        "order_line", blueprint.table.added_foreign_keys
    )
    assert sql == [
        "CONSTRAINT order_line_order_id_tenant_id_foreign "
        'FOREIGN KEY ("order_id", "tenant_id") '
        'REFERENCES "orders"("id", "tenant_id") ON DELETE CASCADE'
    ]


def test_scalar_fk_sql_is_byte_identical():
    """Single-column FK SQL must be exactly what it was before composite
    support was added — the str argument path is untouched."""
    blueprint = Blueprint(None, table="orders")
    blueprint.foreign("user_id").references("id").on("users").on_delete("CASCADE")

    platform = PostgresPlatform()
    sql = platform.foreign_key_constraintize(
        "orders", blueprint.table.added_foreign_keys
    )
    assert sql == [
        "CONSTRAINT orders_user_id_foreign "
        'FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE'
    ]
