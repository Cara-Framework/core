"""The field-type vocabulary has ONE source: ``FieldBuilder``.

``ModelDiscoverer`` used to hand-copy the list of legal ``field.<type>(...)``
methods. Every omission erased columns in total silence, because a call the
AST parser cannot type yields no field definition at all:

  1. ``make:migration --overwrite`` writes ``create_<table>_table.py``
     WITHOUT the column, so a from-scratch install comes up missing it and
     every query touching it answers 500;
  2. ``migrations:check`` compares models to the migration files through the
     same blind view and stays green;
  3. ``schema:check`` compares them to the LIVE database through that same
     blind view and reports the column as present-but-not-declared — the
     drift gate accusing a correct model of not declaring a column it plainly
     declares.

That has been paid for twice already (the copy's own comments record
``jsonb``, which erased ``metadata`` from ~10 tables, and ``double``). It was
STILL missing ``char`` and ``binary``, both of which ``FieldBuilder`` offers
and a currency/country-code column would reach for tomorrow.

The vocabulary is now derived from ``FieldBuilder`` in ``schema/Schema.py``
and read by the discoverer, so adding a builder method is the only step
there is. These tests pin the derivation and the columns that used to fall
through it.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from cara.eloquent.migrations.MigrationGenerator import MigrationGenerator
from cara.eloquent.migrations.ModelDiscoverer import ModelDiscoverer
from cara.eloquent.schema.Schema import (
    CONSTRAINT_BUILDERS,
    FIELD_TYPES_WITH_NAMES,
    FIELD_TYPES_WITHOUT_NAMES,
    FieldBuilder,
    Schema,
)


def _write_model(tmp_path: Path, filename: str, source: str) -> Path:
    path = tmp_path / filename
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    return path


@pytest.fixture
def discoverer() -> ModelDiscoverer:
    return ModelDiscoverer()


# --------------------------------------------------------------------------
# The derivation itself
# --------------------------------------------------------------------------


def test_every_field_builder_method_is_a_known_field_type():
    """No builder method may be invisible to the migration parser.

    ``unique``/``index`` are constraint declarations taken through the
    parser's separate composite path, so they are excluded by name and that
    exclusion is stated once, in ``Schema.py``.
    """
    builders = {
        name for name in vars(FieldBuilder) if not name.startswith("_")
    } - CONSTRAINT_BUILDERS

    assert builders <= (FIELD_TYPES_WITH_NAMES | FIELD_TYPES_WITHOUT_NAMES)


def test_the_discoverer_reads_the_vocabulary_rather_than_restating_it():
    """Identity, not equality — a copy that happens to agree today is still
    a copy, and the whole failure mode is that copies drift apart later."""
    assert ModelDiscoverer.FIELD_TYPES_WITH_NAMES is FIELD_TYPES_WITH_NAMES
    assert ModelDiscoverer.FIELD_TYPES_WITHOUT_NAMES is FIELD_TYPES_WITHOUT_NAMES


def test_char_and_binary_are_in_the_vocabulary():
    """The two the hand-copied list was missing."""
    assert {"char", "binary"} <= FIELD_TYPES_WITH_NAMES


def test_id_is_not_a_field_type():
    """``field.id(...)`` is not something a model can write.

    The copy carried an ``"id"`` entry that no builder method backs, so the
    parser advertised a type ``Schema.build`` would have raised
    ``AttributeError`` on.
    """
    assert "id" not in FIELD_TYPES_WITH_NAMES
    assert not hasattr(FieldBuilder, "id")


# --------------------------------------------------------------------------
# The columns that used to be erased
# --------------------------------------------------------------------------


_MODEL_SOURCE = """
    from cara.eloquent.schema import Schema

    class Payment(Model):
        __table__ = "payment"

        @property
        def fields(self):
            return Schema.build(
                lambda field: (
                    field.big_increments("id"),
                    field.char("currency_code", 3),
                    field.binary("payload_digest"),
                )
            )
"""


def test_char_and_binary_columns_reach_the_discovered_model(discoverer, tmp_path):
    """Pre-fix ``info["fields"]`` held only ``id`` — both columns vanished."""
    model_path = _write_model(tmp_path, "Payment.py", _MODEL_SOURCE)
    info = discoverer._parse_model_file(model_path)

    assert info["fields"]["currency_code"]["type"] == "char"
    assert info["fields"]["payload_digest"]["type"] == "binary"


def test_char_keeps_its_declared_length(discoverer, tmp_path):
    """The length arg was read for ``string`` only.

    Recognising ``char`` without also reading its length would emit
    ``table.char("currency_code", 255)`` for a declared width of 3 — a
    silently widened column, which is a different flavour of the same lie.
    """
    model_path = _write_model(tmp_path, "Payment.py", _MODEL_SOURCE)
    info = discoverer._parse_model_file(model_path)

    assert info["fields"]["currency_code"]["params"]["length"] == 3


def test_generated_migration_carries_both_columns(discoverer, tmp_path):
    """End to end: the column has to survive into the emitted migration."""
    model_path = _write_model(tmp_path, "Payment.py", _MODEL_SOURCE)
    info = discoverer._parse_model_file(model_path)

    content = MigrationGenerator().generate_create_migration(info)

    assert 'table.char("currency_code", 3)' in content
    assert 'table.binary("payload_digest")' in content


# --------------------------------------------------------------------------
# The reverse drift: the builder was missing a type the rest of the stack had
# --------------------------------------------------------------------------


def test_field_builder_can_build_a_double():
    """``ColumnFactory.double``, ``PostgresPlatform`` DOUBLE PRECISION and the
    migration emitter all spoke ``double``; the builder a model actually calls
    did not, so ``field.double(...)`` raised inside ``Schema.build``."""
    (definition,) = Schema.build(lambda field: (field.double("ratio"),))

    assert definition.field_type == "double"
    assert definition.name == "ratio"


def test_unrecognised_field_call_is_announced(discoverer, tmp_path, caplog):
    """A dropped column must never be silent again."""
    src = """
        from cara.eloquent.schema import Schema

        class Widget(Model):
            __table__ = "widget"

            @property
            def fields(self):
                return Schema.build(
                    lambda field: (
                        field.big_increments("id"),
                        field.not_a_real_type("mystery"),
                    )
                )
    """
    model_path = _write_model(tmp_path, "Widget.py", src)

    with caplog.at_level("WARNING"):
        info = discoverer._parse_model_file(model_path)

    assert "mystery" not in info["fields"]
    assert "not_a_real_type" in caplog.text
