"""Unit tests for ModelDiscoverer — the discovery stage of the
model-first migration generator.

These tests exercise the AST-based field/dependency extraction directly,
writing throwaway model source files to a temp dir and asserting on the
extracted ``model_info`` dicts. They pin the behaviours that previously
broke silently:

* chained single-column ``.index()`` is captured (was dropped),
* discovery + topological sort are deterministic run-to-run,
* a circular FK dependency breaks on the lexicographically-lowest table,
* ``*_id`` columns only become FKs when the target is a real table
  (no phantom ``public`` table from ``public_id``),
* ``uuid``/``double`` field types report faithfully.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from cara.eloquent.migrations.ModelDiscoverer import ModelDiscoverer


def _write_model(tmp_path: Path, filename: str, source: str) -> Path:
    path = tmp_path / filename
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    return path


@pytest.fixture
def discoverer() -> ModelDiscoverer:
    return ModelDiscoverer()


# --------------------------------------------------------------------------
# Fix 1: chained single-column .index() must not be silently dropped
# --------------------------------------------------------------------------


def test_chained_single_column_index_is_captured(discoverer, tmp_path):
    src = """
        from cara.eloquent.schema import Schema

        class Widget(Model):
            __table__ = "widget"

            @property
            def fields(self):
                return Schema.build(
                    lambda field: (
                        field.big_increments("id"),
                        field.string("name", 50).index(),
                        field.string("plain", 50),
                    )
                )
    """
    model_path = _write_model(tmp_path, "Widget.py", src)
    info = discoverer._parse_model_file(model_path)

    assert info is not None
    # The field itself is still extracted with its index flag.
    assert info["fields"]["name"]["params"].get("index") is True
    # And it is recorded as a single-column composite index so the
    # (shared) emitter renders table.index(["name"]).
    assert {"columns": ["name"], "name": None} in info["composite_indexes"]
    # Non-indexed column must NOT produce an index entry.
    assert ["plain"] not in [d["columns"] for d in info["composite_indexes"]]
    assert "index" not in info["fields"]["plain"]["params"]


def test_chained_index_coexists_with_composite_index(discoverer, tmp_path):
    src = """
        from cara.eloquent.schema import Schema

        class Combo(Model):
            __table__ = "combo"

            @property
            def fields(self):
                return Schema.build(
                    lambda field: (
                        field.big_increments("id"),
                        field.string("queue", 100).default("default").index(),
                        field.string("status", 50).index(),
                        field.index(["queue", "status"]),
                    )
                )
    """
    model_path = _write_model(tmp_path, "Combo.py", src)
    info = discoverer._parse_model_file(model_path)

    declared = [d["columns"] for d in info["composite_indexes"]]
    assert ["queue"] in declared
    assert ["status"] in declared
    assert ["queue", "status"] in declared
    # default("default") still captured alongside the chained index.
    assert info["fields"]["queue"]["params"].get("default") == "default"


def test_self_constant_default_resolves_to_literal(discoverer, tmp_path):
    """A ``field.string(...).default(self.STATUS_PENDING)`` schema default must
    resolve to the model's class-level literal, NOT be emitted verbatim: the
    generated migration is a standalone class with no ``STATUS_PENDING`` attr, so
    ``.default(self.STATUS_PENDING)`` raised AttributeError on up()."""
    src = """
        from cara.eloquent.schema import Schema

        class Ticket(Model):
            __table__ = "ticket"
            STATUS_PENDING = "pending"
            MAX_ATTEMPTS = 5

            @property
            def fields(self):
                return Schema.build(
                    lambda field: (
                        field.big_increments("id"),
                        field.string("status", 20).default(self.STATUS_PENDING),
                        field.integer("attempts").default(self.MAX_ATTEMPTS),
                    )
                )
    """
    model_path = _write_model(tmp_path, "Ticket.py", src)
    info = discoverer._parse_model_file(model_path)

    status = info["fields"]["status"]["params"]
    assert status.get("default") == "pending"
    assert not status.get("default_is_raw")  # resolved literal, emitted quoted
    # Non-string constants resolve too (rendered unquoted by the generator).
    assert info["fields"]["attempts"]["params"].get("default") == 5


# --------------------------------------------------------------------------
# Fix 2: deterministic discovery + topological sort
# --------------------------------------------------------------------------


def _model(table, fields, deps):
    return {"table": table, "fields": fields, "name": table.title()}


def test_topological_sort_is_deterministic_and_fk_respecting(discoverer):
    # parent <- child relationship via explicit foreign_key info.
    parent = {
        "table": "parent",
        "name": "Parent",
        "fields": {"id": {"type": "big_increments", "params": {}}},
    }
    child = {
        "table": "child",
        "name": "Child",
        "fields": {
            "id": {"type": "big_increments", "params": {}},
            "parent_id": {
                "type": "unsigned_big_integer",
                "params": {},
                "foreign_key": {"on": "parent", "references": "id"},
            },
        },
    }
    # Independent tables to exercise stable per-level ordering.
    alpha = {"table": "alpha", "name": "Alpha", "fields": {}}
    zeta = {"table": "zeta", "name": "Zeta", "fields": {}}

    order_a = [
        m["table"]
        for m in discoverer.resolve_dependency_order([zeta, child, alpha, parent])
    ]
    order_b = [
        m["table"]
        for m in discoverer.resolve_dependency_order([parent, alpha, child, zeta])
    ]

    # Same result regardless of input order → deterministic.
    assert order_a == order_b
    # parent must come before child (FK-respecting).
    assert order_a.index("parent") < order_a.index("child")
    # Independent level sorted lexicographically: alpha, parent, zeta
    # all have no satisfied-dep blocker on round 1.
    assert order_a[:3] == ["alpha", "parent", "zeta"]


# --------------------------------------------------------------------------
# Fix 3: circular dependency breaks on lexicographically-lowest table
# --------------------------------------------------------------------------


def test_circular_dependency_breaks_on_lowest_table_deterministically(discoverer):
    graph = {"beta": ["gamma"], "gamma": ["beta"]}
    models = [
        {"table": "gamma", "name": "Gamma", "fields": {}},
        {"table": "beta", "name": "Beta", "fields": {}},
    ]
    result = [m["table"] for m in discoverer._topological_sort(models, graph)]
    result2 = [
        m["table"] for m in discoverer._topological_sort(list(reversed(models)), graph)
    ]
    # beta < gamma → cycle broken on beta, stable across input orders.
    assert result == ["beta", "gamma"]
    assert result == result2


# --------------------------------------------------------------------------
# Fix 4: *_id columns only become FKs when the target is a real table
# --------------------------------------------------------------------------


def test_phantom_id_columns_are_not_foreign_keys(discoverer):
    tables = ["brand", "product", "seller", "users"]
    info = {"params": {}}
    # Real FK targets resolve to actual tables.
    assert discoverer._is_foreign_key_field("brand_id", info, tables)
    assert discoverer._extract_referenced_table("brand_id", info, tables) == "brand"
    assert discoverer._is_foreign_key_field("seller_id", info, tables)
    assert discoverer._extract_referenced_table("seller_id", info, tables) == "seller"
    # Plural alias: user_id -> users.
    assert discoverer._is_foreign_key_field("user_id", info, tables)
    assert discoverer._extract_referenced_table("user_id", info, tables) == "users"

    # Phantom columns: stripped target is not a known table → plain column.
    for col in ("public_id", "external_id", "correlation_id", "session_id"):
        assert not discoverer._is_foreign_key_field(col, info, tables), col
        assert discoverer._extract_referenced_table(col, info, tables) is None, col

    # merged_into_brand_id strips to merged_into_brand (not a table) → not a FK
    # via the implicit path (its real FK comes from an explicit field.foreign).
    assert not discoverer._is_foreign_key_field("merged_into_brand_id", info, tables)


def test_explicit_foreign_key_param_still_detected(discoverer):
    info = {"params": {"foreign_key": True}}
    # Even with no table set, the explicit flag wins.
    assert discoverer._is_foreign_key_field("anything_id", info, ["other"])


def test_phantom_fk_excluded_from_dependency_graph(discoverer):
    # A table whose only *_id column is a phantom (public_id) must have no
    # invented dependency on a non-existent "public" table.
    model = {
        "table": "thing",
        "name": "Thing",
        "fields": {
            "id": {"type": "big_increments", "params": {}},
            "public_id": {"type": "string", "params": {}},
        },
    }
    ordered = discoverer.resolve_dependency_order([model])
    assert ordered[0]["foreign_keys"] == []


# --------------------------------------------------------------------------
# Fix 6: uuid / double field types report faithfully
# --------------------------------------------------------------------------


def test_uuid_and_double_field_types_are_captured(discoverer, tmp_path):
    src = """
        from cara.eloquent.schema import Schema

        class Sample(Model):
            __table__ = "sample"

            @property
            def fields(self):
                return Schema.build(
                    lambda field: (
                        field.uuid("external_uuid"),
                        field.double("ratio"),
                    )
                )
    """
    model_path = _write_model(tmp_path, "Sample.py", src)
    info = discoverer._parse_model_file(model_path)
    assert info["fields"]["external_uuid"]["type"] == "uuid"
    assert info["fields"]["ratio"]["type"] == "double"


# --------------------------------------------------------------------------
# __indexes__ entries: f-string SQL must resolve, unresolvable SQL must RAISE
# --------------------------------------------------------------------------


def test_indexes_entry_with_module_constant_fstring_resolves(discoverer, tmp_path):
    """An interpolated predicate used to be dropped SILENTLY.

    ``_parse_indexes_attribute`` only accepted ``ast.Constant``, so an entry
    written as an f-string produced no ``up`` and was skipped — the generator
    emitted no DDL, schema:check had nothing to compare, and the index simply
    never existed while every gate stayed green.
    """
    src = '''
        from cara.schema import Schema

        _LIVE_SQL = "'active', 'trialing'"


        class Sub(Model):
            __table__ = "sub"

            __indexes__ = [
                {
                    "name": "sub_live_idx",
                    "up": (
                        "CREATE INDEX IF NOT EXISTS sub_live_idx "
                        "ON sub (tenant_id, id) "
                        f"WHERE status IN ({_LIVE_SQL})"
                    ),
                },
            ]

            @property
            def fields(self):
                return Schema.build(
                    lambda field: (
                        field.big_increments("id"),
                    )
                )
    '''
    info = discoverer._parse_model_file(_write_model(tmp_path, "Sub.py", src))

    assert [index["name"] for index in info["indexes"]] == ["sub_live_idx"]
    assert info["indexes"][0]["up"].endswith(
        "WHERE status IN ('active', 'trialing')"
    )
    # An omitted "down" still defaults to the matching DROP.
    assert info["indexes"][0]["down"] == "DROP INDEX IF EXISTS sub_live_idx"


def test_indexes_entry_with_unresolvable_sql_raises(discoverer, tmp_path):
    """Silently skipping is the dangerous outcome — fail loudly instead."""
    src = '''
        from cara.schema import Schema

        _STATES = ("a", "b")
        _COMPUTED = ", ".join(_STATES)


        class Sub(Model):
            __table__ = "sub"

            __indexes__ = [
                {
                    "name": "sub_computed_idx",
                    "up": (
                        "CREATE INDEX IF NOT EXISTS sub_computed_idx ON sub (id) "
                        f"WHERE status IN ({_COMPUTED})"
                    ),
                },
            ]

            @property
            def fields(self):
                return Schema.build(
                    lambda field: (
                        field.big_increments("id"),
                    )
                )
    '''
    model_path = _write_model(tmp_path, "Sub.py", src)

    with pytest.raises(RuntimeError, match="sub_computed_idx"):
        discoverer._parse_model_file(model_path)
