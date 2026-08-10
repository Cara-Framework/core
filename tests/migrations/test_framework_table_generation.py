"""The framework's own tables are generated like any other model's.

``failed_job`` and the delivery-ledger pair used to live as hand-written
``MODEL_LESS`` migrations copied into every product. They are models now
(``cara.models``), which is what lets the one-file-per-table contract hold
with zero exemptions. These tests pin the two pieces that make that work:

* the generator honours a declared natural primary key
  (``__primary_key__ = "job_id"`` over a VARCHAR) instead of injecting a
  surrogate ``increments("id")`` the model never declared — the exact gap
  that used to make the ledger impossible to model-generate, and
* the emitted files carry the ledger's full constraint surface, so a
  from-scratch install gets the same state machine the hand-written DDL
  used to create.

Discovery itself is exercised through parse, not by scanning this repo:
cara's own tree has no ``app/`` root, so the products' suites are where
end-to-end discovery of ``cara.models`` is proven.
"""

from __future__ import annotations

import ast
from pathlib import Path

from cara.eloquent.migrations.MigrationGenerator import MigrationGenerator
from cara.eloquent.migrations.ModelDiscoverer import ModelDiscoverer

_MODELS_DIR = Path(__file__).resolve().parents[2] / "cara" / "models"


def _model_info(stem: str) -> dict:
    discoverer = ModelDiscoverer()
    models = discoverer._scan_path_for_models(_MODELS_DIR, max_depth=1)
    for model in models:
        if model["name"] == stem:
            return model
    raise AssertionError(f"{stem} not discovered in {_MODELS_DIR}")


def test_framework_models_are_discoverable():
    names = {
        model["name"]: model["table"]
        for model in ModelDiscoverer()._scan_path_for_models(_MODELS_DIR, max_depth=1)
    }
    assert names == {
        "FailedJob": "failed_job",
        "QueueJobDelivery": "queue_job_delivery",
        "QueueJobDeliveryHookRetryAudit": "queue_job_delivery_hook_retry_audit",
    }


def test_declared_natural_primary_key_is_honoured():
    model = _model_info("QueueJobDelivery")
    assert model.get("primary_key") == "job_id"

    source = MigrationGenerator().generate_create_migration(model)

    assert 'table.primary("job_id")' in source
    # The old behaviour minted a surrogate id column the model never declared.
    assert 'table.increments("id")' not in source
    assert 'table.big_increments("id")' not in source
    ast.parse(source)


def test_models_without_a_natural_key_keep_the_surrogate_default():
    model = _model_info("FailedJob")

    source = MigrationGenerator().generate_create_migration(model)

    assert 'table.big_increments("id")' in source
    assert "table.primary(" not in source
    ast.parse(source)


def test_explicit_none_primary_key_generates_a_keyless_table(tmp_path):
    """``__primary_key__ = None`` means keyless BY DESIGN — a membership table
    addressed only through its parent's composite FK. The surrogate-id
    injection must distinguish that explicit declaration from a model that
    simply declared nothing."""
    (tmp_path / "MembershipRow.py").write_text(
        '"""Doc."""\n'
        "from cara.eloquent.models import Model\n"
        "from cara.eloquent.schema import Schema\n\n\n"
        "class MembershipRow(Model):\n"
        '    __table__ = "membership_row"\n'
        "    __primary_key__ = None\n\n"
        "    @property\n"
        "    def fields(self):\n"
        '        """Doc."""\n'
        "        return Schema.build(\n"
        "            lambda field: (\n"
        '                field.big_integer("parent_id"),\n'
        '                field.string("sku", 255).nullable(),\n'
        "            )\n"
        "        )\n",
        encoding="utf-8",
    )

    discoverer = ModelDiscoverer()
    model = next(
        m
        for m in discoverer._scan_path_for_models(tmp_path, max_depth=1)
        if m["name"] == "MembershipRow"
    )
    assert model.get("primary_key") is None
    assert "primary_key" in model

    source = MigrationGenerator().generate_create_migration(model)

    assert "increments(" not in source
    assert "table.primary(" not in source
    ast.parse(source)


def test_ledger_generation_carries_the_full_constraint_surface():
    model = _model_info("QueueJobDelivery")

    source = MigrationGenerator().generate_create_migration(model)

    # State machines, tenancy scope, both foreign keys, and the uniqueness
    # index adopted from the last incremental migration.
    for fragment in (
        "queue_job_delivery_execution_state_check",
        "queue_job_delivery_publish_state_check",
        "queue_job_delivery_tenant_scope_check",
        "queue_job_delivery_db_job_id_foreign FOREIGN KEY (db_job_id) REFERENCES job(id)",
        "queue_job_delivery_replay_of_foreign",
        "queue_job_delivery_open_unique_key_idx",
    ):
        assert fragment in source, fragment


def test_hook_retry_audit_references_the_ledger():
    model = _model_info("QueueJobDeliveryHookRetryAudit")

    source = MigrationGenerator().generate_create_migration(model)

    assert "REFERENCES queue_job_delivery(job_id)" in source
    assert "ON DELETE CASCADE" in source
