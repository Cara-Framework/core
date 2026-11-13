"""A column statistics target is a declared schema object like any other.

``queue_job_delivery`` pins ``status`` / ``publish_status`` at statistics
target 0 (see ``tests/queues/test_relay_claim_plan_stability.py``). The
entry that does it creates no index, constraint or column, so before this
the planner could not tell whether a database already had it and refused
the whole plan. It now reads as ``statistics:<column>=<target>``, and a
database holds that object only when the target is set AND, for target 0,
no sample of the column remains — a 0 target over an old sample still
steers the planner, which is exactly the state the entry exists to end.
"""

from __future__ import annotations

from cara.schema import LiveSchema, plan
from cara.schema.Objects import created_objects

_UP = (
    "ALTER TABLE ledger ALTER COLUMN status SET STATISTICS 0, "
    'ALTER COLUMN "publish_status" SET STATISTICS 0'
)


def _model():
    return {
        "table": "ledger",
        "has_fields_method": True,
        "fields": {
            "id": {"type": "big_increments", "params": {}},
            "status": {"type": "string", "params": {"length": 24}},
            "publish_status": {"type": "string", "params": {"length": 16}},
        },
        "indexes": [{"name": "ledger_state_statistics", "up": _UP, "down": "SELECT 1"}],
    }


def _live(column_statistics):
    return LiveSchema(
        tables={
            "ledger": {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None},
                "status": {
                    "data_type": "character varying",
                    "is_nullable": False,
                    "max_length": 24,
                },
                "publish_status": {
                    "data_type": "character varying",
                    "is_nullable": False,
                    "max_length": 16,
                },
            }
        },
        checks={},
        indexes={},
        constraint_indexes={},
        relation_kinds={"ledger": "BASE TABLE"},
        column_statistics={"ledger": column_statistics},
    )


def test_a_statistics_target_reads_as_a_kind_qualified_object():
    assert created_objects(_UP) == {
        "statistics:status=0",
        "statistics:publish_status=0",
    }


def test_a_database_carrying_the_targets_plans_nothing():
    operations, _, _ = plan([_model()], _live({"status": 0, "publish_status": 0}))
    assert operations == []


def test_a_missing_target_is_planned_with_the_entry_sql():
    operations, _, _ = plan([_model()], _live({"status": 0}))
    assert [operation.forward_sql for operation in operations] == [_UP]


def test_live_schema_qualifies_column_statistics():
    live = _live({"status": 0})
    assert "statistics:status=0" in live.objects_on("ledger")
    assert "statistics:publish_status=0" not in live.objects_on("ledger")
