"""A named CHECK declared in ``__indexes__`` whose text changed under its name.

``changed_checks`` compared only field-level ``field.check`` declarations, so a
CHECK carried as named DDL (``ALTER TABLE ... ADD CONSTRAINT x CHECK (...)``)
was diffed by NAME alone: the model's ``listing_end_lifecycle_check`` could
change and ``schema:check`` still read the old live body as in sync
(logic audit, 2026-11-12).
"""

from __future__ import annotations

from cara.commands.core.SchemaCheckCommand import SchemaCheckCommand
from cara.schema import LiveSchema, changed_checks, declared_checks_by_table

_NAME = "listing_end_lifecycle_check"
_OLD = "CHECK (((ended_reason IS NULL) = (ended_at IS NULL)))"
_NEW = (
    "CHECK ((((ended_reason IS NULL) = (ended_at IS NULL)) AND "
    "((ended_at IS NULL) OR ((status)::text = 'ended'::text))))"
)


def _model(expression="(ended_reason IS NULL) = (ended_at IS NULL)"):
    return {
        "name": "Listing",
        "table": "listing",
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}},
        "indexes": [
            {
                "name": _NAME,
                "up": f"ALTER TABLE listing ADD CONSTRAINT {_NAME} CHECK ({expression})",
                "down": f"ALTER TABLE listing DROP CONSTRAINT IF EXISTS {_NAME}",
            }
        ],
    }


def _live(declared):
    return LiveSchema(
        tables={
            "listing": {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None}
            }
        },
        checks={"listing": {_NAME}},
        indexes={"listing": set()},
        constraint_indexes={"listing": set()},
        constraints={"listing": {_NAME}},
        relation_kinds={"listing": "BASE TABLE"},
        constraint_definitions={"listing": {_NAME: _OLD}},
        unique_constraints={"listing": set()},
        declared_check_definitions={"listing": {_NAME: declared}},
    )


_CHANGED = _model(
    "(ended_reason IS NULL) = (ended_at IS NULL) "
    "AND (ended_at IS NULL OR status = 'ended')"
)


def test_named_ddl_checks_are_handed_to_the_server_for_spelling():
    declared = declared_checks_by_table([_CHANGED])["listing"]

    assert (
        _NAME,
        "(ended_reason IS NULL) = (ended_at IS NULL) "
        "AND (ended_at IS NULL OR status = 'ended')",
    ) in declared


def test_a_changed_named_ddl_check_is_replaced():
    operations = changed_checks(_CHANGED, "listing", _live(_NEW + " NOT VALID"))

    assert [op.kind for op in operations] == ["replace_check", "validate_check"]
    assert operations[0].key == f"listing:{_NAME}"


def test_schema_check_names_the_changed_named_ddl_check():
    drift = SchemaCheckCommand(application=None)._diff_objects(
        _CHANGED, "listing", _live(_NEW + " NOT VALID")
    )

    assert any(_NAME in issue and "differs" in issue for issue in drift), drift


def test_an_unchanged_named_ddl_check_reads_in_sync():
    drift = SchemaCheckCommand(application=None)._diff_objects(
        _model(), "listing", _live(_OLD + " NOT VALID")
    )

    assert drift == []


def test_a_check_body_with_nested_parens_and_quotes_is_read_whole():
    model = _model("status IN ('a)', 'b') AND (x > 0)")

    assert declared_checks_by_table([model])["listing"] == [
        (_NAME, "status IN ('a)', 'b') AND (x > 0)")
    ]
