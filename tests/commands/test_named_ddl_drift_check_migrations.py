"""``migrations:check`` reads a named DDL entry's TEXT, not only its name.

The structured comparator diffs columns; a CHECK carried in ``__indexes__``
whose text changed while the generated file kept the old statement read as in
sync (``listing_end_lifecycle_check``, 2026-11-12). Bare ``make:migration``
already caught it by diffing the file against the model's render.
"""

from __future__ import annotations

from cara.commands.core.MigrationsCheckCommand import audit_migrations
from cara.eloquent.migrations.MigrationGenerator import MigrationGenerator

_NAME = "listing_end_lifecycle_check"


def _model(expression):
    return {
        "name": "Listing",
        "table": "listing",
        "has_fields_method": True,
        "fields": {
            "id": {"type": "big_increments", "params": {}},
            "status": {"type": "string", "params": {"length": 20}},
        },
        "indexes": [
            {
                "name": _NAME,
                "up": f"ALTER TABLE listing ADD CONSTRAINT {_NAME} CHECK ({expression})",
                "down": f"ALTER TABLE listing DROP CONSTRAINT IF EXISTS {_NAME}",
            }
        ],
    }


_OLD = _model("status IN ('active', 'ended')")
_NEW = _model("status IN ('active', 'ended', 'draft')")


def _write(directory, model):
    path = directory / "0001_01_01_000001_create_listing_table.py"
    path.write_text(MigrationGenerator().generate_create_migration(model), "utf-8")
    return path


def test_a_named_check_whose_text_changed_fails_naming_the_constraint(tmp_path):
    _write(tmp_path, _OLD)

    violations = audit_migrations(tmp_path, {"listing": {_NAME}}, model_infos=[_NEW])

    drift = [v for v in violations if v.rule == "named-ddl-drift"]
    assert len(drift) == 1, violations
    assert _NAME in drift[0].message
    assert not drift[0].blocks_fix


def test_the_rendered_file_is_clean(tmp_path):
    _write(tmp_path, _NEW)

    assert audit_migrations(tmp_path, {"listing": {_NAME}}, model_infos=[_NEW]) == []


def test_whitespace_only_differences_are_not_drift(tmp_path):
    path = _write(tmp_path, _NEW)
    path.write_text(
        path.read_text("utf-8").replace(
            "CHECK (status IN", "CHECK  (status\n                IN"
        ),
        "utf-8",
    )

    assert audit_migrations(tmp_path, {"listing": {_NAME}}, model_infos=[_NEW]) == []
