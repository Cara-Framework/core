"""The migration-shape audit, exercised against fixtures that always exist.

WHY THIS FILE WAS REWRITTEN
---------------------------
It used to walk ``Path(__file__).parents[3] / "database" / "migrations"`` and
skip when that directory was absent, "so the tests skip cleanly rather than
fail" in build contexts without the consumer's migrations.

From ``commons/cara/tests/migrations/``, ``parents[3]`` is ``commons`` — so the
path was ``commons/database/migrations``, which exists in no product: the
migrations live in each DEPLOYABLE (``api/database/migrations``, 149 files in
synkronus). The list was therefore always empty, the module-level ``skipif``
always true, and the reason text — "smoke test only runs in the full monorepo
checkout" — was false precisely in the full monorepo checkout. Both tests were
permanently skipped in both products, which reads as coverage and is not.

No path relative to this file reaches a real migration set, and hard-coding
``../../api/database/migrations`` would couple the framework to one product's
layout. So the RULE moved into ``MigrationShapeAudit`` where the framework can
own it, the products point it at their own directory from their own suites
(api/tests/test_migration_convention.py, services/tests/...), and what remains
here is the framework's job: proving the audit itself detects each defect.

The audit is static for the same reason ``audit_migrations`` is — importing a
migration can open a database connection at module scope.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cara.testing.audits import MigrationShapeAudit

GOOD = '''
from cara.migrations import Migration


class CreateWidgetTable(Migration):
    def up(self):
        pass

    def down(self):
        pass
'''


def _write(directory: Path, name: str, body: str) -> Path:
    path = directory / name
    path.write_text(body)
    return path


def test_a_well_formed_migration_raises_nothing(tmp_path: Path) -> None:
    _write(tmp_path, "2026_01_01_000000_create_widget_table.py", GOOD)
    assert MigrationShapeAudit(tmp_path).findings() == []


def test_init_is_not_a_migration(tmp_path: Path) -> None:
    _write(tmp_path, "__init__.py", "")
    assert MigrationShapeAudit(tmp_path).files() == []


def test_a_missing_directory_yields_no_files(tmp_path: Path) -> None:
    """Callers assert non-emptiness themselves — the audit does not guess."""
    assert MigrationShapeAudit(tmp_path / "nope").files() == []


@pytest.mark.parametrize(
    ("name", "body", "expected"),
    [
        ("unparseable", "class Broken(Migration:\n    pass\n", "does not parse"),
        ("no_subclass", "class NotAMigration:\n    pass\n", "declares no Migration subclass"),
        (
            "two_subclasses",
            "class A(Migration):\n    def up(self): pass\n    def down(self): pass\n"
            "class B(Migration):\n    def up(self): pass\n    def down(self): pass\n",
            "declares 2 Migration subclasses",
        ),
        (
            "missing_down",
            "class A(Migration):\n    def up(self): pass\n",
            "is missing down()",
        ),
        (
            "missing_up",
            "class A(Migration):\n    def down(self): pass\n",
            "is missing up()",
        ),
    ],
)
def test_each_defect_is_reported(tmp_path: Path, name: str, body: str, expected: str) -> None:
    _write(tmp_path, f"2026_01_01_000000_{name}.py", body)
    findings = MigrationShapeAudit(tmp_path).findings()
    assert findings, f"{name} must be reported"
    assert expected in str(findings[0])
    assert findings[0].path.endswith(f"{name}.py")


def test_a_dotted_base_still_counts_as_a_migration(tmp_path: Path) -> None:
    """``class X(migrations.Migration)`` is the same declaration."""
    _write(
        tmp_path,
        "2026_01_01_000000_dotted.py",
        "class A(migrations.Migration):\n    def up(self): pass\n    def down(self): pass\n",
    )
    assert MigrationShapeAudit(tmp_path).findings() == []


def test_async_methods_satisfy_the_runner_contract(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "2026_01_01_000000_async.py",
        "class A(Migration):\n    async def up(self): pass\n    async def down(self): pass\n",
    )
    assert MigrationShapeAudit(tmp_path).findings() == []


def test_files_are_returned_in_applied_order(tmp_path: Path) -> None:
    """Timestamps sort lexically; a finding list out of order misleads."""
    for stamp in ("2026_03_01_000000_c", "2026_01_01_000000_a", "2026_02_01_000000_b"):
        _write(tmp_path, f"{stamp}.py", GOOD)
    assert [path.name[:4] for path in MigrationShapeAudit(tmp_path).files()] == [
        "2026",
        "2026",
        "2026",
    ]
    names = [path.stem[-1] for path in MigrationShapeAudit(tmp_path).files()]
    assert names == ["a", "b", "c"]
