from __future__ import annotations

import importlib.util
import sys
from types import ModuleType

import pytest

from cara.eloquent.migrations.MigrationFileManager import MigrationFileManager


def _write_migration(tmp_path, source: str):
    path = tmp_path / "0001_01_01_000000_test_migration.py"
    path.write_text(source)
    return path


def test_loader_registers_module_while_dataclass_decorators_run(tmp_path):
    path = _write_migration(
        tmp_path,
        """\
from __future__ import annotations

from dataclasses import dataclass

from cara.eloquent.migrations.Migration import Migration


@dataclass(slots=True)
class DataclassMigration(Migration):
    marker: str = "loaded"

    def up(self):
        pass

    def down(self):
        pass
""",
    )

    migration_class = MigrationFileManager(tmp_path).load_migration_class(path)

    try:
        assert migration_class.__name__ == "DataclassMigration"
        assert migration_class().marker == "loaded"
        assert (
            sys.modules[migration_class.__module__].DataclassMigration is migration_class
        )
    finally:
        sys.modules.pop(migration_class.__module__, None)


def test_loader_removes_half_initialized_module_when_execution_fails(tmp_path):
    path = _write_migration(
        tmp_path,
        """\
from __future__ import annotations

raise RuntimeError(__name__)
""",
    )

    before = set(sys.modules)
    with pytest.raises(RuntimeError) as raised:
        MigrationFileManager(tmp_path).load_migration_class(path)

    module_name = str(raised.value)
    assert module_name not in before
    assert module_name not in sys.modules


def test_loader_restores_a_preexisting_module_entry_after_failure(
    tmp_path,
    monkeypatch,
):
    path = _write_migration(
        tmp_path,
        """\
raise RuntimeError("broken migration")
""",
    )
    previous = ModuleType("previous_migration_module")
    captured_name = None
    original_spec_from_file_location = importlib.util.spec_from_file_location

    def capture_and_preload(module_name, location):
        nonlocal captured_name
        captured_name = module_name
        monkeypatch.setitem(sys.modules, module_name, previous)
        return original_spec_from_file_location(module_name, location)

    monkeypatch.setattr(
        importlib.util,
        "spec_from_file_location",
        capture_and_preload,
    )

    with pytest.raises(RuntimeError, match="broken migration"):
        MigrationFileManager(tmp_path).load_migration_class(path)

    assert captured_name is not None
    assert sys.modules[captured_name] is previous
