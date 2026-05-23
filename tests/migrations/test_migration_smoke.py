"""Smoke test for the production migration set under
``commons/database/migrations``.

These tests do NOT run the DDL — they only verify that every
migration file:

* parses cleanly (no SyntaxError, no top-level ImportError),
* defines exactly one ``Migration`` subclass, and
* exposes callable ``up()`` and ``down()`` methods.

This is the cheapest way to detect the regressions that would
otherwise only surface mid-sweep against a real database. A broken
migration sitting on disk silently passes type-check and unit tests
but explodes the moment ``run_pending_migrations`` reaches it.

If commons/database/migrations is absent (some build contexts ship
the cara package without the consumer's migration directory), the
tests skip cleanly rather than fail.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

# Resolve commons/database/migrations relative to this test file.
# tests/ → cara/ → commons/ → cara/.. → commons/database/migrations
_MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[3] / "database" / "migrations"
)


def _migration_files() -> list[Path]:
    if not _MIGRATIONS_DIR.is_dir():
        return []
    return sorted(
        p for p in _MIGRATIONS_DIR.glob("*.py") if p.name != "__init__.py"
    )


_FILES = _migration_files()

pytestmark = pytest.mark.skipif(
    not _FILES,
    reason=(
        "No commons/database/migrations directory present in this build "
        "context; smoke test only runs in the full monorepo checkout."
    ),
)


def _load_module(path: Path):
    # Each migration module name must be unique-per-test to avoid
    # cross-test cache hits (importlib treats same name as same module
    # and returns the cached one).
    mod_name = f"_migration_smoke_{path.stem}"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(mod_name, None)
        raise
    return module


@pytest.mark.parametrize("path", _FILES, ids=lambda p: p.name)
def test_migration_file_imports_cleanly(path):
    """A migration that won't import is a migration that won't run.
    Catches syntax errors, missing imports, broken decorators."""
    _load_module(path)


@pytest.mark.parametrize("path", _FILES, ids=lambda p: p.name)
def test_migration_file_defines_migration_subclass_with_up_and_down(path):
    """Every migration must expose exactly one Migration subclass with
    callable ``up`` and ``down``. A missing ``down`` quietly breaks
    rollback months later when someone runs ``cara migrate:rollback``."""
    from cara.eloquent.migrations import Migration

    module = _load_module(path)
    migration_classes = [
        obj
        for obj in vars(module).values()
        if isinstance(obj, type)
        and issubclass(obj, Migration)
        and obj is not Migration
    ]
    assert migration_classes, (
        f"{path.name} defines no Migration subclass"
    )
    assert len(migration_classes) == 1, (
        f"{path.name} defines {len(migration_classes)} Migration subclasses; "
        "the executor's discovery assumes exactly one per file"
    )
    cls = migration_classes[0]
    assert callable(getattr(cls, "up", None)), (
        f"{path.name}::{cls.__name__}.up is missing or not callable"
    )
    assert callable(getattr(cls, "down", None)), (
        f"{path.name}::{cls.__name__}.down is missing or not callable"
    )
