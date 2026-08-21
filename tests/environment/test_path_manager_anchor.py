"""Gate: an unconfigured process may never anchor onto the workspace root.

``PathManager.base_path()`` fell back to ``os.getcwd()``, so every derived
path was a function of where the operator was standing. Running anything from
the WORKSPACE ROOT — which DOCTRINE §1 enumerates exhaustively as
``api/ services/ commons/ <frontend>/ docs/ infrastructure/`` — created
``database/migrations/`` and ``storage/logs/`` there instead. Both appeared for
real in one product: an orphaned migration-generation lock, and a month of API
stack traces written outside every deployable, outside every ``.gitignore``
(the workspace root is not a git repository) and outside log retention.

The anchor is the nearest ancestor owning ``bootstrap.py``. The workspace root
owns none — it HOLDS them — so it fails loudly rather than being written into.
"""

from __future__ import annotations

import pytest

from cara.environment import PathManager


@pytest.fixture(autouse=True)
def _unconfigured_path_manager():
    """Run against a PathManager no bootstrap has anchored."""
    base, anchors = PathManager._base_path, PathManager._anchors
    PathManager._base_path = None
    PathManager._anchors = {}
    yield
    PathManager._base_path, PathManager._anchors = base, anchors


def _workspace(tmp_path):
    root = tmp_path / "product.example" / "code"
    for deployable in ("api", "services"):
        (root / deployable).mkdir(parents=True)
        (root / deployable / "bootstrap.py").write_text("", encoding="utf-8")
    (root / "docs").mkdir()
    return root


def test_a_deployable_subdirectory_anchors_to_the_deployable(tmp_path, monkeypatch):
    """A suite run from ``api/tests`` still writes under ``api/``."""
    root = _workspace(tmp_path)
    tests = root / "api" / "tests"
    tests.mkdir()
    monkeypatch.chdir(tests)

    assert PathManager.base_path() == str(root / "api")
    assert PathManager.migrations_path() == str(root / "api" / "database" / "migrations")


def test_the_workspace_root_is_refused_rather_than_written_into(tmp_path, monkeypatch):
    """Standing where the deployables live is misuse, not a base path."""
    root = _workspace(tmp_path)
    monkeypatch.chdir(root)

    with pytest.raises(RuntimeError, match="workspace root"):
        PathManager.base_path()

    assert not (root / "database").exists()
    assert not (root / "storage").exists()


def test_an_explicit_base_path_always_wins(tmp_path, monkeypatch):
    """A bootstrapped process is unaffected: it anchors itself."""
    root = _workspace(tmp_path)
    monkeypatch.chdir(root)
    PathManager.set_base_path(str(root / "services"))

    assert PathManager.storage_path("logs") == str(root / "services" / "storage" / "logs")


def test_an_ordinary_directory_still_falls_back_to_cwd(tmp_path, monkeypatch):
    """Fixture trees and installed packages keep the old behaviour."""
    plain = tmp_path / "somewhere"
    plain.mkdir()
    monkeypatch.chdir(plain)

    assert PathManager.base_path() == str(plain)
