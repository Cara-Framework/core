"""``Loader.get_modules`` names a module from the base path, not from cwd.

The import name was ``os.path.relpath(directory)``: the same file imported as
``<pkg>.settings`` from the deployable, as ``<deployable>.<pkg>.settings``
from its parent and failed (``..<pkg>``) from a subdirectory.
"""

from __future__ import annotations

import sys

import pytest

from cara.environment import PathManager
from cara.loader.Loader import Loader

PACKAGE = "cara_loader_probe_config"


@pytest.fixture
def deployable(tmp_path, monkeypatch):
    root = tmp_path / "deployable"
    package = root / PACKAGE
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "settings.py").write_text("VALUE = 1\n")
    (root / "sub").mkdir()
    monkeypatch.setattr(PathManager, "_base_path", str(root))
    monkeypatch.syspath_prepend(str(root))
    yield root
    for name in [n for n in sys.modules if n.startswith(PACKAGE)]:
        del sys.modules[name]


@pytest.mark.parametrize("where", ["root", "parent", "sub"])
def test_the_import_name_does_not_depend_on_cwd(deployable, monkeypatch, where):
    cwd = {
        "root": deployable,
        "parent": deployable.parent,
        "sub": deployable / "sub",
    }[where]
    monkeypatch.chdir(cwd)

    modules = Loader().get_modules(str(deployable / PACKAGE), raise_exception=True)

    assert modules["settings"].__name__ == f"{PACKAGE}.settings"
    assert modules["settings"].VALUE == 1


def test_a_directory_outside_the_base_path_keeps_its_cwd_relative_name(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(PathManager, "_base_path", str(tmp_path / "base"))
    monkeypatch.chdir(tmp_path)

    assert Loader._import_name(str(tmp_path / "other" / "pkg"), "mod") == "other.pkg.mod"
