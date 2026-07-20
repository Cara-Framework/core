"""HTTP client imports must not boot server-only body parsing."""

from __future__ import annotations

import subprocess
import sys


def test_http_client_import_does_not_load_request_stack() -> None:
    code = """
import sys
from cara.http.client.HttpClient import HttpFacade
assert HttpFacade is not None
assert 'cara.http.request.Request' not in sys.modules
assert 'cara.http.request.mixins.MakesBodyParsing' not in sys.modules
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        check=False,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_lazy_barrel_symbols_never_collide_with_submodule_names() -> None:
    """A lazy export whose name is also a submodule name is a live trap.

    Importing that submodule makes Python bind it as a package attribute,
    which shadows ``__getattr__`` — so ``from cara.http import Pagination``
    starts returning the MODULE instead of the class, but only for callers
    unlucky enough to import in the wrong order. Colliding names must be
    bound eagerly (see ``cara/http/__init__.py``), and this guard proves it
    across every lazy barrel in the framework.
    """
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[2] / "cara"
    offenders: list[str] = []
    for init in root.rglob("__init__.py"):
        source = init.read_text()
        if "_EXPORTS = {" not in source:
            continue
        lazy = set(re.findall(r'^\s{4}"([^"]+)":', source, re.M))
        package = init.parent
        submodules = {
            path.stem for path in package.glob("*.py") if path.stem != "__init__"
        } | {
            path.name
            for path in package.iterdir()
            if path.is_dir() and (path / "__init__.py").exists()
        }
        collisions = sorted(lazy & submodules)
        if collisions:
            offenders.append(f"{init.relative_to(root.parent)}: {collisions}")

    assert not offenders, (
        "Lazy exports collide with submodule names — bind these eagerly:\n  "
        + "\n  ".join(offenders)
    )


def test_pagination_stays_the_class_even_after_the_submodule_is_imported() -> None:
    code = """
import cara.http.Pagination  # binds the submodule onto the package
from cara.http import Pagination

assert isinstance(Pagination, type), type(Pagination)
assert hasattr(Pagination, "from_validated")
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        check=False,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
