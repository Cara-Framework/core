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


def test_exported_symbols_never_collide_with_submodule_names() -> None:
    """A public export whose name is also a submodule name is a live trap.

    Importing that submodule makes Python bind it as a package attribute,
    which shadows a lazy ``__getattr__`` — so ``from cara.http import
    Pagination`` starts returning the MODULE instead of the class, but only
    for callers unlucky enough to import in the wrong order. The failure
    surfaces far from its cause, as ``module has no attribute ...`` or
    ``module.__new__(X): X is not a type object``.

    Colliding names must therefore be bound EAGERLY (see
    ``cara/http/__init__.py``). This resolves each collision for real rather
    than reading the source: it imports the package, then the submodule — the
    order that springs the trap — and proves the exported name still hands
    back the object. Source-scanning an ``_EXPORTS`` literal only ever saw
    lazy barrels, and would miss an eager barrel that someone lazifies later.

    Names a package does NOT export are out of scope: ``cara.commands.core``
    deliberately exports nothing so that one command's optional runtime
    dependencies cannot disable unrelated CLI commands, and its callers are
    expected to import from the defining module.
    """
    import importlib
    import pathlib
    import types

    root = pathlib.Path(__file__).resolve().parents[2] / "cara"
    offenders: list[str] = []
    for init in sorted(root.rglob("__init__.py")):
        directory = init.parent
        if directory == root:
            package_name = "cara"
        else:
            package_name = "cara." + str(
                directory.relative_to(root)
            ).replace("/", ".")
        package = importlib.import_module(package_name)

        exported = set(getattr(package, "__all__", ()) or ())
        submodules = {
            path.stem for path in directory.glob("*.py") if path.stem != "__init__"
        } | {
            path.name
            for path in directory.iterdir()
            if path.is_dir() and (path / "__init__.py").exists()
        }
        for name in sorted(exported & submodules):
            importlib.import_module(f"{package_name}.{name}")
            if isinstance(getattr(package, name, None), types.ModuleType):
                offenders.append(f"{package_name}.{name}")

    assert not offenders, (
        "Exported names are shadowed by their own submodules — bind these "
        "eagerly in the package __init__:\n  " + "\n  ".join(offenders)
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
