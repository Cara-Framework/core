"""BarrelMidLoad: no import binds a barrel's half-built submodule (§5.1).

The exemption cases matter as much as the detections here: this rule fires on
a shape that is written thousands of times legitimately, so a copy that got
the pinned-first window or the module-object contract wrong would red a
correct tree.
"""

from __future__ import annotations

from pathlib import Path

from cara.architecture.scanners.BarrelMidLoad import BarrelMidLoad

from ._fixtures import make_manifest, write


def _messages(root: Path, **overrides) -> list[str]:
    return [
        str(finding) for finding in BarrelMidLoad.scan(make_manifest(root, **overrides))
    ]


def _package(root: Path, barrel: str) -> Path:
    package = root / "app" / "services"
    write(package / "__init__.py", barrel)
    write(package / "Report.py", "class Report:\n    pass\n")
    write(package / "Export.py", "class Export:\n    pass\n")
    return package


CLASS_BARREL = "from .Export import Export\nfrom .Report import Report\n"


def test_a_sibling_importing_through_its_own_barrel_is_a_finding(tmp_path: Path) -> None:
    package = _package(tmp_path, CLASS_BARREL)
    write(
        package / "Export.py",
        "from app.services import Report\n\n\nclass Export:\n    pass\n",
    )

    messages = _messages(tmp_path)

    assert any("binds a half-built module" in message for message in messages)
    assert any(
        "from app.services.Report import Report" in message for message in messages
    )


def test_the_direct_submodule_path_is_the_fix(tmp_path: Path) -> None:
    package = _package(tmp_path, CLASS_BARREL)
    write(
        package / "Export.py",
        "from app.services.Report import Report\n\n\nclass Export:\n    pass\n",
    )

    assert _messages(tmp_path) == []


def test_a_relative_sibling_import_resolves_to_the_same_package(tmp_path: Path) -> None:
    package = _package(tmp_path, CLASS_BARREL)
    write(package / "Export.py", "from . import Report\n\n\nclass Export:\n    pass\n")

    # ``from . import Report`` targets the package itself, so it is the same
    # order-fragile shape spelled relatively.
    assert _messages(tmp_path) != []


def test_an_ancestor_barrel_import_is_a_finding_once_the_pin_window_has_closed(
    tmp_path: Path,
) -> None:
    # A subpackage import re-enters the package, so every name bound after it
    # can be observed half-built — including by this subpackage's own modules.
    _package(tmp_path, "from .catalog.Sync import Sync\nfrom .Report import Report\n")
    write(
        tmp_path / "app" / "services" / "catalog" / "Sync.py",
        "from app.services import Report\n\n\nclass Sync:\n    pass\n",
    )

    assert _messages(tmp_path) != []


def test_a_leading_run_of_class_reexports_stays_inside_the_pin_window(
    tmp_path: Path,
) -> None:
    # Consecutive ``from .X import X`` lines do not close the window: each
    # binds its class on the barrel before the next one runs, so a subpackage
    # loaded later always sees a real class. The marketplace packages'
    # ``<Noun>Marketplace``-first inits depend on exactly this.
    _package(tmp_path, CLASS_BARREL)
    write(
        tmp_path / "app" / "services" / "catalog" / "Sync.py",
        "from app.services import Report\n\n\nclass Sync:\n    pass\n",
    )

    assert _messages(tmp_path) == []


def test_an_absolute_self_prefixed_import_also_closes_the_window(
    tmp_path: Path,
) -> None:
    _package(
        tmp_path,
        "from app.services.Export import Export\nfrom .Report import Report\n",
    )
    write(
        tmp_path / "app" / "services" / "catalog" / "Sync.py",
        "from app.services import Report\n\n\nclass Sync:\n    pass\n",
    )

    assert _messages(tmp_path) != []


def test_a_module_object_export_is_not_a_footgun(tmp_path: Path) -> None:
    package = tmp_path / "app" / "services"
    write(package / "__init__.py", "from . import catalog\n")
    write(package / "catalog" / "__init__.py", "")
    write(package / "Export.py", "from app.services import catalog\n")

    # ``from . import catalog`` binds a MODULE on purpose (the module-object
    # contract); there is no class hiding behind the name.
    assert _messages(tmp_path) == []


def test_a_name_that_is_not_a_submodule_is_never_ambiguous(tmp_path: Path) -> None:
    package = tmp_path / "app" / "services"
    write(package / "__init__.py", "from .Report import Report, REPORT_LIMIT\n")
    write(package / "Report.py", "REPORT_LIMIT = 10\n\n\nclass Report:\n    pass\n")
    write(package / "Export.py", "from app.services import REPORT_LIMIT\n")

    assert _messages(tmp_path) == []


def test_an_unrelated_package_import_is_left_alone(tmp_path: Path) -> None:
    _package(tmp_path, CLASS_BARREL)
    write(tmp_path / "app" / "jobs" / "__init__.py", "from .SyncJob import SyncJob\n")
    write(tmp_path / "app" / "jobs" / "SyncJob.py", "class SyncJob:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "Export.py",
        "from app.jobs import SyncJob\n\n\nclass Export:\n    pass\n",
    )

    # ``app.jobs`` is fully loaded before ``app.services`` asks for a name from
    # it — a cross-package barrel import is the SANCTIONED form (§5.1).
    assert _messages(tmp_path) == []
