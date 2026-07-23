"""ArchCheckCommand / ArchBarrelsCommand: boot-free craft commands.

Mirrors ``tests/commands/test_vendor_commons_scan.py``: instantiate the
command directly with ``application=None`` (no container, no bootstrap) and
call ``handle()`` — proving the command needs no app config/secrets/DB.
"""

from __future__ import annotations

from cara.commands.core.ArchBarrelsCommand import ArchBarrelsCommand
from cara.commands.core.ArchCheckCommand import ArchCheckCommand

from ._fixtures import write


def _write_manifest(tmp_path, **fields) -> str:
    extra = "".join(f"    {k}={v!r},\n" for k, v in fields.items())
    manifest_file = write(
        tmp_path / "app" / "architecture_manifest.py",
        "from pathlib import Path\n\n"
        "from cara.architecture.Manifest import Manifest, ManifestRoots\n\n"
        "ROOT = Path(__file__).resolve().parents[1]\n\n"
        "MANIFEST = Manifest(\n"
        "    product='acme',\n"
        "    deployable='api',\n"
        "    roots=ManifestRoots(deployable=ROOT, app=ROOT / 'app'),\n"
        "    layers=('services',),\n"
        "    domain_layers=('services',),\n"
        "    domains={'catalog': 'Catalog domain.'},\n"
        "    scan_plugin_string_literals=True,\n"
        "    kernel_barrel_packages=frozenset(),\n"
        "    seam_kernel_packages=frozenset(),\n"
        f"{extra}"
        ")\n",
    )
    return str(manifest_file)


def test_arch_check_clean_tree_exits_zero(tmp_path):
    manifest_path = _write_manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "catalog" / "Thing.py", "class Thing:\n    pass\n"
    )
    write(
        tmp_path / "app" / "services" / "catalog" / "__init__.py",
        '"""Catalog."""\n\nfrom .Thing import Thing\n\n__all__ = [\n    "Thing",\n]\n',
    )
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\n__all__: list[str] = []\n',
    )
    code = ArchCheckCommand(application=None).handle(
        manifest=manifest_path, scanner="domain_registry"
    )
    assert code == 0


def test_arch_check_dirty_tree_exits_nonzero(tmp_path):
    manifest_path = _write_manifest(tmp_path)
    # An undeclared domain folder — domain_registry violation.
    write(
        tmp_path / "app" / "services" / "rogue" / "Thing.py", "class Thing:\n    pass\n"
    )
    code = ArchCheckCommand(application=None).handle(
        manifest=manifest_path, scanner="domain_registry"
    )
    assert code == 1


def test_arch_check_unknown_scanner_exits_nonzero(tmp_path):
    manifest_path = _write_manifest(tmp_path)
    code = ArchCheckCommand(application=None).handle(
        manifest=manifest_path, scanner="not_a_real_scanner"
    )
    assert code == 1


def test_arch_check_missing_manifest_exits_nonzero(tmp_path):
    code = ArchCheckCommand(application=None).handle(manifest=str(tmp_path / "nope.py"))
    assert code == 1


def test_arch_barrels_check_reports_drift_and_does_not_write(tmp_path):
    manifest_path = _write_manifest(tmp_path)
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    cmd = ArchBarrelsCommand(application=None)
    cmd.set_parsed_options({"check": True})
    code = cmd.handle(manifest=manifest_path)
    assert code == 1
    assert not (tmp_path / "app" / "services" / "__init__.py").exists()


def test_arch_barrels_write_regenerates_and_second_run_is_clean(tmp_path):
    manifest_path = _write_manifest(tmp_path)
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    cmd = ArchBarrelsCommand(application=None)
    cmd.set_parsed_options({"write": True})
    assert cmd.handle(manifest=manifest_path) == 0
    assert (tmp_path / "app" / "services" / "__init__.py").exists()

    check_cmd = ArchBarrelsCommand(application=None)
    check_cmd.set_parsed_options({"check": True})
    assert check_cmd.handle(manifest=manifest_path) == 0


def test_arch_barrels_rejects_check_and_write_together(tmp_path):
    manifest_path = _write_manifest(tmp_path)
    cmd = ArchBarrelsCommand(application=None)
    cmd.set_parsed_options({"check": True, "write": True})
    assert cmd.handle(manifest=manifest_path) == 1
