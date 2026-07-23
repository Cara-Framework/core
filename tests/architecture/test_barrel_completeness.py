"""BarrelCompleteness: every barrel is a sorted superset of its children."""

from __future__ import annotations

from cara.architecture.scanners import BarrelCompleteness

from ._fixtures import make_manifest, write


def test_missing_dunder_all_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(tmp_path / "app" / "services" / "__init__.py", '"""Layer."""\n')
    findings = BarrelCompleteness.scan(manifest)
    assert any("no __all__ declared" in f.message for f in findings)


def test_incomplete_superset_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(tmp_path / "app" / "services" / "Bar.py", "class Bar:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\nfrom .Foo import Foo\n\n__all__ = [\n    "Foo",\n]\n',
    )
    findings = BarrelCompleteness.scan(manifest)
    assert any("Bar" in f.message and "missing re-export" in f.message for f in findings)


def test_unsorted_dunder_all_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(tmp_path / "app" / "services" / "Bar.py", "class Bar:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\nfrom .Foo import Foo\nfrom .Bar import Bar\n\n__all__ = [\n    "Foo",\n    "Bar",\n]\n',
    )
    findings = BarrelCompleteness.scan(manifest)
    assert any("not alphabetically sorted" in f.message for f in findings)


def test_complete_sorted_barrel_passes(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(tmp_path / "app" / "services" / "Bar.py", "class Bar:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\nfrom .Bar import Bar\nfrom .Foo import Foo\n\n__all__ = [\n    "Bar",\n    "Foo",\n]\n',
    )
    assert BarrelCompleteness.scan(manifest) == []


def test_module_object_child_is_exempt_from_the_superset(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(
        tmp_path / "app" / "services" / "Text.py",
        "def helper():\n    pass\n\n\nOTHER = 1\n",
    )
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\nfrom . import Text\n\n__all__ = [\n    "Text",\n]\n',
    )
    assert BarrelCompleteness.scan(manifest) == []


def test_kernel_package_completeness_is_checked_too(tmp_path):
    manifest = make_manifest(tmp_path)
    write(tmp_path / "commons" / "models" / "User.py", "class User:\n    pass\n")
    write(
        tmp_path / "commons" / "models" / "__init__.py",
        '"""Models."""\n\n__all__: list[str] = []\n',
    )
    findings = BarrelCompleteness.scan(manifest)
    assert any("User" in f.message for f in findings)
