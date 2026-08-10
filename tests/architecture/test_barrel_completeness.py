"""BarrelCompleteness: every barrel is a sorted superset of its children."""

from __future__ import annotations

from cara.architecture.scanners import BarrelCompleteness

from ._fixtures import make_manifest, write


def test_kernel_barrels_can_be_outside_product_barrel_scope(tmp_path):
    manifest = make_manifest(tmp_path, kernel_barrel_packages=frozenset())
    write(
        tmp_path / "commons" / "shared" / "Helper.py",
        "def helper():\n    return 1\n",
    )
    assert BarrelCompleteness.scan(manifest) == []


def test_declared_but_unbound_export_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '__all__ = ["Ghost"]\n',
    )
    findings = BarrelCompleteness.scan(manifest)
    assert any("never bound" in finding.message for finding in findings)


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


# ── The dev-only bridge barrel: app/<kernel-pkg> over commons/<kernel-pkg> ──
# VendorBarrelParity covers the FLATTENED packages, where a stale barrel
# survives development and dies in the production image. These cover the
# other half: a stale barrel over a verbatim-shipped package dies in
# DEVELOPMENT, at boot, with a traceback that names config loading rather
# than the kernel edit that caused it.


def _kernel_with(tmp_path, pkg: str, *names: str, module_objects: str = "") -> None:
    """A kernel package that both BINDS and exports ``names`` — an unbound
    ``__all__`` entry is a different rule's finding and would mask this one."""
    bindings = "".join(f"{name} = None\n" for name in names if name != "persistence")
    exports = ", ".join(f'"{name}"' for name in sorted(names))
    write(
        tmp_path / "commons" / pkg / "__init__.py",
        f'"""Kernel."""\n\n{module_objects}{bindings}\n__all__ = [{exports}]\n',
    )


def test_bridge_barrel_missing_a_kernel_name_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path)
    _kernel_with(tmp_path, "contracts", "CAP_ADS", "CAP_ORDERS")
    write(
        tmp_path / "app" / "contracts" / "__init__.py",
        '"""Barrel."""\n\nfrom commons.contracts import CAP_ADS\n\n__all__ = ["CAP_ADS"]\n',
    )
    findings = BarrelCompleteness.scan(manifest)
    assert any(
        "CAP_ORDERS" in f.message and "does not re-export" in f.message for f in findings
    )


def test_bridge_barrel_carrying_a_local_member_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path)
    _kernel_with(tmp_path, "contracts", "CAP_ADS")
    write(
        tmp_path / "app" / "contracts" / "__init__.py",
        '"""Barrel."""\n\nfrom commons.contracts import CAP_ADS\n'
        "\n\nclass LocalPort:\n    pass\n\n\n"
        '__all__ = ["CAP_ADS", "LocalPort"]\n',
    )
    findings = BarrelCompleteness.scan(manifest)
    assert any("LocalPort" in f.message and "app/ports" in f.message for f in findings)


def test_kernel_module_object_need_not_be_bridged(tmp_path):
    """``gates/persistence`` is kernel-internal — app trees are forbidden to
    reach it, so its absence from the bridge must not be a finding. The
    exemption is read from the kernel's own module-object binding rather than
    kept as a second list of forbidden names."""
    manifest = make_manifest(tmp_path)
    write(tmp_path / "commons" / "gates" / "persistence" / "__init__.py", "")
    _kernel_with(
        tmp_path,
        "gates",
        "Writer",
        "persistence",
        module_objects="from . import persistence\n",
    )
    write(
        tmp_path / "app" / "gates" / "__init__.py",
        '"""Barrel."""\n\nfrom commons.gates import Writer\n\n__all__ = ["Writer"]\n',
    )
    findings = BarrelCompleteness.scan(manifest)
    assert not [f for f in findings if "gates" in f.path]


def test_a_complete_bridge_barrel_passes(tmp_path):
    manifest = make_manifest(tmp_path)
    _kernel_with(tmp_path, "shared", "normalize_tags")
    write(
        tmp_path / "app" / "shared" / "__init__.py",
        '"""Barrel."""\n\nfrom commons.shared import normalize_tags\n\n'
        '__all__ = ["normalize_tags"]\n',
    )
    assert [f for f in BarrelCompleteness.scan(manifest) if "shared" in f.path] == []


def test_both_twins_check_their_own_bridge_even_when_one_owns_the_kernel_walk(tmp_path):
    """``kernel_barrel_packages`` splits who walks the SHARED kernel tree; the
    bridge is per-deployable and must be checked from either side."""
    manifest = make_manifest(tmp_path, kernel_barrel_packages=frozenset())
    _kernel_with(tmp_path, "contracts", "CAP_ADS")
    write(
        tmp_path / "app" / "contracts" / "__init__.py",
        '"""Barrel."""\n\n__all__: list[str] = []\n',
    )
    findings = BarrelCompleteness.scan(manifest)
    assert any("CAP_ADS" in f.message for f in findings)


def test_a_directory_with_no_barrel_is_a_finding_not_a_crash(tmp_path):
    """GUARDPACK §5 step 9 puts a product in exactly this state mid-change —
    modules written, barrel not regenerated yet. Reading the absent
    ``__init__.py`` aborted the whole pack with a ``FileNotFoundError``, so the
    guard died precisely when a product followed its own documented procedure."""
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")

    findings = BarrelCompleteness.scan(manifest)

    assert any("has no __init__.py" in f.message for f in findings)
    assert all(f.path == "app/services/__init__.py" for f in findings)


def test_an_empty_directory_with_no_barrel_is_silent(tmp_path):
    """Nothing to re-export, nothing to regenerate — a layer a deployable
    simply does not use must not manufacture a finding."""
    manifest = make_manifest(tmp_path, layers=("services",))
    (tmp_path / "app" / "services").mkdir(parents=True)
    assert BarrelCompleteness.scan(manifest) == []


def test_a_non_literal_dunder_all_in_a_package_is_a_finding(tmp_path):
    """A computed ``__all__`` is unreadable to a pure-AST checker. It used to
    be reported as "no __all__ declared", sending readers after a line that was
    right there."""
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\nfrom .Foo import Foo\n\n__all__ = sorted({"Foo"})\n',
    )
    findings = BarrelCompleteness.scan(manifest)
    assert any("non-literal __all__" in f.message for f in findings)


def test_a_kernel_with_a_non_literal_dunder_all_fails_the_bridge_check(tmp_path):
    """The bridge check exists to catch import-time death; on a computed kernel
    ``__all__`` it returned ``[]`` — greener than what it had inspected. §9 says
    fail closed."""
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "commons" / "contracts" / "__init__.py",
        '"""Kernel."""\n\nCAP_ADS = None\n\n__all__ = sorted({"CAP_ADS"})\n',
    )
    write(
        tmp_path / "app" / "contracts" / "__init__.py",
        '"""Barrel."""\n\n__all__: list[str] = []\n',
    )

    findings = BarrelCompleteness.scan(manifest)

    assert any(
        f.path == "commons/contracts/__init__.py"
        and "cannot be verified" in f.message
        and "app.contracts" in f.message
        for f in findings
    )


def test_a_kernel_with_no_dunder_all_at_all_stays_silent(tmp_path):
    """Absent and computed are different failures: a kernel package that
    exports nothing has no bridge to verify."""
    manifest = make_manifest(tmp_path, kernel_barrel_packages=frozenset())
    write(tmp_path / "commons" / "contracts" / "__init__.py", '"""Kernel."""\n')
    assert BarrelCompleteness.scan(manifest) == []
