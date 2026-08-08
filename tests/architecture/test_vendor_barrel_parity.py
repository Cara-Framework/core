"""VendorBarrelParity: the flat-copied kernel package's app barrel must be a
complete superset, because the vendor step folds deep imports onto it (§2)."""

from __future__ import annotations

from cara.architecture.scanners import VendorBarrelParity

from ._fixtures import make_manifest, write

BARREL = (
    '"""Models barrel."""\n\nfrom commons.models import {names}\n\n__all__ = [{quoted}]\n'
)


def _barrel(*names: str) -> str:
    return BARREL.format(
        names=", ".join(names) or "*",
        quoted=", ".join(f'"{name}"' for name in names),
    )


def _kernel(*names: str) -> str:
    exports = ", ".join(f'"{name}"' for name in names)
    return f'"""Kernel package."""\n\n__all__ = [{exports}]\n'


def test_complete_barrel_passes(tmp_path):
    manifest = make_manifest(tmp_path)
    write(tmp_path / "commons" / "models" / "__init__.py", _kernel("User", "normalize"))
    write(tmp_path / "app" / "models" / "__init__.py", _barrel("User", "normalize"))
    assert VendorBarrelParity.scan(manifest) == []


def test_a_name_the_barrel_never_re_exports_is_a_finding(tmp_path):
    """The regression that shipped: a non-class helper re-exported by the
    kernel but absent from the deployable's hand-written barrel."""
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "commons" / "models" / "__init__.py",
        _kernel("User", "normalize_gtin"),
    )
    write(tmp_path / "app" / "models" / "__init__.py", _barrel("User"))
    findings = VendorBarrelParity.scan(manifest)
    assert len(findings) == 1
    assert "normalize_gtin" in findings[0].message
    assert findings[0].path == "app/models/__init__.py"


def test_a_missing_barrel_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path)
    write(tmp_path / "commons" / "models" / "__init__.py", _kernel("User"))
    findings = VendorBarrelParity.scan(manifest)
    assert len(findings) == 1
    assert "vendor swap point" in findings[0].message


def test_verbatim_packages_are_not_checked(tmp_path):
    """``gates`` ships verbatim — the vendor step overwrites its app barrel
    with the kernel's own ``__init__``, so a curated door is legal there."""
    manifest = make_manifest(tmp_path)
    write(tmp_path / "commons" / "models" / "__init__.py", _kernel())
    write(tmp_path / "app" / "models" / "__init__.py", _barrel())
    write(tmp_path / "commons" / "gates" / "__init__.py", _kernel("Writer", "helper"))
    write(tmp_path / "app" / "gates" / "__init__.py", _barrel("Writer"))
    assert VendorBarrelParity.scan(manifest) == []


def test_declaring_gates_flattened_makes_it_checked(tmp_path):
    """The rule follows ``vendor_flattened_packages``, not a hardcoded name."""
    manifest = make_manifest(
        tmp_path, vendor_flattened_packages=frozenset({"models", "gates"})
    )
    write(tmp_path / "commons" / "models" / "__init__.py", _kernel())
    write(tmp_path / "app" / "models" / "__init__.py", _barrel())
    write(tmp_path / "commons" / "gates" / "__init__.py", _kernel("Writer", "helper"))
    write(tmp_path / "app" / "gates" / "__init__.py", _barrel("Writer"))
    findings = VendorBarrelParity.scan(manifest)
    assert len(findings) == 1
    assert "helper" in findings[0].message
