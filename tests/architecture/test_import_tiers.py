"""ImportTiers: stdlib -> third-party -> framework/kernel -> app-local."""

from __future__ import annotations

from cara.architecture.scanners import ImportTiers

from ._fixtures import make_manifest, write


def test_catches_third_party_after_app_local(tmp_path):
    """DOCTRINE §5.1 violation: a third-party import placed after an
    app-local one breaks the tier order."""
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "Bad.py",
        "import os\nfrom app.services import Foo\nimport requests\n",
    )
    findings = ImportTiers.scan(manifest)
    assert findings, "expected a tier-order violation to be caught"
    assert any("requests" in f.message for f in findings)


def test_clean_tier_order_passes(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "Good.py",
        "import os\n"
        "import requests\n"
        "from cara.foo import Bar\n"
        "from app.services import Foo\n",
    )
    assert ImportTiers.scan(manifest) == []


def test_relative_imports_are_always_tier_three(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "Ok.py",
        "import os\nfrom cara.foo import Bar\nfrom .Sibling import Sibling\n",
    )
    assert ImportTiers.scan(manifest) == []


def test_closed_third_party_enumeration_rejects_unlisted_before_app_local(tmp_path):
    """When a product enumerates third_party_packages, an UNRECOGNISED
    third-party import is tier 4 ("accepted only at the end") — placing it
    BEFORE an app-local import is a violation even though a bare catch-all
    third-party tier would have accepted it silently."""
    manifest = make_manifest(tmp_path, third_party_packages=frozenset({"requests"}))
    write(
        tmp_path / "app" / "Weird.py",
        "import unlisted_pkg\nfrom app.services import Foo\n",
    )
    findings = ImportTiers.scan(manifest)
    assert findings and any(
        "tier 3" in f.message and "app.services" in f.message for f in findings
    )


def test_closed_third_party_enumeration_accepts_unlisted_at_the_tail(tmp_path):
    manifest = make_manifest(tmp_path, third_party_packages=frozenset({"requests"}))
    write(
        tmp_path / "app" / "WeirdButFine.py",
        "from app.services import Foo\nimport unlisted_pkg\n",
    )
    assert ImportTiers.scan(manifest) == []
