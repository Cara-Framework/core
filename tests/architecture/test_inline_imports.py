"""InlineImports: every function-local import carries a legal ``# local:`` tag."""

from __future__ import annotations

from cara.architecture.scanners import InlineImports

from ._fixtures import make_manifest, write


def test_untagged_local_import_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "Bad.py",
        "def do_work():\n    import json\n    return json\n",
    )
    findings = InlineImports.scan(manifest)
    assert findings and "without a '# local:" in findings[0].message


def test_heavy_optional_dep_tag_passes(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "Good.py",
        "def do_work():\n"
        "    import playwright  # local: heavy optional dep\n"
        "    return playwright\n",
    )
    assert InlineImports.scan(manifest) == []


def test_cycle_tag_must_name_a_module(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "Cyclic.py",
        "def do_work():\n    import os  # local: cycle with\n    return os\n",
    )
    findings = InlineImports.scan(manifest)
    assert any("names no module" in f.message for f in findings)


def test_cycle_tag_naming_a_module_passes(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "Cyclic.py",
        "def do_work():\n"
        "    import os  # local: cycle with app.services.Other\n"
        "    return os\n",
    )
    assert InlineImports.scan(manifest) == []


def test_envelope_body_tag_outside_envelopes_dir_fails(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "NotAnEnvelope.py",
        "def body():\n    import os  # local: envelope body\n    return os\n",
    )
    findings = InlineImports.scan(manifest)
    assert any("outside a declared envelope directory" in f.message for f in findings)


def test_envelope_body_tag_inside_envelopes_dir_passes(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "commons" / "contracts" / "envelopes" / "SyncEnvelope.py",
        "class SyncEnvelope:\n"
        "    def body(self):\n"
        "        import os  # local: envelope body\n"
        "        return os\n",
    )
    assert InlineImports.scan(manifest) == []


def test_documented_exemption_skips_untagged_import(tmp_path):
    write(
        tmp_path / "app" / "services" / "Legacy.py",
        "def do_work():\n    import json\n    return json\n",
    )
    manifest = make_manifest(
        tmp_path,
        inline_import_exemptions=frozenset({("app/services/Legacy.py", "json")}),
    )
    assert InlineImports.scan(manifest) == []
