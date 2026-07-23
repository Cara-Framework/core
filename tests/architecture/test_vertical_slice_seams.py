"""VerticalSliceSeams: plugin tokens confined to the Four Legal Seams (DOCTRINE §4)."""

from __future__ import annotations

from cara.architecture.Manifest import SeamLocations
from cara.architecture.scanners import VerticalSliceSeams

from ._fixtures import make_manifest, write

TOKENS = frozenset({"ebay", "amazon"})


def test_identifier_hit_outside_seams_is_a_leak(tmp_path):
    manifest = make_manifest(tmp_path, plugin_tokens=TOKENS)
    write(
        tmp_path / "app" / "services" / "EbayThing.py", "class EbayConnector:\n    pass\n"
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert findings and "outside the Four Legal Seams" in findings[0].message


def test_evasion_via_compare_literal_is_caught(tmp_path):
    """A bare string literal in a Compare dodges the identifier scan the
    same branch on a real constant would not — the scanner must still
    catch it."""
    manifest = make_manifest(tmp_path, plugin_tokens=TOKENS)
    write(
        tmp_path / "app" / "services" / "Check.py",
        "def is_ebay(slug):\n    if slug == 'ebay':\n        return True\n    return False\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert findings and "compare-literal" in findings[0].message


def test_evasion_via_dict_key_literal_is_caught(tmp_path):
    manifest = make_manifest(tmp_path, plugin_tokens=TOKENS)
    write(tmp_path / "app" / "services" / "Table.py", "LANES = {\n    'ebay': 1,\n}\n")
    findings = VerticalSliceSeams.scan(manifest)
    assert findings and "dict-key-literal" in findings[0].message


def test_evasion_via_call_arg_literal_is_caught(tmp_path):
    manifest = make_manifest(tmp_path, plugin_tokens=TOKENS)
    write(
        tmp_path / "app" / "services" / "Dispatch.py",
        "def run():\n    dispatch('ebay')\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert findings and "call-arg-literal" in findings[0].message


def test_evasion_via_default_value_literal_is_caught(tmp_path):
    manifest = make_manifest(tmp_path, plugin_tokens=TOKENS)
    write(
        tmp_path / "app" / "services" / "Defaults.py",
        "def handler(marketplace='ebay'):\n    return marketplace\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert findings and "default-literal" in findings[0].message


def test_prose_and_docstrings_are_never_flagged(tmp_path):
    manifest = make_manifest(tmp_path, plugin_tokens=TOKENS)
    write(
        tmp_path / "app" / "services" / "Clean.py",
        '"""Talks about ebay in prose — never a hit."""\n\n'
        "# a comment about amazon — never a hit\n"
        "def helper():\n"
        "    return 1\n",
    )
    assert VerticalSliceSeams.scan(manifest) == []


def test_composition_root_seam_is_exempt(tmp_path):
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=TOKENS,
        seam_locations=SeamLocations(composition_root="config/providers.py"),
    )
    write(
        tmp_path / "config" / "providers.py",
        "from packages.ebay.Connector import Connector\n",
    )
    assert VerticalSliceSeams.scan(manifest) == []


def test_manifest_data_seam_is_exempt(tmp_path):
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=TOKENS,
        seam_locations=SeamLocations(
            manifest_files=frozenset({"commons/shared/Marketplaces.py"})
        ),
    )
    write(
        tmp_path / "commons" / "shared" / "Marketplaces.py",
        "class EbayMarketplace:\n    pass\n",
    )
    assert VerticalSliceSeams.scan(manifest) == []


def test_data_vocabulary_seam_exempts_upper_snake_slug_constants(tmp_path):
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=TOKENS,
        seam_locations=SeamLocations(data_vocabulary_prefixes=("commons/models/",)),
    )
    write(
        tmp_path / "commons" / "models" / "Channel.py",
        "class Channel:\n    MARKETPLACE_EBAY = 'ebay'\n",
    )
    assert VerticalSliceSeams.scan(manifest) == []


def test_sunset_debt_within_pin_passes(tmp_path):
    # EbayThing.py hits twice: the module-path itself, and the class name.
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=TOKENS,
        seam_allowlists={"vertical_slice_seams": {"app/services/EbayThing.py": 2}},
    )
    write(
        tmp_path / "app" / "services" / "EbayThing.py", "class EbayConnector:\n    pass\n"
    )
    assert VerticalSliceSeams.scan(manifest) == []


def test_sunset_debt_growth_is_a_finding(tmp_path):
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=TOKENS,
        seam_allowlists={"vertical_slice_seams": {"app/services/EbayThing.py": 2}},
    )
    write(
        tmp_path / "app" / "services" / "EbayThing.py",
        "class EbayConnector:\n    pass\n\n\nclass EbayOther:\n    pass\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert any("shrink-only" in f.message for f in findings)


def test_sunset_debt_stale_pin_is_a_finding(tmp_path):
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=TOKENS,
        seam_allowlists={"vertical_slice_seams": {"app/services/EbayThing.py": 2}},
    )
    write(tmp_path / "app" / "services" / "EbayThing.py", "class Clean:\n    pass\n")
    findings = VerticalSliceSeams.scan(manifest)
    assert any("stale allowlist pin" in f.message for f in findings)


def test_no_plugin_tokens_declared_noops(tmp_path):
    manifest = make_manifest(tmp_path, plugin_tokens=frozenset())
    write(
        tmp_path / "app" / "services" / "EbayThing.py", "class EbayConnector:\n    pass\n"
    )
    assert VerticalSliceSeams.scan(manifest) == []
