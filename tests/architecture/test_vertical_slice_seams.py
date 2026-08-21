"""VerticalSliceSeams: plugin tokens confined to the Four Legal Seams (DOCTRINE §4)."""

from __future__ import annotations

from cara.architecture.scanners import VerticalSliceSeams
from cara.architecture.SeamLocations import SeamLocations

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


def test_product_cannot_disable_string_literal_scanning(tmp_path):
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=TOKENS,
        scan_plugin_string_literals=False,
    )
    write(
        tmp_path / "app" / "services" / "Check.py",
        "def check(slug):\n    return slug == 'ebay'\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert len(findings) == 1
    assert "scan_plugin_string_literals must be true" in findings[0].message


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


def test_evasion_via_container_element_literal_is_caught(tmp_path):
    """The fifth position: a brand token as a bare list/tuple element —
    a ``where_in`` list or capability tuple sat structurally invisible
    to the other four positions. Prose elements mentioning a brand stay
    legal: a description is a word, not a branch."""
    manifest = make_manifest(tmp_path, plugin_tokens=TOKENS)
    write(
        tmp_path / "app" / "services" / "Funnel.py",
        "LANES = ['ebay', 'other']\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert findings and "container-literal" in findings[0].message

    write(
        tmp_path / "app" / "services" / "Funnel.py",
        "STEPS = ['sync the eBay catalog once']\n",
    )
    assert VerticalSliceSeams.scan(manifest) == []


def test_export_manifests_are_exempt_container_positions(tmp_path):
    """``__all__`` and a generated barrel's ``_LAZY_EXPORTS`` tuples restate
    module names the identifier scan already counts in the modules
    themselves — the container position must not double-charge them."""
    manifest = make_manifest(tmp_path, plugin_tokens=TOKENS)
    write(
        tmp_path / "app" / "jobs" / "__init__.py",
        "_LAZY_EXPORTS = {\n"
        '    "PullEbayJob": (".PullEbayJob", "PullEbayJob"),\n'
        "}\n"
        "__all__ = [\n"
        '    "PullEbayJob",\n'
        "]\n",
    )
    assert VerticalSliceSeams.scan(manifest) == []

    # The same token in an ORDINARY container in the same file still hits.
    write(
        tmp_path / "app" / "jobs" / "__init__.py",
        "__all__ = [\"PullEbayJob\"]\nLANES = ['ebay']\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert findings and "container-literal" in findings[0].message


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
        seam_locations=SeamLocations(
            composition_roots=frozenset({"config/providers.py"})
        ),
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


def test_architecture_manifest_is_implicit_manifest_data(tmp_path):
    manifest = make_manifest(tmp_path, plugin_tokens=TOKENS)
    write(
        tmp_path / "app" / "architecture_manifest.py",
        "PLUGIN_TOKENS = {'amazon': 'packages/amazon'}\n",
    )
    assert VerticalSliceSeams.scan(manifest) == []


def test_owned_non_marketplace_integration_lane_allows_only_its_provider(tmp_path):
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=frozenset({"google", "amazon"}),
        seam_locations=SeamLocations(
            owned_integration_prefixes={
                "discovery/google_shopping": frozenset({"google"})
            }
        ),
    )
    from dataclasses import replace

    discovery = tmp_path / "discovery"
    manifest = replace(
        manifest,
        roots=replace(
            manifest.roots,
            scanner_roots={
                **manifest.roots.scanner_roots,
                "vertical_slice_seams": (
                    *manifest.roots.scan_dirs("vertical_slice_seams"),
                    discovery,
                ),
            },
        ),
    )
    write(
        discovery / "google_shopping" / "GoogleShoppingDiscovery.py",
        "class GoogleShoppingDiscovery:\n    pass\n",
    )
    assert VerticalSliceSeams.scan(manifest) == []

    write(
        discovery / "price_feeds" / "GooglePriceFeed.py",
        "class GooglePriceFeed:\n    pass\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert any(
        finding.path == "discovery/price_feeds/GooglePriceFeed.py" for finding in findings
    )

    write(
        discovery / "google_shopping" / "AmazonBridge.py",
        "class AmazonBridge:\n    pass\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert any(
        finding.path == "discovery/google_shopping/AmazonBridge.py"
        for finding in findings
    )


def test_core_import_of_owned_integration_provider_is_still_a_leak(tmp_path):
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=frozenset({"google"}),
        seam_locations=SeamLocations(
            owned_integration_prefixes={
                "discovery/google_shopping": frozenset({"google"})
            }
        ),
    )
    write(
        tmp_path / "app" / "jobs" / "Discover.py",
        "from discovery.google_shopping import GoogleShoppingDiscovery\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert any(finding.path == "app/jobs/Discover.py" for finding in findings)


def test_product_owned_extra_core_tree_is_scanned(tmp_path):
    from dataclasses import replace

    manifest = make_manifest(tmp_path, plugin_tokens=TOKENS)
    extra = tmp_path / "discovery"
    manifest = replace(
        manifest,
        roots=replace(
            manifest.roots,
            scanner_roots={
                **manifest.roots.scanner_roots,
                "vertical_slice_seams": (
                    *manifest.roots.scan_dirs("vertical_slice_seams"),
                    extra,
                ),
            },
        ),
    )
    write(extra / "EbayDiscovery.py", "class EbayDiscovery:\n    pass\n")
    findings = VerticalSliceSeams.scan(manifest)
    assert any(finding.path == "discovery/EbayDiscovery.py" for finding in findings)


def test_kernel_can_be_scanned_by_only_one_deployable_twin(tmp_path):
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=TOKENS,
        seam_kernel_packages=frozenset(),
    )
    write(
        tmp_path / "commons" / "models" / "EbayModel.py",
        "class EbayModel:\n    pass\n",
    )
    assert VerticalSliceSeams.scan(manifest) == []


def test_symlinked_kernel_keeps_deployable_relative_path(tmp_path):
    from dataclasses import replace

    shared_kernel = tmp_path / "shared-kernel"
    write(
        shared_kernel / "models" / "EbayModel.py",
        "class EbayModel:\n    pass\n",
    )
    deployable = tmp_path / "api"
    (deployable / "app").mkdir(parents=True)
    (deployable / "commons").symlink_to(shared_kernel, target_is_directory=True)
    manifest = make_manifest(tmp_path, plugin_tokens=TOKENS)
    manifest = replace(
        manifest,
        roots=replace(
            manifest.roots,
            deployable=deployable,
            app=deployable / "app",
            scanner_roots={
                **manifest.roots.scanner_roots,
                "vertical_slice_seams": (deployable / "app",),
            },
            kernel={"models": deployable / "commons" / "models"},
        ),
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert findings[0].path == "commons/models/EbayModel.py"


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


def test_data_vocabulary_seam_exempts_the_barrel_re_export_of_a_slug(tmp_path):
    """A kernel barrel inside the seam re-exports the vocabulary constants
    under the mandated ``app.*`` runtime name (§2). That is ONE declaration
    reaching its second legal spelling mechanically, not a second appearance
    of the brand — the generated barrel has no other shape it could take."""
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=TOKENS,
        seam_locations=SeamLocations(
            data_vocabulary_prefixes=("commons/models/", "app/models/")
        ),
    )
    write(
        tmp_path / "commons" / "models" / "Channel.py",
        "MARKETPLACE_EBAY = 'ebay'\n",
    )
    write(
        tmp_path / "app" / "models" / "__init__.py",
        "from commons.models import MARKETPLACE_EBAY\n",
    )
    assert VerticalSliceSeams.scan(manifest) == []

    # The exemption is the CONSTANT's name, never the module path: importing
    # a brand-carrying module is still core code reaching for a plug-in.
    write(
        tmp_path / "app" / "models" / "__init__.py",
        "from commons.models.EbayChannel import MARKETPLACE_EBAY\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert findings and "import" in findings[0].message


def test_data_vocabulary_seam_exempts_declaration_positions_only(tmp_path):
    """Within the vocabulary seam, container/dict-key literals ARE the
    vocabulary (column keys, jsonb specs). Branching positions — a compare,
    a call argument — stay flagged even there."""
    manifest = make_manifest(
        tmp_path,
        plugin_tokens=TOKENS,
        seam_locations=SeamLocations(data_vocabulary_prefixes=("commons/models/",)),
    )
    write(
        tmp_path / "commons" / "models" / "Listing.py",
        "class Listing:\n"
        "    __fillable__ = ['ebay_item_id', 'is_amazon_prime']\n"
        "    SPECS = {'ebay_item_id': 'str'}\n",
    )
    assert VerticalSliceSeams.scan(manifest) == []

    write(
        tmp_path / "commons" / "models" / "Listing.py",
        "class Listing:\n"
        "    def is_ebay(self):\n"
        "        return self.marketplace == 'ebay'\n",
    )
    findings = VerticalSliceSeams.scan(manifest)
    assert findings and "compare-literal" in findings[0].message


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


def test_token_matching_is_segment_bounded_not_substring(tmp_path):
    """``lowes`` must not report itself inside ``lowest_price_30d``.

    Blind substring matching is why a product with 199 retailer packages could
    declare only a handful of tokens: turning the rest on drowned the guard in
    English words. The boundary rule is what makes a real token set usable.
    """
    from cara.architecture.scanners.VerticalSliceSeams import _token_re

    manifest = make_manifest(tmp_path, plugin_tokens=frozenset({"lowes", "on", "bose"}))
    token_re = _token_re(manifest)

    for benign in ("lowest_price_30d", "lowest", "slowest", "json", "version",
                   "connection", "comparison", "verbose", "season"):
        assert not token_re.search(benign), f"false positive on {benign!r}"

    for leak in ("lowes", "lowes.com", "on_delete", "BOSE_SPEAKER"):
        assert token_re.search(leak), f"missed leak in {leak!r}"


def test_token_matching_keeps_camel_case_and_fused_host_leaks(tmp_path):
    """Boundaries must not cost the leaks the substring form did catch."""
    from cara.architecture.scanners.VerticalSliceSeams import _token_re

    manifest = make_manifest(
        tmp_path, plugin_tokens=frozenset({"amazon", "ebay", "shopify"})
    )
    token_re = _token_re(manifest)

    for leak in ("AmazonListingExtractor", "PullEbayJob", "MARKETPLACE_EBAY",
                 "amazon_id", "cdn.shopify.com", "shopify_product_id",
                 "myshopify.com", "Amazon2"):
        assert token_re.search(leak), f"missed leak in {leak!r}"

    assert not token_re.search("amazonian")
