"""The Guard Pack, pointed at cara (DOCTRINE §11).

Every other module in this package scans a synthetic ``tmp_path`` tree. This
one scans the repository it lives in. See ``_cara_self_scan`` for the
manifest, the include/exclude reasoning, and the regeneration recipe;
``_cara_census`` holds the dated, shrink-only pins.
"""

from __future__ import annotations

from cara.architecture._ratchet import _ratchet
from cara.architecture.scanners import REGISTRY
from tests.architecture._cara_census import CENSUS
from tests.architecture._cara_self_scan import (
    NOT_APPLICABLE,
    SELF_SCANNED,
    cara_manifest,
    counts,
    scan,
)

#: Total pinned findings. Zero is intentional: cara must obey the Guard Pack
#: it publishes without carrying a framework-local allowlist.
CENSUS_TOTAL = 0


class TestEveryScannerIsDecidedOn:
    """No scanner may be silently skipped for the framework's own tree."""

    def test_the_two_lists_exhaust_the_registry(self):
        # The blind spot this whole module exists to close began as an
        # omission nobody had to justify. Adding a scanner to the framework
        # now forces an explicit decision: run it against cara, or write down
        # why the question does not apply.
        decided = set(SELF_SCANNED) | set(NOT_APPLICABLE)
        assert decided == set(REGISTRY), (
            f"undecided scanners: {sorted(set(REGISTRY) - decided)}; "
            f"unknown ids: {sorted(decided - set(REGISTRY))}"
        )

    def test_no_scanner_is_both_run_and_excused(self):
        assert not set(SELF_SCANNED) & set(NOT_APPLICABLE)

    def test_every_exclusion_carries_a_reason(self):
        assert all(reason.strip() for reason in NOT_APPLICABLE.values())


class TestTheManifestDescribesCaraAndNotAProduct:
    """The manifest must be genuinely cara-rooted, or the scan is vacuous."""

    def test_the_app_namespace_is_cara(self):
        # If this ever reads "app" again, ImportForm and SourceShape are back
        # to matching nothing and reporting a clean pass over unjudged code.
        roots = cara_manifest().roots
        assert roots.app_namespace == "cara"
        assert roots.app_path_prefix == "cara"

    def test_the_layers_are_caras_own_packages(self):
        layers = cara_manifest().layers
        assert {"eloquent", "queues", "middleware", "architecture"} <= set(layers)

    def test_the_deployable_root_is_cara(self):
        roots = cara_manifest().roots
        assert roots.app.parent == roots.deployable
        assert roots.deployable.name == "cara"


class TestCaraOwesExactlyItsPinnedDebt:
    """Each self-scanned rule is at, and only at, its pinned count."""

    def _ratchet(self, scanner_id: str):
        return _ratchet(
            key=f"cara self-scan {scanner_id}",
            current=counts(scan(scanner_id)),
            pinned=CENSUS[scanner_id],
            message=f"unpinned {scanner_id} violation in cara",
        )

    def test_import_tiers_is_clean(self):
        # The one rule cara already obeys outright: no pinned debt at all.
        assert CENSUS["import_tiers"] == {}
        assert self._ratchet("import_tiers") == []

    def test_collaborator_calls_is_clean(self):
        """Every ``self.<attr>.<method>()`` matches the declared attribute type.

        The five findings this scanner used to report were one mismatch:
        ``ResponseFactory`` declared its collaborator as ``BaseResponse`` and
        called ``header()`` / ``with_headers()``, which only ``Response``
        defines. It never crashed because both construction sites happen to
        pass ``self`` — a type annotation the code was free to disagree with.
        """
        assert CENSUS["collaborator_calls"] == {}
        assert self._ratchet("collaborator_calls") == []

    def test_barrel_mid_load_is_clean(self):
        """No module reaches through the barrel of its own package (§5.1).

        During a barrel's ``__init__`` the package is only partially
        initialized, so a sibling importing through it binds a half-built
        module — a boot-order crash that appears only in the import order a
        particular entry point happens to produce. Siblings import by direct
        submodule path.
        """
        assert CENSUS["barrel_mid_load"] == {}
        assert self._ratchet("barrel_mid_load") == []

    def test_inline_imports_matches_the_census(self):
        assert self._ratchet("inline_imports") == []

    def test_import_form_matches_the_census(self):
        assert self._ratchet("import_form") == []

    def test_source_shape_matches_the_census(self):
        assert self._ratchet("source_shape") == []

    def test_barrel_completeness_matches_the_census(self):
        assert self._ratchet("barrel_completeness") == []

    def test_every_pinned_scanner_is_actually_scanned(self):
        assert set(CENSUS) == set(SELF_SCANNED)


class TestTheDebtIsVisibleAndShrinkOnly:
    """§11: cara carries no pinned debt against its own guards."""

    def test_the_total_is_pinned(self):
        total = sum(sum(paths.values()) for paths in CENSUS.values())
        assert total == CENSUS_TOTAL, (
            f"cara's self-scan debt is now {total}; expected zero."
        )

    def test_every_pinned_count_is_positive(self):
        # A zero pin is a stale pin wearing a disguise: the ratchet would
        # never surface it, because a file with no findings is absent from
        # `current` and a zero would silently "match" nothing.
        assert all(count > 0 for paths in CENSUS.values() for count in paths.values())
