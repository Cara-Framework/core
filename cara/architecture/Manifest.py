"""Manifest: the ONE typed contract a product supplies to the Guard Pack.

DOCTRINE §11: "the pack converges on ONE implementation... products supply
only their manifests (domains, flows, ownership, sunset lists)." Every value
a scanner used to hardcode per-product (root paths, domain names, brand
tokens, sunset allowlists...) is a field here instead. A product wires this
once, at ``app/architecture_manifest.py``, binding a module-level
``MANIFEST: Manifest``; ``Manifest.load()`` reads it with zero app boot —
mirrors how the existing product guards spec-load ``app/domains.py``.

A Manifest is scoped to ONE deployable (api, or services): the two
deployables of a product each get their own ``architecture_manifest.py``,
because their layer names, domain sets and root paths genuinely differ.
Kernel-membership questions that span BOTH deployables (the single-consumer
counter) are answered by pointing ``roots.consumer_app_roots`` at every
sibling deployable's ``app/`` — a tree that isn't checked out is simply
absent from that tuple and the corresponding check no-ops, mirroring the
"whole repo fact, per-service CI" contract the product guards already used.
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ManifestRoots:
    """Filesystem roots one deployable's Guard Pack run needs.

    ``deployable`` is the directory craft runs from (e.g. ``.../api``).
    ``app``/``config`` are expected to exist; ``routes`` (api-shaped
    products) and ``packages`` (plugin-shaped products, DOCTRINE §4) are
    each optional — a deployable declares only the ones it has.

    ``kernel`` maps each dev-only kernel package name (``models`` /
    ``contracts`` / ``gates`` / ``shared`` — see ``Manifest.kernel_packages``)
    to its directory. In a vendored production tree ``commons/`` no longer
    exists (DOCTRINE §2) — an empty dict is legal and kernel-direction
    scanners simply find nothing to walk.
    """

    deployable: Path
    app: Path
    config: Path | None = None
    routes: Path | None = None
    packages: Path | None = None
    kernel: dict[str, Path] = field(default_factory=dict)
    consumer_app_roots: tuple[Path, ...] = ()
    framework_root_name: str = "cara"
    kernel_dev_root_name: str = "commons"
    local_root_names: tuple[str, ...] = ("app", "config", "routes", "packages")

    def scan_dirs(self) -> tuple[Path, ...]:
        """Every top-level tree a consumer-facing scanner should walk."""
        return tuple(
            p
            for p in (self.app, self.config, self.routes, self.packages)
            if p is not None
        )


@dataclass(frozen=True, slots=True)
class SeamLocations:
    """DOCTRINE §4 — the Four Legal Seams a plugin token may appear at.

    ``composition_root`` (Seam 2) and ``manifest_files`` (Seam 4) are
    deployable-relative paths; ``data_vocabulary_prefixes`` (Seam 1) are
    deployable-relative directory prefixes (typically a kernel models
    package) whose UPPER_SNAKE constants are exempt. Seam 3 (generic,
    parameterized ingress routes) never touches an *identifier* or a
    scanned string-literal position, so it needs no location here.
    """

    composition_root: str | None = None
    manifest_files: frozenset[str] = frozenset()
    data_vocabulary_prefixes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Manifest:
    """The typed contract a product's ``app/architecture_manifest.py`` binds.

    Field-by-field mapping to what scanners used to hardcode:

    * ``roots`` — every path a scanner walks (see :class:`ManifestRoots`).
    * ``layers`` — this deployable's domain-partitioned layer names,
      ports included (``("controllers", "ports", "repositories", ...)``).
    * ``domains`` / ``flows`` — the ``app/domains.py`` / ``app/flows.py``
      registries (DOCTRINE §3): domain name → charter, flow-stage name →
      charter. A layer folder must be a key of one or the other.
    * ``universal_domains`` — domains DOCTRINE §3 mandates in every product
      (``user``, ``platform``, ``billing``, ``shared``); each must be a
      ``domains`` key.
    * ``kernel_packages`` — the exactly-four kernel package names (§2).
    * ``plugin_tokens`` — the brand/vendor slugs the seam scanner polices.
    * ``seam_allowlists`` — dated, shrink-only sunset debts, keyed by
      scanner id (``"vertical_slice_seams"``, ``"kernel_direction"``, ...),
      each a ``{path: allowed-hit-count}`` map — one generic mechanism for
      every counted allowlist a scanner needs.
    * ``inline_import_exemptions`` — ``(path, first-imported-name)`` pairs
      the InlineImports scanner accepts without a ``# local:`` tag (a
      documented, shrink-only escape hatch — see the product guards'
      ``_EXEMPT``).
    * ``pure_modules`` — module stems that must never import a
      ``side_effect_facade_roots`` name (kernel pure-math modules, §2).
    * ``single_consumer_allowlist`` — ``commons/shared`` module stems
      currently consumed by exactly one tree (a tracked eviction debt).
    * ``port_membership_tags`` — the comment prefix that documents a
      deliberate single-implementor port (``"# port:"``).
    * ``forbidden_domain_names`` — domain names banned forever
      (``misc`` / ``utils`` / ``helpers``, §3).
    """

    product: str
    deployable: str
    roots: ManifestRoots
    layers: tuple[str, ...]
    domains: dict[str, str]
    flows: dict[str, str] = field(default_factory=dict)
    universal_domains: frozenset[str] = frozenset()
    kernel_packages: frozenset[str] = frozenset(
        {"models", "contracts", "gates", "shared"}
    )
    plugin_tokens: frozenset[str] = frozenset()
    seam_allowlists: dict[str, dict[str, int]] = field(default_factory=dict)
    inline_import_exemptions: frozenset[tuple[str, str]] = frozenset()
    pure_modules: frozenset[str] = frozenset()
    single_consumer_allowlist: frozenset[str] = frozenset()
    port_membership_tags: str = "# port:"
    forbidden_domain_names: frozenset[str] = frozenset({"misc", "utils", "helpers"})

    # --- extension points beyond the pinned field list, each a hardcoded
    # per-scanner value the product previously baked into its own guard file.
    seam_locations: SeamLocations = field(default_factory=SeamLocations)
    domain_layer_root_allowlist: frozenset[str] = frozenset()
    job_idempotency_exemptions: frozenset[str] = frozenset()
    job_root_class: str = "BaseJob"
    job_roots: tuple[str, ...] = ("jobs",)
    idempotency_field_name: str = "idempotency_params"
    side_effect_facade_roots: frozenset[str] = frozenset()
    third_party_packages: frozenset[str] = frozenset()

    @classmethod
    def load(cls, path: Path) -> Manifest:
        """Boot-free load of a product's ``app/architecture_manifest.py``.

        Executed by file location — no package import, no sys.path
        mutation, no app boot (mirrors ``VendorCommonsCommand``'s
        contract and the product guards' ``app/domains.py`` loader). The
        module must bind a module-level ``MANIFEST: Manifest``.
        """
        path = Path(path)
        spec = importlib.util.spec_from_file_location("architecture_manifest", path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load manifest module: {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        manifest = getattr(module, "MANIFEST", None)
        if not isinstance(manifest, cls):
            raise TypeError(
                f"{path} must bind a module-level `MANIFEST: Manifest` "
                f"(got {type(manifest).__name__})"
            )
        return manifest
