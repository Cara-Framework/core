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
counter) are answered by grouping every process tree in
``roots.consumer_roots`` — a process may own more than one root (for example
``services/app`` plus ``services/packages``). A group with no checked-out
root is ignored and the corresponding check no-ops, mirroring the
"whole repo fact, per-service CI" contract the product guards already used.
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass, field
from pathlib import Path

from .ManifestRoots import ManifestRoots
from .SeamLocations import SeamLocations


@dataclass(frozen=True, slots=True)
class Manifest:
    """The typed contract a product's ``app/architecture_manifest.py`` binds.

    Field-by-field mapping to what scanners used to hardcode:

    * ``roots`` — every path a scanner walks (see :class:`ManifestRoots`).
    * ``layers`` — every barrel/import-governed layer, ports included
      (``("controllers", "ports", "repositories", ...)``).
    * ``domain_layers`` — the subset partitioned by ``domains`` / ``flows``.
      Cross-cutting ``support`` trees remain governed by import and barrel
      rules without being misclassified as business domains.
    * ``domains`` / ``flows`` — the ``app/domains.py`` / ``app/flows.py``
      registries (DOCTRINE §3): domain name → charter, flow-stage name →
      charter. A layer folder must be a key of one or the other.
    * ``universal_domains`` — domains DOCTRINE §3 mandates in every product
      (``user``, ``platform``, ``billing``, ``shared``); each must be a
      ``domains`` key.
    * ``kernel_packages`` — the exactly-four kernel package names (§2).
      ``kernel_barrel_packages`` and ``seam_kernel_packages`` select which of
      them participate in those scanners; API/worker twins can split a
      whole-product guard without duplicating findings.
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
    domain_layers: tuple[str, ...]
    domains: dict[str, str]
    scan_plugin_string_literals: bool
    kernel_barrel_packages: frozenset[str]
    seam_kernel_packages: frozenset[str]
    flows: dict[str, str] = field(default_factory=dict)
    universal_domains: frozenset[str] = frozenset()
    kernel_packages: frozenset[str] = frozenset(
        {"models", "contracts", "gates", "shared"}
    )
    plugin_tokens: frozenset[str] = frozenset()
    # Kernel packages that ``build:vendor-commons`` FLAT-copies into
    # ``app/<pkg>/`` and whose deep imports it therefore folds onto the
    # barrel. VendorBarrelParity requires those barrels to be complete
    # supersets; every other kernel package ships verbatim and keeps its
    # sub-paths. Mirrors VendorCommonsCommand's models-specific handling.
    vendor_flattened_packages: frozenset[str] = frozenset({"models"})
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
    # Dotted callable names whose call shape is `forwarder(callable, *args,
    # **kwargs)` == `callable(*args, **kwargs)` — the codebase's thread-offload
    # idiom (`ExecutionContext.run_in_thread` is the sanctioned
    # `asyncio.to_thread` drop-in). CollaboratorCalls treats a bare
    # `self.attr.method` passed as the first argument to one of these the
    # same as a direct `self.attr.method(...)` call.
    collaborator_call_forwarders: frozenset[str] = frozenset(
        {
            "ExecutionContext.run_in_thread",
            "run_in_thread",
            "asyncio.to_thread",
            "to_thread",
        }
    )
    # Dated, shrink-only sunset debt for CollaboratorCalls: exact
    # `"<path>:<line>:self.<attr>.<method>"` pins for pre-rule mismatches.
    collaborator_call_exemptions: frozenset[str] = frozenset()
    job_root_class: str = "BaseJob"
    job_roots: tuple[str, ...] = ("jobs",)
    idempotency_field_name: str = "idempotency_params"
    side_effect_facade_roots: frozenset[str] = frozenset()
    # Imported names to police within those facade modules. Empty means every
    # imported name, for products whose facade module is itself the boundary.
    side_effect_facade_names: frozenset[str] = frozenset()
    third_party_packages: frozenset[str] = frozenset()
    # Dated cycle-breakers where a consumer must import a concrete module
    # instead of its layer/domain barrel. Entries are (consumer path, module).
    deep_import_allowlist: frozenset[tuple[str, str]] = frozenset()
    source_shape_hard_limit: int = 700
    source_shape_edge_method_limit: int = 40
    source_shape_edge_layers: frozenset[str] = frozenset({"controllers", "jobs"})
    flow_edge_layers: frozenset[str] = frozenset({"controllers", "jobs"})
    atomic_repository_methods: frozenset[str] = frozenset()
    write_ownership: dict[str, str] = field(default_factory=dict)

    # --- source-law scanners (raw SQL, ORM reach, HTTP/env leakage, silent
    # swallows). Each value is a rule DIAL, not a scope: scope is always
    # ``roots.scan_dirs(<scanner id>)`` so a product declares exactly which of
    # its layers a law governs.
    #
    # POSIX path fragments that legitimately own raw SQL; matched against a
    # contiguous run of a file's deployable-relative path parts (§5).
    raw_sql_homes: frozenset[str] = frozenset(
        {"repositories", "commons/gates/persistence"}
    )
    # Import roots whose names are ORM model classes. A model reached through
    # one of these is the receiver ModelQueryDiscipline polices.
    model_import_roots: tuple[str, ...] = ("app.models", "models.core")
    # Import prefixes carrying HTTP transport types. Business logic raises
    # domain errors; only the edge speaks HTTP.
    http_import_prefixes: tuple[str, ...] = (
        "cara.http",
        "cara.request",
        "cara.response",
    )
    # ``os.environ`` methods that snapshot the whole mapping rather than read
    # one variable (``copy()`` hands an env to a subprocess) — composition, not
    # a hidden config read.
    env_read_exempt_environ_attrs: frozenset[str] = frozenset({"copy"})

    # --- domain registry shape (§3). ``registry_size_bounds`` stays None by
    # default: §3 makes entry count a REVIEW threshold, so a product opts into
    # enforcing its own budget. ``single_layer_domains`` pins the domains that
    # legitimately appear in exactly one domain layer, making the pin itself
    # go stale-loud when the domain grows a second layer.
    registry_size_bounds: tuple[int, int] | None = None
    single_layer_domains: frozenset[str] = frozenset()

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
