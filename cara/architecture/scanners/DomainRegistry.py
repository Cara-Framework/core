"""DomainRegistry: the mirror rule, enforced (DOCTRINE §3).

A deployable declares its domains in ONE file (``app/domains.py`` ->
``manifest.domains``: name -> one-line charter) and, optionally, its
non-domain FLOW STAGES in a sibling file (``app/flows.py`` ->
``manifest.flows`` — a worker's ``jobs/pipeline/`` stage tree groups by
mechanics, not business capability, and is declared as such rather than
smuggled into the domain registry). This scanner makes both registries
real:

* every folder in a domain-partitioned layer (``manifest.layers``) must be
  a ``domains`` OR ``flows`` key — a folder that exists only on disk is an
  undeclared domain (or an undeclared flow-stage tree);
* every ``domains`` key must have at least one member module in at least
  one layer — a memberless entry is a dead charter;
* ``manifest.forbidden_domain_names`` (``misc``/``utils``/``helpers`` by
  default) may never be a ``domains`` key — junk-drawer magnets, forever;
* every ``manifest.universal_domains`` name DOCTRINE §3 mandates
  (``user``/``platform``/``billing``/``shared``) must be declared;
* a domain-partitioned layer root holds only ``__init__.py`` plus domain
  folders — a loose module there must be in the documented, per-product
  ``manifest.domain_layer_root_allowlist`` (a base class, §3's "at most a
  documented base class" exception) or it is a Finding; a stale allowlist
  entry (the file no longer exists) is a Finding too;
* every charter (domain AND flow) is a real, non-empty one-liner — a dict
  of names is not a registry.

Registry SHAPE, on the same pass, because a registry that is merely populated
is not yet a registry you can read:

* keys are alphabetical and are lowercase identifiers — a registry read as a
  list needs a stable order, and a name that is not an identifier cannot be a
  package folder;
* ``domains`` and ``flows`` never share a key — §3 v1.3 registers a
  stage-grouped folder in exactly one of the two, and an overlap makes
  "which registry owns this?" unanswerable;
* the MIRROR EXCEPTION is current: a domain present in exactly one domain
  layer must be pinned in ``manifest.single_layer_domains``, and a pinned
  name that has since grown a second layer is a stale pin. Both directions
  fail, so the pin cannot rot in either;
* ``manifest.registry_size_bounds``, when a product sets it, holds the
  combined ``domains`` + ``flows`` entry count inside its declared budget.
  It stays OFF by default: §3 makes registry size a REVIEW threshold, and a
  default-on budget would be a doctrine amendment rather than a scanner.

These four lived in a hand-written product test that only ONE of the two
products carried — and the master atlas claimed both did. The claim was false
for weeks precisely because the rule had no framework home.
"""

from __future__ import annotations

from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

#: Registry name -> the product file a shape finding should point the reader at.
_REGISTRY_FILES = {"DOMAINS": "app/domains.py", "FLOWS": "app/flows.py"}


def _registries(manifest: Manifest) -> tuple[tuple[str, dict[str, str]], ...]:
    return (("DOMAINS", manifest.domains), ("FLOWS", manifest.flows))


def _layer_domain_dirs(manifest: Manifest, layer: str) -> list:
    base = manifest.roots.app / layer
    if not base.is_dir():
        return []
    return sorted(p for p in base.iterdir() if p.is_dir() and p.name != "__pycache__")


class DomainRegistry:
    """Mirror rule + registry membership + forbidden names + flows partition."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        return (
            DomainRegistry._unregistered_layer_folders(manifest)
            + DomainRegistry._memberless_domains(manifest)
            + DomainRegistry._forbidden_domain_names(manifest)
            + DomainRegistry._missing_universal_domains(manifest)
            + DomainRegistry._loose_layer_root_files(manifest)
            + DomainRegistry._unreal_charters(manifest)
            + DomainRegistry._registry_shape(manifest)
            + DomainRegistry._overlapping_registries(manifest)
            + DomainRegistry._stale_mirror_exceptions(manifest)
            + DomainRegistry._registry_size(manifest)
        )

    @staticmethod
    def _unregistered_layer_folders(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        known = set(manifest.domains) | set(manifest.flows)
        for layer in manifest.domain_layers:
            for folder in _layer_domain_dirs(manifest, layer):
                if folder.name not in known:
                    findings.append(
                        Finding(
                            f"app/{layer}/{folder.name}",
                            0,
                            "not a DOMAINS or FLOWS key — declare the domain (and "
                            "its one-line charter) or the flow stage",
                        )
                    )
        return findings

    @staticmethod
    def _memberless_domains(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        for name in sorted(manifest.domains):
            has_member = any(
                any(
                    child.suffix == ".py" and child.stem != "__init__"
                    for child in (manifest.roots.app / layer / name).rglob("*.py")
                )
                for layer in manifest.domain_layers
                if (manifest.roots.app / layer / name).is_dir()
            )
            if not has_member:
                findings.append(
                    Finding(
                        "app/domains.py",
                        0,
                        f"DOMAINS[{name!r}] has no member module in any domain "
                        f"layer — drop the dead charter or add its first member",
                    )
                )
        return findings

    @staticmethod
    def _forbidden_domain_names(manifest: Manifest) -> list[Finding]:
        return [
            Finding(
                "app/domains.py",
                0,
                f"DOMAINS[{name!r}] — forbidden domain name (junk-drawer magnet, §3)",
            )
            for name in sorted(manifest.domains)
            if name in manifest.forbidden_domain_names
        ]

    @staticmethod
    def _missing_universal_domains(manifest: Manifest) -> list[Finding]:
        missing = sorted(manifest.universal_domains - set(manifest.domains))
        return [
            Finding("app/domains.py", 0, f"universal domain {name!r} is not declared")
            for name in missing
        ]

    @staticmethod
    def _loose_layer_root_files(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        for layer in manifest.domain_layers:
            base = manifest.roots.app / layer
            if not base.is_dir():
                continue
            for path in sorted(base.glob("*.py")):
                if path.name == "__init__.py":
                    continue
                rel = f"{layer}/{path.name}"
                if rel in manifest.domain_layer_root_allowlist:
                    continue
                findings.append(
                    Finding(
                        f"app/{rel}",
                        0,
                        "loose module at a domain-partitioned layer root — move it "
                        "into a domain folder, or (a documented base class only) "
                        "pin it in domain_layer_root_allowlist",
                    )
                )
        for entry in sorted(manifest.domain_layer_root_allowlist):
            if not (manifest.roots.app / entry).is_file():
                findings.append(
                    Finding(
                        "app/domains.py",
                        0,
                        f"domain_layer_root_allowlist entry {entry!r} no longer "
                        f"exists — drop it",
                    )
                )
        return findings

    @staticmethod
    def _registry_shape(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        for registry_name, registry in _registries(manifest):
            if list(registry) != sorted(registry):
                findings.append(
                    Finding(
                        _REGISTRY_FILES[registry_name],
                        0,
                        f"{registry_name} keys are not alphabetical",
                    )
                )
            findings.extend(
                Finding(
                    _REGISTRY_FILES[registry_name],
                    0,
                    f"{registry_name}[{name!r}] is not a lowercase identifier — a "
                    f"registry key has to be usable as a package folder name",
                )
                for name in registry
                if name != name.lower() or not name.isidentifier()
            )
        return findings

    @staticmethod
    def _overlapping_registries(manifest: Manifest) -> list[Finding]:
        overlap = sorted(set(manifest.domains) & set(manifest.flows))
        return [
            Finding(
                "app/domains.py",
                0,
                f"{name!r} is registered as BOTH a domain and a flow stage — §3 "
                f"puts a stage-grouped folder in exactly one registry",
            )
            for name in overlap
        ]

    @staticmethod
    def _stale_mirror_exceptions(manifest: Manifest) -> list[Finding]:
        """A one-layer domain must be pinned, and a pin must still be true."""
        findings: list[Finding] = []
        pinned = manifest.single_layer_domains
        if len(manifest.domain_layers) < 2:
            # Nothing to mirror ACROSS: with one domain layer every domain is
            # trivially single-layer, so the rule would flag the whole registry
            # and say nothing. A stale pin is still worth reporting, though.
            return [
                Finding(
                    "app/architecture_manifest.py",
                    0,
                    f"single_layer_domains pins {name!r}, but this deployable has "
                    f"fewer than two domain layers — the pin cannot mean anything",
                )
                for name in sorted(pinned)
            ]
        registered = {**manifest.domains, **manifest.flows}
        for name in sorted(registered):
            layers = [
                layer
                for layer in manifest.domain_layers
                if (manifest.roots.app / layer / name).is_dir()
            ]
            if len(layers) == 1 and name not in pinned:
                findings.append(
                    Finding(
                        f"app/{layers[0]}/{name}",
                        0,
                        f"{name!r} exists in one domain layer only — mirror it "
                        f"across its layers or pin it in single_layer_domains",
                    )
                )
            elif len(layers) > 1 and name in pinned:
                findings.append(
                    Finding(
                        "app/architecture_manifest.py",
                        0,
                        f"stale single_layer_domains pin {name!r}: it now spans "
                        f"{', '.join(layers)}",
                    )
                )
        return findings

    @staticmethod
    def _registry_size(manifest: Manifest) -> list[Finding]:
        bounds = manifest.registry_size_bounds
        if bounds is None:
            return []
        low, high = bounds
        total = len(manifest.domains) + len(manifest.flows)
        if low <= total <= high:
            return []
        return [
            Finding(
                "app/domains.py",
                0,
                f"DOMAINS + FLOWS holds {total} entries; this product declares a "
                f"budget of {low}-{high}",
            )
        ]

    @staticmethod
    def _unreal_charters(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        if not manifest.domains:
            findings.append(Finding("app/domains.py", 0, "DOMAINS is empty"))
        for registry_name, registry in (
            ("DOMAINS", manifest.domains),
            ("FLOWS", manifest.flows),
        ):
            for name, charter in registry.items():
                if not isinstance(charter, str) or not charter.strip():
                    findings.append(
                        Finding(
                            "app/domains.py",
                            0,
                            f"{registry_name}[{name!r}] has no real charter",
                        )
                    )
        return findings
