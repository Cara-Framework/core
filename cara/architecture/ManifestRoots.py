"""ManifestRoots."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ManifestRoots:
    """Filesystem roots one deployable's Guard Pack run needs.

    ``deployable`` is the directory craft runs from (e.g. ``.../api``).
    ``app``/``config`` are expected to exist; ``routes`` (api-shaped
    products) and ``packages`` (plugin-shaped products, DOCTRINE §4) are
    each optional — a deployable declares only the ones it has.

    ``scanner_roots`` maps each scanner id to its exact product-owned trees;
    this is deliberately scanner-specific because import tiers, deep-import
    form, plugin seams and port implementors do not share one honest scope.

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
    scanner_roots: dict[str, tuple[Path, ...]] = field(default_factory=dict)
    kernel: dict[str, Path] = field(default_factory=dict)
    consumer_roots: dict[str, tuple[Path, ...]] = field(default_factory=dict)
    framework_root_name: str = "cara"
    kernel_dev_root_name: str = "commons"
    local_root_names: tuple[str, ...] = ("app", "config", "routes", "packages")

    @property
    def app_namespace(self) -> str:
        """Dotted root of every app-local import: ``app`` in a product tree,
        ``cara`` when the pack is pointed at the framework's own source.

        Scanners used to spell this ``"app"`` as a literal. A wrong literal in
        a guard is invisible: ``ImportForm`` and ``SourceShape`` compared
        against ``app.<layer>`` / ``app/<layer>/`` and, aimed at a tree rooted
        under any other name, matched NOTHING and reported a clean pass over
        code they had never judged — a vacuously green guard, the one outcome
        §9 forbids. It is also why cara itself could not be scanned for the
        first eleven months the Guard Pack existed. ``BarrelGenerator`` always
        derived the namespace from this directory; the scanners now read the
        same source instead of restating it (§5).
        """
        return self.app.name

    @property
    def app_path_prefix(self) -> str:
        """``app_namespace`` as it appears at the head of a deployable-relative
        path (``app`` / ``cara``), for scanners that classify by PATH rather
        than by dotted module. Derived, not assumed equal to the dotted name:
        the two only coincide because the app package sits directly under the
        deployable, and a guard should not depend on a coincidence it never
        checked.
        """
        try:
            return self.app.absolute().relative_to(self.deployable.absolute()).as_posix()
        except ValueError:
            return self.app.name

    def scan_dirs(self, scanner: str) -> tuple[Path, ...]:
        """Exact product-owned trees governed by ``scanner``.

        Import-tier, import-form, inline-import, port-membership and plugin-seam
        guards intentionally have different scopes. Requiring an explicit map
        prevents a scanner from silently skipping a product-owned top-level
        tree or accidentally treating plugin packages as core.
        """
        try:
            return self.scanner_roots[scanner]
        except KeyError as exc:
            raise ValueError(f"scanner roots are not declared for {scanner!r}") from exc
