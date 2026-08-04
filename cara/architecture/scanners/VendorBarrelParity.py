"""VendorBarrelParity: the vendor swap point must resolve every folded name
(DOCTRINE §2).

``build:vendor-commons`` treats one kernel package specially. ``models`` is
FLAT-copied into the deployable's ``app/models/`` and every reference to it
— including a DEEP one such as
``from commons.models.core.IdentifierNormalization import normalize_gtin`` —
is folded onto the barrel (``from app.models import normalize_gtin``). Every
other kernel package ships verbatim and keeps its sub-paths, so its
references never depend on the barrel's surface.

That fold is only sound while the deployable's ``app/<pkg>/__init__.py`` is a
complete superset of the kernel package's ``__all__``. It is a hand-written
file in development — a re-export facade over ``commons.<pkg>`` — and the
package it fronts lives in a different repository, so nothing local goes red
when the kernel gains a public name. The failure mode is invisible in
development and fatal in production: dev resolves the deep import against
``commons.models`` and stays green, while the vendored image folds the same
import onto an ``app.models`` barrel that never re-exported the name and
dies at import time. A missing ``normalize_gtin`` shipped exactly this way.

The flattened packages are declared by ``Manifest.vendor_flattened_packages``
so the rule follows the vendor command's behaviour rather than restating it
(§5: read the SSOT, never restate it). Packages that ship verbatim are NOT
checked here — their ``app/<pkg>`` barrel is a deliberately curated door that
the vendor step overwrites with the kernel's own ``__init__``, and their
submodules stay reachable by path.
"""

from __future__ import annotations

from cara.architecture._ast_utils import dunder_all, parse, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest


class VendorBarrelParity:
    """Each flattened kernel package's app barrel re-exports the whole kernel surface."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        for pkg in sorted(manifest.vendor_flattened_packages):
            kernel_dir = manifest.roots.kernel.get(pkg)
            if kernel_dir is None:
                continue
            kernel_all = dunder_all(parse(kernel_dir / "__init__.py"))
            if kernel_all is None:
                continue

            barrel = manifest.roots.app / pkg / "__init__.py"
            rel = relpath(barrel, manifest.roots.deployable)
            barrel_tree = parse(barrel) if barrel.is_file() else None
            if barrel_tree is None:
                findings.append(
                    Finding(
                        rel,
                        0,
                        f"the app.{pkg} barrel is missing — it is the vendor swap "
                        f"point for the flat-copied commons/{pkg} package",
                    )
                )
                continue

            barrel_all = dunder_all(barrel_tree) or []
            missing = sorted(set(kernel_all) - set(barrel_all))
            if missing:
                findings.append(
                    Finding(
                        rel,
                        0,
                        f"does not re-export {len(missing)} public name(s) of "
                        f"commons/{pkg}: {', '.join(missing)} — build:vendor-commons "
                        f"folds deep commons.{pkg} imports onto this barrel, so a "
                        "missing name compiles in development and fails at import "
                        "in the production image",
                    )
                )
        return findings
