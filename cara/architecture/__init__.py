"""cara.architecture — the Guard Pack (DOCTRINE §11).

"The pack converges on ONE implementation. Guard logic (the AST scanners)
belongs in the framework; products supply only their manifests." A product
wires one :class:`Manifest` per deployable (``app/architecture_manifest.py``)
and gets every scanner in :mod:`cara.architecture.scanners`, the shared
:class:`ImportGraph` and :class:`BarrelGenerator` tools, and the
``craft arch:check`` / ``craft arch:barrels`` commands, all boot-free.
"""

from .BarrelGenerator import BarrelGenerator, BarrelPlan
from .Finding import Finding
from .ImportGraph import ImportGraph
from .Manifest import Manifest, ManifestRoots, SeamLocations

# ``scanners`` is a regular subpackage (import it directly —
# ``from cara.architecture.scanners import ImportTiers`` or
# ``cara.architecture.scanners.REGISTRY``) and is deliberately NOT
# re-exported here: a name that is both an ``__all__`` entry and a
# submodule is the exact shadowing footgun ``test_http_lazy_exports.py``
# guards against repo-wide.
__all__ = [
    "BarrelGenerator",
    "BarrelPlan",
    "Finding",
    "ImportGraph",
    "Manifest",
    "ManifestRoots",
    "SeamLocations",
]
