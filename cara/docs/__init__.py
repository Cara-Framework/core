"""cara.docs — the documentation engine products configure rather than copy.

Three passes over a checkout, all pure filesystem (AST, regex and git; no DB,
no broker, no app boot):

1. REFERENCE GENERATION — the route/queue/model/job/command/permission/env and
   package inventories, extracted straight from code. These pages are never
   hand-written, so they cannot go stale.
2. FRESHNESS — every hand-written page declares its ``sources:`` and the date a
   human last read it against them; sources newer than that date are STALE.
3. CLAIM VERIFICATION — the paths, pointers, commands and ports a page asserts,
   resolved against this checkout, plus every line that PRESCRIBES a practice
   the atlas forbids.

Product knowledge enters as a :class:`~cara.docs.Manifest.DocsManifest` and
nowhere else. This package never imports a product, never names one, and never
infers which product it is walking from what happens to be on disk.

``Claims``, ``ClaimSources``, ``Freshness``, ``Inventory``, ``Manifest`` and
``Support`` are regular submodules — import them directly. They are
deliberately not re-exported here: a name that is both an ``__all__`` entry and
a submodule is a shadowing footgun.
"""

from .ClaimSources import check_path_claim, sibling_roots
from .Claims import atlas_bans, check_forbidden, claims_report, verify_claims
from .Freshness import freshness, front_matter, gen_nav
from .Inventory import generate_reference
from .Manifest import DocsManifest, ModelScope
from .Support import newest_change, read, write_if_changed

# ``owned_markdowns`` is deliberately absent: ClaimSources exposes the
# four-argument primitive and Claims the manifest-shaped wrapper, and a barrel
# entry would silently pick one of the two signatures for every caller.
__all__ = [
    "DocsManifest",
    "ModelScope",
    "atlas_bans",
    "check_forbidden",
    "check_path_claim",
    "claims_report",
    "freshness",
    "front_matter",
    "gen_nav",
    "generate_reference",
    "newest_change",
    "read",
    "sibling_roots",
    "verify_claims",
    "write_if_changed",
]
