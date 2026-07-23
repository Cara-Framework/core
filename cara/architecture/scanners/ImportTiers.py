"""ImportTiers: the four import tiers (DOCTRINE §5.1).

Every Python file's LEADING top-level import block must order
stdlib → third-party → framework/kernel (``cara`` / the dev-only kernel
root) → app-local. Within a tier, alphabetical order is a formatter's job;
the tier ORDER is the load-bearing convention this scanner pins.

Tiers (root of the imported dotted path, or 3 for any relative import):

    0 stdlib            sys.stdlib_module_names + __future__
    1 third-party       everything not recognised as 0/2/3 below
    2 framework/kernel  ``roots.framework_root_name`` (``cara``) and
                        ``roots.kernel_dev_root_name`` (``commons``)
    3 app-local         ``roots.local_root_names`` (``app``/``config``/
                        ``routes``/``packages``) and any relative import

If the product declares ``third_party_packages`` (a closed enumeration),
tier 1 is reserved for THOSE names and an unrecognised third-party import
gets tier 4 — "accepted only at the end" — so a brand-new dependency's
placement is a deliberate, reviewed act rather than a silent pass. A
product that leaves the set empty gets the simpler 4-tier model with a
single catch-all third-party tier.

Only the file's LEADING import block is policed (a docstring may precede
it); function-local imports are InlineImports' territory.
"""

from __future__ import annotations

import ast

from cara.architecture._ast_utils import STDLIB, parse, python_files, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest


def _tier(manifest: Manifest, root: str, is_relative: bool) -> int:
    if is_relative:
        return 3
    roots = manifest.roots
    if root in STDLIB:
        return 0
    if root in (roots.framework_root_name, roots.kernel_dev_root_name):
        return 2
    if root in roots.local_root_names:
        return 3
    if manifest.third_party_packages:
        return 1 if root in manifest.third_party_packages else 4
    return 1


def _leading_import_rows(tree: ast.Module) -> list[tuple[int, int | None, str]]:
    """``(lineno, tier_input_is_relative_marker, module_name)`` for the file's
    leading import block only (stops at the first non-import statement,
    a module docstring excepted)."""
    body = tree.body
    start = 0
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        start = 1
    out: list[tuple[int, bool, str]] = []
    for node in body[start:]:
        if not isinstance(node, (ast.Import, ast.ImportFrom)):
            break
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            is_rel = (node.level or 0) > 0
            name = module or ("." * (node.level or 0))
        else:
            module = node.names[0].name if node.names else ""
            is_rel = False
            name = module
        if not module and not is_rel:
            continue
        out.append((node.lineno, is_rel, name))
    return out


class ImportTiers:
    """Enforce the four-tier top-level import ordering (§5.1)."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        for base in manifest.roots.scan_dirs("import_tiers"):
            for path in python_files(base):
                tree = parse(path)
                if tree is None:
                    continue
                rows = _leading_import_rows(tree)
                if len(rows) < 2:
                    continue
                rel = relpath(path, manifest.roots.deployable)
                max_seen = 0
                for lineno, is_rel, name in rows:
                    root = name.split(".")[0] if not is_rel else name
                    tier = _tier(manifest, root, is_rel)
                    if tier < max_seen:
                        findings.append(
                            Finding(
                                rel,
                                lineno,
                                f"`{name}` (tier {tier}) appears after a tier-{max_seen} "
                                f"import — order stdlib -> third-party -> framework/kernel "
                                f"-> app-local",
                            )
                        )
                    max_seen = max(max_seen, tier)
        return findings
