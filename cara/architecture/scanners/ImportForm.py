"""ImportForm: consumer import FORM (DOCTRINE §5.1).

Three rules, all module-level only (function-local placement is
InlineImports' territory; ``if TYPE_CHECKING:`` blocks never execute):

* **Barrel for consumers.** A file OUTSIDE a domain-partitioned layer
  imports from that layer through a barrel — the layer barrel
  (``app.services``) or a domain barrel (``app.services.channels``) —
  never a leaf module deep-path (``app.services.channels.ChannelService``
  reaching into the module directly). Exempt: a leaf bound as a MODULE
  OBJECT by its own package ``__init__`` (``from . import X``) — its
  symbols are deliberately module-qualified.
* **Direct path for siblings.** A file INSIDE layer ``L`` never imports
  ``app.L`` (its own layer barrel): during barrel ``__init__`` execution
  the package is only partially initialised, and a sibling reaching
  through the barrel is a boot-order crash.
* **Kernel via app.* barrels only.** The dev-only kernel root
  (``roots.kernel_dev_root_name`` — ``commons``) may be imported ONLY by
  the four kernel barrels themselves (``app/<kernel_pkg>/__init__.py``);
  every other file reaches the kernel through ``app.*``. This is the ONE
  swap point ``build:vendor-commons`` rewrites at production build, so
  this check walks every import statement (incl. function-local and
  string-free plain ``import``), not just the leading block.
"""

from __future__ import annotations

import ast
from pathlib import Path

from cara.architecture._ast_utils import module_object_names, parse, python_files, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest


def _kernel_barrel_files(manifest: Manifest) -> frozenset[str]:
    return frozenset(f"app/{pkg}/__init__.py" for pkg in manifest.kernel_packages)


class ImportForm:
    """Barrel-for-consumers / direct-for-siblings / kernel-via-app-barrels."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        return (
            ImportForm._no_deep_imports_from_outside_a_layer(manifest)
            + ImportForm._siblings_never_import_own_barrel(manifest)
            + ImportForm._kernel_consumed_only_via_app_barrels(manifest)
        )

    @staticmethod
    def _no_deep_imports_from_outside_a_layer(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        exercised_allowlist: set[tuple[str, str]] = set()
        app = manifest.roots.app
        layers = manifest.layers
        deep_prefixes = tuple(f"app.{layer}." for layer in layers)
        for base in manifest.roots.scan_dirs("import_form"):
            for path in python_files(base):
                if path.name == "__init__.py":
                    continue
                tree = parse(path)
                if tree is None:
                    continue
                rel = relpath(path, manifest.roots.deployable)
                for node in tree.body:
                    if (
                        not isinstance(node, ast.ImportFrom)
                        or node.level != 0
                        or not node.module
                    ):
                        continue
                    module = node.module
                    matched_layer = next(
                        (
                            layer
                            for layer, prefix in zip(layers, deep_prefixes, strict=True)
                            if module.startswith(prefix)
                        ),
                        None,
                    )
                    if matched_layer is None:
                        continue
                    if rel.startswith(f"app/{matched_layer}/"):
                        continue  # sibling — direct path is the rule there
                    if all(alias.name.startswith("_") for alias in node.names):
                        continue  # private symbols deliberately have no barrel home
                    allowlist_key = (rel, module)
                    if allowlist_key in manifest.deep_import_allowlist:
                        exercised_allowlist.add(allowlist_key)
                        continue
                    target = app / Path(*module.split(".")[1:])
                    if target.is_dir():
                        continue  # domain/package barrel import — legal
                    leaf = target.with_suffix(".py")
                    if leaf.exists() and leaf.stem in module_object_names(leaf.parent):
                        continue  # module-object contract
                    findings.append(
                        Finding(
                            rel,
                            node.lineno,
                            f"deep import `from {module} import ...` — use the "
                            f"app.{matched_layer} barrel",
                        )
                    )
        for rel, module in sorted(manifest.deep_import_allowlist - exercised_allowlist):
            findings.append(
                Finding(
                    rel,
                    0,
                    f"stale deep-import allowlist entry for {module!r} — delete it",
                )
            )
        return findings

    @staticmethod
    def _siblings_never_import_own_barrel(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        for layer in manifest.layers:
            layer_dir = manifest.roots.app / layer
            for path in python_files(layer_dir):
                if path.name == "__init__.py":
                    continue
                tree = parse(path)
                if tree is None:
                    continue
                rel = relpath(path, manifest.roots.deployable)
                for node in tree.body:
                    if (
                        isinstance(node, ast.ImportFrom)
                        and node.level == 0
                        and node.module == f"app.{layer}"
                    ):
                        findings.append(
                            Finding(
                                rel,
                                node.lineno,
                                f"sibling reaches through its own layer barrel "
                                f"`app.{layer}` — import the submodule directly",
                            )
                        )
        return findings

    @staticmethod
    def _kernel_consumed_only_via_app_barrels(manifest: Manifest) -> list[Finding]:
        findings: list[Finding] = []
        kernel_root = manifest.roots.kernel_dev_root_name
        kernel_barrels = _kernel_barrel_files(manifest)
        for base in manifest.roots.scan_dirs("import_form"):
            for path in python_files(base):
                rel = relpath(path, manifest.roots.deployable)
                if rel in kernel_barrels:
                    continue
                tree = parse(path)
                if tree is None:
                    continue
                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom) and node.level == 0:
                        bases = [node.module or ""]
                    elif isinstance(node, ast.Import):
                        bases = [a.name for a in node.names]
                    else:
                        continue
                    for candidate in bases:
                        if candidate == kernel_root or candidate.startswith(
                            kernel_root + "."
                        ):
                            findings.append(
                                Finding(
                                    rel,
                                    node.lineno,
                                    f"`{candidate}` — the kernel is consumed only "
                                    f"through the app.* barrels (§2)",
                                )
                            )
        return findings
