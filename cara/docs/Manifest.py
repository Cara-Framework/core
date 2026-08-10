"""DocsManifest: the ONE typed contract a product gives the docs engine.

The generator, the freshness checker and the claim verifier are pure
filesystem passes — AST, regex and git over a checkout. None of that knows
which product it is walking, and it must not: the moment the engine sniffs a
product ("this file exists, therefore I am X") it has hard-coded a second
product's name into the first product's tree, and the two copies start
drifting the day one of them is fixed.

So every product-specific fact is a FIELD here instead, and the engine reads
nothing else. A product wires this once, at ``app/docs_manifest.py``, binding
a module-level ``MANIFEST: DocsManifest``; :meth:`load` reads it with zero app
boot — the same boot-free contract ``cara.architecture.Manifest`` uses for the
Guard Pack.

Everything NOT here is framework convention, not product knowledge: the
deployable names (``api`` / ``services``), the dev-only kernel root
(``commons``), the routes package and the reference page set are all DOCTRINE
layout, identical in every Cara product, and the generators probe for each one
and no-op when it is absent.
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ModelScope:
    """How a product marks a model as scoped to one partition of its data.

    The engine only ever asks "is ``base`` among this class's bases?" — the
    partition itself (a tenant, an account, a region, a shop) is product
    vocabulary, so the words printed on the page travel WITH the class name
    rather than being hard-coded next to the AST walk. All three fields are
    required: a base with no label produces an unnamed column, and a label
    with no explanation produces a tick mark nobody can act on.

    * ``base`` — the mixin class name that appears in a scoped model's bases.
    * ``label`` — the column header; its lower-cased form also reads as the
      adjective in the summary line ("N of M models are tenant-scoped").
    * ``note`` — one sentence saying what the scope actually enforces.
    """

    base: str
    label: str
    note: str


@dataclass(frozen=True, slots=True)
class DocsManifest:
    """Product facts the documentation engine cannot derive for itself.

    * ``product`` — the name printed on every generated page and used in claim
      verdicts ("belongs to <other>, not <product>"). Explicit, because the
      alternative is a filesystem sniff that has to know the other product's
      name to rule it out.
    * ``root`` — the checkout that owns ``docs/``; see :meth:`discover_root`.
    * ``viewer_port`` / ``viewer_host`` — where ``docs:serve`` binds. A
      DEFAULT-FREE field on purpose: two checkouts on one machine sharing a
      port means whichever viewer binds first wins and the second silently
      serves the other product's documentation.
    * ``port_source_dirs`` — the component directories whose configuration
      declares ports. ``api``/``services`` exist everywhere; the front-end
      names differ per product, so the list is supplied rather than guessed.
    * ``packages_page_title`` — heading for the plug-in package inventory.
      What those packages ARE is product vocabulary; that they exist under
      ``services/packages`` is DOCTRINE §4.
    * ``model_scope`` — how this product marks a model as scoped to one
      partition of its data, or ``None`` when it has no such concept. DEFAULT-
      FREE on purpose: a default here would be one product's mixin name and
      its vocabulary compiled into the framework, silently inherited by every
      other product until the day one of them happens to define a class by the
      same name. ``None`` omits the column entirely.
    * ``generator_command`` — the CLI name the product registers this engine
      under. Every generated page opens by naming the command that rebuilds
      it, so a product that renames the command must say so here or the pages
      would print an invocation that does not exist.
    """

    product: str
    root: Path
    viewer_port: int
    viewer_host: str = "127.0.0.1"
    port_source_dirs: tuple[str, ...] = ("api", "services")
    packages_page_title: str = "Packages"
    model_scope: ModelScope | None = None
    generator_command: str = "maintenance:docs"
    docs_dirname: str = "docs"
    reference_subpath: tuple[str, ...] = ("internal", "reference")

    @property
    def docs(self) -> Path:
        """Root of the documentation tree this product owns."""
        return self.root / self.docs_dirname

    @property
    def reference(self) -> Path:
        """Directory holding the code-derived reference pages."""
        return self.docs.joinpath(*self.reference_subpath)

    @staticmethod
    def discover_root(marker: Path, docs_dirname: str = "docs") -> Path:
        """Nearest ancestor of ``marker`` that owns a documentation viewer.

        A checkout is identified by ``<root>/<docs>/index.html``: the viewer
        page is present in a real checkout and absent from every sparse or
        single-deployable CI layout, which is exactly the distinction the
        callers need.
        """
        start = Path(marker).resolve()
        for candidate in (start, *start.parents):
            if (candidate / docs_dirname / "index.html").is_file():
                return candidate
        raise FileNotFoundError(
            f"no documentation root above {start}: expected an ancestor "
            f"containing {docs_dirname}/index.html"
        )

    @classmethod
    def load(cls, path: Path) -> DocsManifest:
        """Boot-free load of a product's ``app/docs_manifest.py``.

        Executed by file location — no package import, no ``sys.path``
        mutation, no app boot, so a bootless command and a test suite in a
        different deployable can both read it.
        """
        path = Path(path)
        spec = importlib.util.spec_from_file_location("docs_manifest", path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load documentation manifest module: {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        manifest = getattr(module, "MANIFEST", None)
        if not isinstance(manifest, cls):
            raise TypeError(
                f"{path} must bind a module-level `MANIFEST: DocsManifest` "
                f"(got {type(manifest).__name__})"
            )
        return manifest


__all__ = ["DocsManifest", "ModelScope"]
