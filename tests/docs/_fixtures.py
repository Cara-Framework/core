"""A throwaway product checkout the documentation engine can be pointed at.

Framework tests must never resolve against a real product tree: the answers
would depend on whichever checkout happens to sit beside this one, which is
precisely the coupling ``cara.docs`` was extracted to remove.
"""

from __future__ import annotations

from pathlib import Path

from cara.docs import DocsManifest


def write(path: Path, content: str = "") -> Path:
    """Create ``path`` and every missing parent, then write ``content``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def make_checkout(
    workspace: Path,
    name: str,
    atlas: str = "`some:banned-command` is FORBIDDEN.\n",
) -> Path:
    """Build ``<workspace>/<name>.example/code`` with a documentation viewer.

    The shape — a product directory holding a checkout that owns
    ``docs/index.html`` — is what :func:`cara.docs.sibling_roots` recognises,
    so two calls produce two checkouts that see each other as neighbours.
    """
    root = workspace / f"{name}.example" / "code"
    write(root / "docs" / "index.html", "<html></html>")
    write(root / "CLAUDE.md", atlas)
    return root


def manifest_for(root: Path, product: str, **overrides) -> DocsManifest:
    """A manifest pointing at a fixture checkout."""
    fields: dict = {"viewer_port": 9999}
    fields.update(overrides)
    return DocsManifest(product=product, root=root, **fields)
