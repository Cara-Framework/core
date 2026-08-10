"""DocsManifest: the engine's ONLY source of product knowledge.

The generator used to work out which product it was walking by probing for a
file only one of them had — ``PRODUCT = "a" if <path>.exists() else "b"``. That
line put each product's name inside the other product's tooling, and it was
duplicated in both copies, so fixing it in one place fixed nothing. Every fact
it guessed is a field here now, and the engine reads nothing else.
"""

from __future__ import annotations

import pytest

from cara.docs import DocsManifest

from ._fixtures import make_checkout, write


def test_discover_root_finds_the_checkout_that_owns_the_viewer(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    deep = write(root / "services" / "app" / "commands" / "Thing.py", "")

    assert DocsManifest.discover_root(deep) == root


def test_discover_root_refuses_a_tree_with_no_viewer(tmp_path):
    orphan = write(tmp_path / "loose" / "Thing.py", "")

    with pytest.raises(FileNotFoundError, match="index.html"):
        DocsManifest.discover_root(orphan)


def test_derived_paths_follow_the_declared_docs_layout(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    manifest = DocsManifest(product="alpha", root=root, viewer_port=9101)

    assert manifest.docs == root / "docs"
    assert manifest.reference == root / "docs" / "internal" / "reference"


def test_load_reads_the_module_level_manifest(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    module = write(
        root / "services" / "app" / "docs_manifest.py",
        "from pathlib import Path\n\n"
        "from cara.docs import DocsManifest\n\n"
        f"MANIFEST = DocsManifest(product='alpha', root=Path({str(root)!r}), "
        "viewer_port=9101)\n",
    )

    manifest = DocsManifest.load(module)

    assert manifest.product == "alpha"
    assert manifest.viewer_port == 9101


def test_load_rejects_a_module_that_binds_no_manifest(tmp_path):
    module = write(tmp_path / "docs_manifest.py", "MANIFEST = 3\n")

    with pytest.raises(TypeError, match="MANIFEST"):
        DocsManifest.load(module)


def test_the_viewer_port_has_no_framework_default():
    """Two checkouts sharing a default port serve each other's documentation.

    Whichever viewer binds first wins and the second silently answers with the
    other product's pages, so the port is a required field: a product that
    forgets it fails to construct rather than quietly colliding.
    """
    with pytest.raises(TypeError):
        DocsManifest(product="alpha", root="/tmp")  # type: ignore[call-arg]
