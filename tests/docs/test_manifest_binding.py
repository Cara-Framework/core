"""A documentation command must IMPORT where no documentation exists.

The production image is built from one deployable directory as its build
context, so the workspace-level ``docs/`` tree never enters it — while the
command provider still imports the docs commands at boot to register them.

A manifest resolved in a class body therefore runs
:meth:`DocsManifest.discover_root` at import time, and that call raises when no
ancestor holds ``docs/index.html``. The result is a container that dies on
import while every guard in development stays green, because a development
checkout always has the documentation tree. These tests pin the late binding
that keeps import and resolution apart.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from cara.commands.core.DocsCommand import DocsCommand
from cara.commands.core.DocsGenerateCommand import DocsGenerateCommand
from cara.docs import DocsManifest

from ._fixtures import make_checkout, write


def _manifest_module(path: Path, root: Path, product: str) -> Path:
    """A product's ``docs_manifest.py``, resolving its root the way products do."""
    return write(
        path,
        textwrap.dedent(
            f"""
            from pathlib import Path

            from cara.docs import DocsManifest

            MANIFEST = DocsManifest(
                product={product!r},
                root=DocsManifest.discover_root(Path({str(root)!r})),
                viewer_port=9999,
            )
            """
        ),
    )


def test_binding_a_path_touches_no_filesystem_until_the_command_runs(tmp_path):
    """Defining the subclass must not read the manifest module at all."""
    missing = tmp_path / "never" / "docs_manifest.py"

    class Product(DocsGenerateCommand):
        manifest_path = missing

    # Class creation is the import-time event. It completed, and nothing was
    # read: the path does not even exist.
    assert not missing.exists()
    assert Product.manifest is None


def test_the_manifest_module_is_read_on_first_use(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    module = _manifest_module(tmp_path / "docs_manifest.py", root, "alpha")

    class Product(DocsCommand):
        manifest_path = module

    command = Product()
    manifest = command._manifest()
    assert manifest.product == "alpha"
    assert manifest.root == root


def test_a_resolved_manifest_is_cached_on_the_instance_not_the_class(tmp_path):
    """One run's product must never become the default for the next.

    Caching on the class would let a process that generated product A's docs
    answer product B's run with A's manifest — the cross-product bleed the
    manifest exists to prevent.
    """
    root = make_checkout(tmp_path, "alpha")
    module = _manifest_module(tmp_path / "docs_manifest.py", root, "alpha")

    class Product(DocsCommand):
        manifest_path = module

    first = Product()
    assert first._manifest().product == "alpha"
    assert first.manifest is not None
    # The class is untouched, so a second instance resolves from the path again.
    assert Product.manifest is None
    assert "manifest" not in Product.__dict__ or Product.__dict__["manifest"] is None


def test_an_in_process_manifest_still_wins(tmp_path):
    """Callers holding a manifest bind it directly and never read a module."""
    root = make_checkout(tmp_path, "alpha")

    class Product(DocsCommand):
        manifest = DocsManifest(product="alpha", root=root, viewer_port=9999)
        manifest_path = tmp_path / "absent" / "docs_manifest.py"

    assert Product()._manifest().product == "alpha"


def test_the_docs_commands_import_with_no_documentation_tree_anywhere(tmp_path):
    """The production-image shape: a deployable copied without the docs tree.

    This is the regression that killed boot — the product module resolved its
    manifest in the class body, so importing the command provider raised
    ``FileNotFoundError`` in an image that simply has no documentation.
    """
    deployable = tmp_path / "image" / "app"
    _manifest_module(deployable / "docs_manifest.py", tmp_path / "image", "alpha")
    write(
        deployable / "commands" / "GenerateDocs.py",
        textwrap.dedent(
            """
            from pathlib import Path

            from cara.commands.core.DocsGenerateCommand import DocsGenerateCommand

            _MANIFEST_PATH = Path(__file__).resolve().parents[1] / "docs_manifest.py"


            class GenerateDocs(DocsGenerateCommand):
                name = "maintenance:docs"
                manifest_path = _MANIFEST_PATH
            """
        ),
    )

    # No ancestor of the deployable holds docs/index.html — exactly the image.
    with pytest.raises(FileNotFoundError):
        DocsManifest.discover_root(deployable)

    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_image_generate_docs",
        deployable / "commands" / "GenerateDocs.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Registration reads the command's identity, never its manifest.
    assert module.GenerateDocs.name == "maintenance:docs"
