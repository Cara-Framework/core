"""Shared tmp-tree + Manifest builders for the Guard Pack test suite.

Leading underscore: a test-support module, not a test file itself (mirrors
the framework's own ``cara/commands/_optional.py`` convention).
"""

from __future__ import annotations

from pathlib import Path

from cara.architecture.Manifest import Manifest, ManifestRoots


def write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def make_manifest(root: Path, **overrides) -> Manifest:
    """A minimal, valid Manifest over a fresh ``tmp_path`` tree: ``app/``,
    ``config/`` and all four dev-only kernel packages (each an empty,
    already-barrelled package) exist. Callers write whatever files their
    scenario needs and pass field overrides (``layers=...``,
    ``domains=...``, ``plugin_tokens=...``, a custom ``roots=...``, ...)."""
    app = root / "app"
    config = root / "config"
    app.mkdir(parents=True, exist_ok=True)
    config.mkdir(parents=True, exist_ok=True)

    kernel: dict[str, Path] = {}
    commons = root / "commons"
    for pkg in ("models", "contracts", "gates", "shared"):
        pkg_dir = commons / pkg
        pkg_dir.mkdir(parents=True, exist_ok=True)
        init = pkg_dir / "__init__.py"
        if not init.exists():
            init.write_text(
                '"""Kernel package."""\n\n__all__: list[str] = []\n', encoding="utf-8"
            )
        kernel[pkg] = pkg_dir

    scanner_roots = {
        scanner: (app, config)
        for scanner in (
            "import_form",
            "import_tiers",
            "inline_imports",
            "domain_ownership",
            "flow_law",
            "port_membership",
            "source_shape",
            "transaction_ownership",
            "vertical_slice_seams",
            "write_ownership",
        )
    }

    layers = overrides.get("layers", ())
    defaults: dict = dict(
        product="acme",
        deployable="api",
        roots=ManifestRoots(
            deployable=root,
            app=app,
            config=config,
            scanner_roots=scanner_roots,
            kernel=kernel,
        ),
        layers=layers,
        domain_layers=overrides.pop("domain_layers", layers),
        domains={},
        scan_plugin_string_literals=True,
        kernel_barrel_packages=frozenset(kernel),
        seam_kernel_packages=frozenset(kernel),
    )
    defaults.update(overrides)
    return Manifest(**defaults)
