"""Manifest.load(): boot-free load of a product's architecture_manifest.py."""

from __future__ import annotations

import pytest

from cara.architecture.Manifest import Manifest

from ._fixtures import write


def test_load_reads_the_module_level_manifest(tmp_path):
    deployable = tmp_path / "api"
    (deployable / "app").mkdir(parents=True)
    manifest_file = write(
        deployable / "app" / "architecture_manifest.py",
        "from pathlib import Path\n\n"
        "from cara.architecture.Manifest import Manifest, ManifestRoots\n\n"
        "ROOT = Path(__file__).resolve().parents[1]\n\n"
        "MANIFEST = Manifest(\n"
        "    product='acme',\n"
        "    deployable='api',\n"
        "    roots=ManifestRoots(deployable=ROOT, app=ROOT / 'app'),\n"
        "    layers=('services',),\n"
        "    domain_layers=('services',),\n"
        "    domains={'catalog': 'Catalog domain.'},\n"
        "    scan_plugin_string_literals=True,\n"
        "    kernel_barrel_packages=frozenset(),\n"
        "    seam_kernel_packages=frozenset(),\n"
        ")\n",
    )
    loaded = Manifest.load(manifest_file)
    assert loaded.product == "acme"
    assert loaded.layers == ("services",)
    assert loaded.domains == {"catalog": "Catalog domain."}
    assert loaded.roots.app == deployable / "app"


def test_load_requires_a_manifest_binding(tmp_path):
    manifest_file = write(tmp_path / "architecture_manifest.py", "NOT_A_MANIFEST = 1\n")
    with pytest.raises(TypeError):
        Manifest.load(manifest_file)


def test_load_is_boot_free_no_app_config_required(tmp_path):
    """The manifest module must load with ZERO application config/secrets —
    it may only reference stdlib + cara.architecture."""
    manifest_file = write(
        tmp_path / "architecture_manifest.py",
        "from pathlib import Path\n\n"
        "from cara.architecture.Manifest import Manifest, ManifestRoots\n\n"
        "MANIFEST = Manifest(\n"
        "    product='acme', deployable='api',\n"
        "    roots=ManifestRoots(deployable=Path('.'), app=Path('./app')),\n"
        "    layers=(), domain_layers=(), domains={},\n"
        "    scan_plugin_string_literals=True,\n"
        "    kernel_barrel_packages=frozenset(),\n"
        "    seam_kernel_packages=frozenset(),\n"
        ")\n",
    )
    # No environment variables, no config/.env, no DB — must not raise.
    loaded = Manifest.load(manifest_file)
    assert loaded.product == "acme"
