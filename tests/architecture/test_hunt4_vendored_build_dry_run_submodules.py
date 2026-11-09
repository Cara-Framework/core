"""The §2 dry-run must prove more than the four barrels import.

Barrels of ``contracts``, ``gates`` and ``shared`` are thin, so a kernel
submodule that cannot import in the vendored tree leaves ``VendorDryRun`` green
and the image dead on boot.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import cara
from cara.architecture.VendorDryRun import VendorDryRun

from ._fixtures import make_manifest, write


def test_a_kernel_submodule_that_cannot_import_fails_the_dry_run(tmp_path) -> None:
    manifest = make_manifest(tmp_path)
    write(tmp_path / "app" / "__init__.py", "")
    write(
        tmp_path / "app" / "models" / "__init__.py",
        '"""Models barrel."""\n\n__all__: list[str] = []\n',
    )
    write(
        tmp_path / "commons" / "shared" / "runtime" / "Broken.py",
        "import a_package_the_image_does_not_ship\n",
    )
    write(tmp_path / "commons" / "shared" / "runtime" / "__init__.py", "")
    shutil.copytree(
        Path(cara.__file__).parent,
        tmp_path / "commons" / "cara" / "cara",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    result = VendorDryRun.run(manifest)

    assert not result.ok, str(result)
