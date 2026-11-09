"""``walk_kernel_modules``: the check the dry-run and CI's built image share."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import cara

from ._fixtures import write

WALK = (
    "from cara.architecture.VendorDryRun import VendorDryRun as v;"
    "raise SystemExit(v.walk_kernel_modules())"
)


def _walk(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", WALK],
        cwd=root,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(Path(cara.__file__).parents[1]),
        },
    )


def _kernel(root: Path) -> None:
    write(root / "app" / "__init__.py", "")
    for package in ("models", "contracts", "gates", "shared"):
        write(root / "app" / package / "__init__.py", "")


def test_a_clean_tree_passes_and_counts_the_modules_it_reached(tmp_path) -> None:
    _kernel(tmp_path)
    write(tmp_path / "app" / "shared" / "runtime" / "__init__.py", "")
    write(tmp_path / "app" / "shared" / "runtime" / "Deep.py", "VALUE = 1\n")

    walked = _walk(tmp_path)

    assert walked.returncode == 0, walked.stdout + walked.stderr
    assert "6 of 6 kernel modules import" in walked.stdout


def test_a_dead_submodule_under_a_thin_barrel_fails_and_is_named(tmp_path) -> None:
    _kernel(tmp_path)
    write(
        tmp_path / "app" / "gates" / "persistence" / "Dead.py",
        "from app.models import a_name_the_barrel_never_had\n",
    )

    walked = _walk(tmp_path)

    assert walked.returncode == 1
    assert "app.gates.persistence.Dead: ImportError" in walked.stdout


def test_a_module_that_needs_a_booted_app_is_not_a_vendoring_failure(tmp_path) -> None:
    _kernel(tmp_path)
    write(tmp_path / "app" / "shared" / "NeedsBoot.py", "raise RuntimeError('no app')\n")

    walked = _walk(tmp_path)

    assert walked.returncode == 0
    assert "app.shared.NeedsBoot: not verified, raised RuntimeError" in walked.stdout
    assert "4 of 5 kernel modules import" in walked.stdout


def test_a_tree_with_no_kernel_modules_never_passes(tmp_path) -> None:
    """The wrong working directory must not read as "0 of 0, all clear"."""
    walked = _walk(tmp_path)

    assert walked.returncode == 1
    assert "no kernel modules under" in walked.stdout
