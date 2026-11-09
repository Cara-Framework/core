"""A deep ``commons.models`` import of a non-barrel helper must survive vendoring.

``from commons.models.core.Thing import helper`` folds to ``from app.models
import helper``. When the barrel never re-exports ``helper`` the vendored module
is dead at import while the build reports success. Either outcome is sound:
fail the build, or leave a module that imports.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from cara.commands.core.VendorCommonsCommand import VendorCommonsCommand


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _make_tree(root: Path) -> None:
    _write(root / "app" / "__init__.py", "")
    _write(
        root / "app" / "models" / "__init__.py",
        'from commons.models import Thing\n\n__all__ = ["Thing"]\n',
    )
    for package in ("contracts", "gates", "shared"):
        _write(root / "app" / package / "__init__.py", "")
    commons = root / "commons"
    _write(commons / "models" / "__init__.py", "from .core.Thing import Thing\n")
    _write(commons / "models" / "core" / "__init__.py", "")
    _write(
        commons / "models" / "core" / "Thing.py",
        "class Thing:\n    pass\n\n\ndef helper():\n    return 'folded'\n",
    )
    for package in ("contracts", "gates"):
        _write(commons / package / "__init__.py", "")
    _write(commons / "shared" / "__init__.py", "")
    _write(
        commons / "shared" / "Uses.py",
        "from commons.models.core.Thing import helper\n\nVALUE = helper()\n",
    )


def test_a_folded_helper_either_fails_the_build_or_still_imports(
    tmp_path, monkeypatch
) -> None:
    _make_tree(tmp_path)
    monkeypatch.chdir(tmp_path)

    if VendorCommonsCommand(application=None).handle() != 0:
        return  # refusing the build is an honest answer

    probe = subprocess.run(
        [sys.executable, "-c", "import app.shared.Uses as u; print(u.VALUE)"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert probe.returncode == 0, probe.stderr[-1000:]
    assert probe.stdout.strip() == "folded"
