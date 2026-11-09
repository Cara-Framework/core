"""How ``build:vendor-commons`` folds ``commons.models`` import statements.

Every name resolves on its own: the barrel when it holds that definition, the
flat module otherwise, and a build failure naming the file when neither does.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from cara.commands.core.VendorCommonsCommand import VendorCommonsCommand


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _tree(root: Path, shared_module: str) -> None:
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
        "class Thing:\n    pass\n\n\ndef helper():\n    return 'folded'\n\n\n"
        "SHARED_NAME = 'thing'\n",
    )
    _write(commons / "models" / "core" / "Other.py", "SHARED_NAME = 'other'\n")
    for package in ("contracts", "gates"):
        _write(commons / package / "__init__.py", "")
    _write(commons / "shared" / "__init__.py", "")
    _write(commons / "shared" / "Uses.py", shared_module)


def _vendor(root: Path, monkeypatch) -> int:
    monkeypatch.chdir(root)
    return VendorCommonsCommand(application=None).handle()


def _evaluate(root: Path, expression: str) -> str:
    probe = subprocess.run(
        [sys.executable, "-c", f"import app.shared.Uses as u; print({expression})"],
        cwd=root,
        capture_output=True,
        text=True,
    )
    assert probe.returncode == 0, probe.stderr[-1000:]
    return probe.stdout.strip()


def test_one_statement_can_mix_a_barrel_model_and_a_private_helper(
    tmp_path, monkeypatch
) -> None:
    _tree(
        tmp_path,
        "def read():\n"
        "    from commons.models.core.Thing import (  # a comment inside\n"
        "        Thing,\n"
        "        helper as read_helper,\n"
        "    )\n"
        "    return Thing.__name__, read_helper()\n",
    )

    assert _vendor(tmp_path, monkeypatch) == 0

    assert _evaluate(tmp_path, "u.read()") == "('Thing', 'folded')"
    assert "from app.models import Thing" in (tmp_path / "app/shared/Uses.py").read_text()


def test_a_name_lands_on_the_module_the_statement_named(tmp_path, monkeypatch) -> None:
    """Two model modules may define one public name; the barrel map's last
    definition is not the one this statement asked for."""
    _tree(
        tmp_path,
        "from commons.models.core.Other import SHARED_NAME as OTHER\n"
        "from commons.models.core.Thing import SHARED_NAME as THING\n",
    )

    assert _vendor(tmp_path, monkeypatch) == 0

    assert _evaluate(tmp_path, "(u.OTHER, u.THING)") == "('other', 'thing')"


def test_a_name_nothing_defines_fails_the_build_and_says_where(
    tmp_path, monkeypatch, capsys
) -> None:
    _tree(tmp_path, "from commons.models.core.Thing import missing_helper\n")

    assert _vendor(tmp_path, monkeypatch) == 1

    output = capsys.readouterr()
    reported = output.out + output.err
    assert "missing_helper" in reported
    assert "app/shared/Uses.py:1" in reported
    assert (tmp_path / "commons").exists(), "a refused build must not drop the kernel"


def test_an_example_in_prose_is_rewritten_but_never_fails_the_build(
    tmp_path, monkeypatch
) -> None:
    _tree(
        tmp_path,
        '"""Usage: from commons.models.core.Thing import documented_only."""\n\nVALUE = 1\n',
    )

    assert _vendor(tmp_path, monkeypatch) == 0

    assert "commons.models" not in (tmp_path / "app/shared/Uses.py").read_text()


def test_the_rewrite_survives_non_ascii_text_before_the_statement(
    tmp_path, monkeypatch
) -> None:
    _tree(
        tmp_path,
        "LABEL = 'şğü'; from commons.models.core.Thing import helper\nAFTER = helper()\n",
    )

    assert _vendor(tmp_path, monkeypatch) == 0

    assert _evaluate(tmp_path, "(u.LABEL, u.AFTER)") == "('şğü', 'folded')"
