"""Bootless commands run through Cara's CLI parser without an application."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from cara.commands.BootlessCommandRunner import (
    BootlessCommandSpec,
    dispatch_bootless,
)


def _command_module(tmp_path: Path, handle_signature: str, body: str) -> Path:
    path = tmp_path / "Probe.py"
    path.write_text(
        "from cara.commands.CommandBase import CommandBase\n\n"
        "class Probe(CommandBase):\n"
        "    name = 'probe'\n"
        f"    async def handle({handle_signature}):\n"
        f"        {body}\n",
        encoding="utf-8",
    )
    return path


def test_dispatches_pure_command_with_standard_cli_parsing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = _command_module(
        tmp_path,
        "self, label: str = 'default'",
        "self.line(label); return 0",
    )
    specs = (BootlessCommandSpec("probe", path, "Probe"),)
    monkeypatch.setattr(sys, "argv", ["craft", "probe", "--label", "ready"])

    with pytest.raises(SystemExit) as exit_info:
        dispatch_bootless(["probe", "--label", "ready"], specs)

    assert exit_info.value.code == 0
    assert "ready" in capsys.readouterr().out


def test_non_matching_command_does_not_load_spec(tmp_path: Path) -> None:
    missing = tmp_path / "Missing.py"
    specs = (BootlessCommandSpec("probe", missing, "Probe"),)

    assert dispatch_bootless(["queue:work"], specs) is False


def test_container_dependency_is_rejected_before_execution(tmp_path: Path) -> None:
    path = _command_module(
        tmp_path,
        "self, repository: object",
        "return 0",
    )
    specs = (BootlessCommandSpec("probe", path, "Probe"),)

    with pytest.raises(TypeError, match="container dependencies: repository"):
        dispatch_bootless(["probe"], specs)
