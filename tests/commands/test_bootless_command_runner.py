"""Bootless commands run through Cara's CLI parser without an application."""

from __future__ import annotations

import inspect
import sys
from pathlib import Path

import pytest

from cara.commands.BootlessCommandSpec import (
    BootlessCommandSpec,
    dispatch_architecture,
    dispatch_bootless,
)
from cara.commands.CommandRunner import CommandRunner


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


def test_dispatches_decorator_wrapped_command_with_positional_instance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "Probe.py"
    path.write_text(
        "from cara.commands.CommandBase import CommandBase\n"
        "from cara.decorators import command\n\n"
        "@command('probe')\n"
        "class Probe(CommandBase):\n"
        "    name = 'probe'\n"
        "    async def handle(self, label: str = 'default'):\n"
        "        self.line(label)\n"
        "        return 0\n",
        encoding="utf-8",
    )
    specs = (BootlessCommandSpec("probe", path, "Probe"),)
    monkeypatch.setattr(sys, "argv", ["craft", "probe", "--label", "wrapped"])

    with pytest.raises(SystemExit) as exit_info:
        dispatch_bootless(["probe", "--label", "wrapped"], specs)

    assert exit_info.value.code == 0
    assert "wrapped" in capsys.readouterr().out


def test_non_matching_command_does_not_load_spec(tmp_path: Path) -> None:
    missing = tmp_path / "Missing.py"
    specs = (BootlessCommandSpec("probe", missing, "Probe"),)

    assert dispatch_bootless(["queue:work"], specs) is False


def test_non_architecture_command_skips_canonical_dispatch() -> None:
    assert dispatch_architecture(["queue:work"]) is False


def test_container_dependency_is_rejected_before_execution(tmp_path: Path) -> None:
    path = _command_module(
        tmp_path,
        "self, repository: object",
        "return 0",
    )
    specs = (BootlessCommandSpec("probe", path, "Probe"),)

    with pytest.raises(TypeError, match="container dependencies: repository"):
        dispatch_bootless(["probe"], specs)


def test_command_options_require_canonical_explicit_metadata() -> None:
    runner = CommandRunner.__new__(CommandRunner)

    assert runner._parse_decorator_options(
        [
            {
                "name": "-c|--count",
                "help": "Number of records",
                "type": int,
                "default": 10,
                "is_flag": False,
            },
            {
                "name": "--force",
                "help": "Run without prompting",
                "is_flag": True,
            },
        ]
    ) == [
        ("count", ["-c", "--count"], 10, "Number of records", int),
        ("force", ["--force"], False, "Run without prompting", bool),
    ]


@pytest.mark.parametrize(
    "metadata, error",
    [
        ({"--count=10": "Number of records"}, TypeError),
        (
            [{"name": "--count=10", "help": "Number of records", "type": int}],
            ValueError,
        ),
        ([{"name": "--count", "help": "Number of records", "type": "int"}], TypeError),
        ([{"name": "--force", "help": "Run", "default": False}], TypeError),
        (
            [
                {
                    "name": "--count",
                    "help": "Number of records",
                    "type": int,
                    "default": "10",
                }
            ],
            TypeError,
        ),
    ],
)
def test_command_options_reject_legacy_or_ambiguous_metadata(
    metadata: object,
    error: type[Exception],
) -> None:
    runner = CommandRunner.__new__(CommandRunner)

    with pytest.raises(error):
        runner._parse_decorator_options(metadata)  # type: ignore[arg-type]


def test_invalid_command_annotation_fails_registration() -> None:
    runner = CommandRunner.__new__(CommandRunner)

    def broken(value):
        return value

    broken.__annotations__ = {"value": "MissingCommandType"}

    with pytest.raises(NameError, match="MissingCommandType"):
        runner._split_handle_params(inspect.signature(broken), broken)
