"""``craft tinker --execute`` / ``--file`` must exit non-zero when the code fails.

``_execute_single_command`` ran the code through ``Repl.execute``, which
prints the error and answers ``None`` — the answer a successful statement
gives. Its ``except`` therefore never ran: ``craft tinker --execute "1/0"``
printed "Command executed successfully" and exited 0, so a deploy step or a
script chained on ``&&`` marched on past a failed one-liner. ``--file`` had
the same shape: it counted the failed lines, warned, and still exited 0.
"""

from __future__ import annotations

from cara.commands.core.TinkerCommand import TinkerCommand
from tests.commands._fixtures import make_command


def _command() -> TinkerCommand:
    # The enhanced namespace needs a booted application; the exit code does not.
    return make_command(
        TinkerCommand,
        options={"quiet": True, "verbose": False},
        _add_enhanced_features=lambda shell: None,
    )


def _printed(command) -> str:
    return "\n".join(
        str(call.args[0]) for call in command.console.print.call_args_list if call.args
    )


def test_a_failing_one_liner_exits_non_zero():
    cmd = _command()

    assert cmd.handle(execute="1/0") == 1

    printed = _printed(cmd)
    assert "Command execution failed" in printed
    assert "executed successfully" not in printed


def test_a_one_liner_that_is_not_python_exits_non_zero():
    cmd = _command()

    assert cmd.handle(execute="this is not python") == 1


def test_a_working_one_liner_still_exits_zero():
    cmd = _command()

    assert cmd.handle(execute="total = 1 + 1") is None
    assert "executed successfully" in _printed(cmd)


def test_a_script_with_a_failing_line_exits_non_zero(tmp_path):
    script = tmp_path / "script.py"
    script.write_text("value = 1\n1/0\n", encoding="utf-8")
    cmd = _command()

    assert cmd.handle(file=str(script)) == 1


def test_a_clean_script_still_exits_zero(tmp_path):
    script = tmp_path / "script.py"
    script.write_text("value = 1\nother = value + 1\n", encoding="utf-8")
    cmd = _command()

    assert cmd.handle(file=str(script)) is None


def test_a_missing_script_exits_non_zero(tmp_path):
    cmd = _command()

    assert cmd.handle(file=str(tmp_path / "absent.py")) == 1
