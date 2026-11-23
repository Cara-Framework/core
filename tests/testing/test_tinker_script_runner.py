"""``craft tinker`` scripts: a line that raised must be reported as failed.

``Repl.execute`` prints the error and returns ``None`` — what a successful
statement returns too. ``ScriptRunner`` called it and waited for an exception
that could not arrive, so every line was recorded ``success=True``: a script
whose every line raised ended with "Successful: N", and ``craft tinker
--file`` announced "File executed successfully (N/N commands)".

There is no tinker test directory; these live with the other toolkit tests.
"""

from __future__ import annotations

import io
from types import SimpleNamespace

import pytest
from rich.console import Console

from cara.tinker.Repl import Repl
from cara.tinker.ScriptRunner import ScriptRunner


def _runner() -> tuple[ScriptRunner, io.StringIO]:
    # A bare namespace stands in for ``Shell``: building the real one boots
    # facades and model discovery, none of which a script line needs here.
    runner = ScriptRunner(SimpleNamespace(namespace={}))
    output = io.StringIO()
    console = Console(file=output, width=120, color_system=None)
    runner.console = console
    runner.repl.console = console
    return runner, output


def test_a_line_that_raises_is_recorded_as_failed() -> None:
    runner, output = _runner()
    results = runner.run_script(
        "x = 20\nundefined_name\ny = x + 1\n1 / 0\n", show_progress=False
    )
    assert [result["success"] for result in results] == [True, False, True, False]
    assert "undefined_name" in results[1]["error"]
    assert results[3]["error"] == "division by zero"
    assert runner.repl.namespace["y"] == 21  # the lines around a failure still ran
    printed = output.getvalue()
    assert "Successful: 2" in printed
    assert "Failed: 2" in printed
    assert "Failed Commands" in printed


def test_a_clean_script_reports_no_failure() -> None:
    runner, output = _runner()
    results = runner.run_script("a = 2\na * 21\n", show_progress=False)
    assert [(r["success"], r["result"]) for r in results] == [(True, None), (True, 42)]
    assert "Failed" not in output.getvalue()


def test_evaluate_raises_where_execute_reports_and_returns_none() -> None:
    repl = Repl({})
    repl.console = Console(file=io.StringIO(), color_system=None)
    with pytest.raises(ZeroDivisionError):
        repl.evaluate("1 / 0")
    assert repl.execute("1 / 0") is None
    assert repl.evaluate("6 * 7") == 42
    assert repl.evaluate("z = 5") is None
    assert repl.namespace["z"] == 5


def test_the_error_panel_shows_the_code_and_survives_markup_in_the_message() -> None:
    """The message was printed as rich MARKUP and the code as a formatted
    ``Syntax`` object: ``[/tag]`` in an error raised ``MarkupError`` out of
    ``execute``, and the "Code:" section read ``<rich.syntax.Syntax object``."""
    repl = Repl({})
    output = io.StringIO()
    repl.console = Console(file=output, width=120, color_system=None)

    assert repl.execute("raise ValueError('bad [/tag] and [red]key')") is None

    printed = output.getvalue()
    assert "ValueError: bad [/tag] and [red]key" in printed
    assert "raise ValueError(" in printed
    assert "rich.syntax.Syntax object" not in printed


def test_the_failed_commands_table_prints_the_command_as_written() -> None:
    runner, output = _runner()
    results = runner.run_script("rows[i]\nvalue = '[/x]' + 1\n", show_progress=False)
    assert [result["success"] for result in results] == [False, False]
    printed = output.getvalue()
    assert "rows[i]" in printed
    assert "value = '[/x]' + 1" in printed
