"""Operator-facing output must not hand out another framework's commands.

``cache:clear`` used to close with three ``php artisan`` invocations — for a
view cache, a route cache and a config cache that Cara does not have. They
were Laravel scaffolding nobody deleted. An operator who runs one, watches it
fail, and works out that the tool suggested a command from a different
language has learned to distrust the rest of the output too.

Docstrings are exempt on purpose: "mirrors Laravel's ``optional()``" is
orientation for a developer reading the source, not an instruction to anyone
holding a terminal. The line that matters is the one printed.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

CARA_ROOT = pathlib.Path(__file__).resolve().parents[2] / "cara"

# Printed to a terminal by these; anything else is internal.
OUTPUT_CALLS = {"info", "line", "warn", "warning", "error", "comment", "echo", "print"}

FOREIGN = ("php artisan", "artisan ", "composer ", "npm run artisan")


def _string_literals_passed_to_output(tree: ast.AST):
    """Yield every literal that a command hands to an output call."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if name not in OUTPUT_CALLS:
            continue
        for arg in node.args:
            for piece in ast.walk(arg):
                if isinstance(piece, ast.Constant) and isinstance(piece.value, str):
                    yield piece.value, piece.lineno


def _python_sources():
    return sorted(CARA_ROOT.rglob("*.py"))


@pytest.mark.parametrize("path", _python_sources(), ids=lambda p: str(p.name))
def test_no_foreign_framework_command_is_printed(path: pathlib.Path):
    tree = ast.parse(path.read_text(), filename=str(path))
    offenders = [
        (line, text)
        for text, line in _string_literals_passed_to_output(tree)
        if any(marker in text.lower() for marker in FOREIGN)
    ]
    assert not offenders, (
        f"{path.relative_to(CARA_ROOT.parent)} prints a command from another "
        f"framework: {offenders}. Suggest a craft command that exists, or say "
        f"nothing."
    )
