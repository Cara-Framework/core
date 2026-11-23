"""
Repl - Read-Eval-Print Loop for Cara Tinker

This file provides REPL functionality for executing commands in the Cara framework with Rich integration.
"""

from __future__ import annotations

import ast
from typing import Any

from rich.console import Console, Group
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text


class Repl:
    """Read-Eval-Print Loop for Cara Tinker with Rich integration."""

    def __init__(self, namespace: dict[str, Any]):
        """Initialize REPL with given namespace and Rich console."""
        self.namespace = namespace
        self.console = Console()

    def evaluate(self, code: str) -> Any:
        """Execute code and return its result; a failure RAISES.

        The door for callers that must know whether the code worked.
        ``execute`` prints the error and answers ``None`` — the same answer a
        successful statement gives — so a caller reading only its return
        value counted every failed line as a success.
        """
        if not code.strip():
            return None

        # Try to parse as expression first
        try:
            parsed = ast.parse(code, mode="eval")
        except SyntaxError:
            # If it's not an expression, try as statement
            parsed = ast.parse(code, mode="exec")
            exec(compile(parsed, "<tinker>", "exec"), self.namespace)
            return None
        return eval(compile(parsed, "<tinker>", "eval"), self.namespace)

    def execute(self, code: str) -> Any:
        """Execute code and return result."""
        try:
            return self.evaluate(code)
        except Exception as e:
            # Print error but don't crash
            self.print_error(e, code)
            return None

    def print_error(self, error: Exception, code: str | None = None):
        """Print error in a beautiful Rich format."""
        # ``Text``, never a markup string: the message is the user's data. As
        # markup, ``[red]`` inside it was swallowed and a ``[/tag]`` raised
        # ``MarkupError`` out of the one method whose job is to report an
        # error without crashing.
        error_content = Text()
        error_content.append(f"{type(error).__name__}:", style="bold red")
        error_content.append(f" {error}")

        if code:
            # Show the problematic code with syntax highlighting. The Syntax
            # is a renderable of its own: formatted into the string it
            # printed as ``<rich.syntax.Syntax object at 0x...>``.
            syntax = Syntax(code, "python", theme="monokai", line_numbers=False)
            error_panel = Panel(
                Group(error_content, Text(""), Text("Code:", style="dim"), syntax),
                title="❌ Execution Error",
                border_style="red",
                padding=(1, 2),
            )
        else:
            error_panel = Panel(
                error_content, title="❌ Error", border_style="red", padding=(1, 2)
            )

        self.console.print(error_panel)

    def format_result(self, result: Any) -> str:
        """Format result for display with Rich."""
        if result is None:
            return ""

        # Use Rich to format the result beautifully
        self.console.print(result, style="bold green")
        return ""  # Rich already printed it

    def add_to_namespace(self, name: str, value: Any):
        """Add variable to namespace."""
        self.namespace[name] = value

    def get_namespace(self) -> dict[str, Any]:
        """Get current namespace."""
        return self.namespace.copy()
