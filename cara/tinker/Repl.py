"""
Repl - Read-Eval-Print Loop for Cara Tinker

This file provides REPL functionality for executing commands in the Cara framework with Rich integration.
"""

from __future__ import annotations

import ast
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax


class Repl:
    """Read-Eval-Print Loop for Cara Tinker with Rich integration."""

    def __init__(self, namespace: dict[str, Any]):
        """Initialize REPL with given namespace and Rich console."""
        self.namespace = namespace
        self.console = Console()

    def execute(self, code: str) -> Any:
        """Execute code and return result."""
        if not code.strip():
            return None

        try:
            # Try to parse as expression first
            try:
                # Parse as expression
                parsed = ast.parse(code, mode="eval")
                return eval(compile(parsed, "<tinker>", "eval"), self.namespace)
            except SyntaxError:
                # If it's not an expression, try as statement
                parsed = ast.parse(code, mode="exec")
                exec(compile(parsed, "<tinker>", "exec"), self.namespace)
                return None

        except Exception as e:
            # Print error but don't crash
            self.print_error(e, code)
            return None

    def print_error(self, error: Exception, code: str | None = None):
        """Print error in a beautiful Rich format."""
        error_type = type(error).__name__
        error_message = str(error)

        error_content = f"[bold red]{error_type}:[/bold red] {error_message}"

        if code:
            # Show the problematic code with syntax highlighting
            syntax = Syntax(code, "python", theme="monokai", line_numbers=False)
            error_panel = Panel(
                f"{error_content}\n\n[dim]Code:[/dim]\n{syntax}",
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
