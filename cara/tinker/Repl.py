"""
Repl - Read-Eval-Print Loop for Cara Tinker

This file provides REPL functionality for executing commands in the Cara framework with Rich integration.
"""

import ast
from typing import Any, Dict

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax


class Repl:
    """Read-Eval-Print Loop for Cara Tinker with Rich integration."""

    def __init__(self, namespace: Dict[str, Any]):
        """Initialize REPL with given namespace and Rich console."""
        self.namespace = namespace
        self.history = []
        self.last_result = None
        self.console = Console()

    def execute(self, code: str) -> Any:
        """Execute code and return result."""
        if not code.strip():
            return None

        # Add to history
        self.history.append(code)

        try:
            # Try to parse as expression first
            try:
                # Parse as expression
                parsed = ast.parse(code, mode="eval")
                result = eval(compile(parsed, "<tinker>", "eval"), self.namespace)
                self.last_result = result
                return result
            except SyntaxError:
                # If it's not an expression, try as statement
                parsed = ast.parse(code, mode="exec")
                exec(compile(parsed, "<tinker>", "exec"), self.namespace)
                return None

        except Exception as e:
            # Print error but don't crash
            self.print_error(e, code)
            return None

    def print_error(self, error: Exception, code: str = None):
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

    def get_history(self) -> list:
        """Get command history."""
        return self.history.copy()

    def clear_history(self):
        """Clear command history."""
        self.history.clear()

    def get_last_result(self) -> Any:
        """Get last execution result."""
        return self.last_result

    def add_to_namespace(self, name: str, value: Any):
        """Add variable to namespace."""
        self.namespace[name] = value

    def get_namespace(self) -> Dict[str, Any]:
        """Get current namespace."""
        return self.namespace.copy()

    def run_interactive(self):
        """Run interactive REPL session with Rich."""
        self.console.print(
            Panel.fit(
                "[bold blue]Cara Tinker REPL[/bold blue]\n"
                "[green]Type 'exit()' or 'quit()' to exit[/green]",
                border_style="blue",
            )
        )

        while True:
            try:
                # Get input with Rich prompt
                code = input(">>> ")

                # Check for exit commands
                if code.strip() in ["exit()", "quit()", "exit", "quit"]:
                    break

                # Execute code
                result = self.execute(code)

                # Display result if not None (Rich handles the formatting)
                if result is not None:
                    self.format_result(result)

            except KeyboardInterrupt:
                self.console.print("\n[yellow]KeyboardInterrupt[/yellow]")
                continue
            except EOFError:
                self.console.print("\n[green]Exiting...[/green]")
                break

    def evaluate_expression(self, expression: str) -> Any:
        """Evaluate a single expression."""
        try:
            parsed = ast.parse(expression, mode="eval")
            return eval(compile(parsed, "<tinker>", "eval"), self.namespace)
        except Exception as e:
            self.print_error(e, expression)
            return None

    def execute_statement(self, statement: str) -> bool:
        """Execute a statement."""
        try:
            parsed = ast.parse(statement, mode="exec")
            exec(compile(parsed, "<tinker>", "exec"), self.namespace)
            return True
        except Exception as e:
            self.print_error(e, statement)
            return False

    def show_code_with_syntax(self, code: str, title: str = "Code"):
        """Show code with syntax highlighting."""
        syntax = Syntax(code, "python", theme="monokai", line_numbers=True)
        self.console.print(Panel(syntax, title=title, border_style="cyan"))
