"""
Tinker Command - Laravel-style console command for Cara Tinker

This file contains the main tinker console command with Typer and Rich integration.
"""

from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

from .Repl import Repl
from .Shell import Shell


class Command:
    """Cara Tinker console command with Typer and Rich integration."""

    def __init__(self):
        """Initialize command with Rich console."""
        self.console = Console()
        self.app = typer.Typer(
            name="tinker",
            help="Laravel-style interactive shell for Cara framework",
            rich_markup_mode="rich",
        )
        self._setup_commands()

    def _setup_commands(self):
        """Setup Typer commands."""
        self.app.command()(self.interactive)
        self.app.command("exec")(self.execute_command)
        self.app.command("file")(self.execute_file)

    def interactive(
        self,
        no_ipython: bool = typer.Option(
            False, "--no-ipython", help="Use basic Python shell instead of IPython"
        ),
        include: Optional[List[str]] = typer.Option(
            None, "--include", help="Additional modules to include"
        ),
        verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
        quiet: bool = typer.Option(False, "--quiet", "-q", help="Quiet mode"),
    ):
        """Start interactive Tinker shell."""
        if not quiet:
            self._show_startup_banner()

        try:
            # Create shell
            shell = Shell()

            # Include additional modules
            if include:
                self._include_modules(include, shell, verbose)

            # Start shell
            use_ipython = not no_ipython
            shell.start(use_ipython=use_ipython)

        except KeyboardInterrupt:
            self.console.print("\n👋 [bold green]Cara Tinker interrupted[/bold green]")
        except Exception as e:
            self.console.print(f"❌ [bold red]Tinker error:[/bold red] {e}")
            if verbose:
                import traceback

                traceback.print_exc()
            raise typer.Exit(1)

    def execute_command(
        self,
        command: str = typer.Argument(..., help="Command to execute"),
        verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
        show_code: bool = typer.Option(
            False, "--show-code", help="Show code with syntax highlighting"
        ),
    ):
        """Execute a single command and exit."""
        if verbose:
            self.console.print(f"🔧 [bold blue]Executing:[/bold blue] {command}")

        if show_code:
            syntax = Syntax(command, "python", theme="monokai", line_numbers=False)
            self.console.print(
                Panel(syntax, title="Code to Execute", border_style="cyan")
            )

        try:
            # Create shell and REPL
            shell = Shell()
            repl = Repl(shell.namespace)

            # Execute command
            result = repl.execute(command)

            # Show result (Rich formatting handled by REPL)
            if result is not None:
                repl.format_result(result)

        except Exception as e:
            self.console.print(f"❌ [bold red]Command execution error:[/bold red] {e}")
            raise typer.Exit(1)

    def execute_file(
        self,
        file_path: str = typer.Argument(..., help="Path to Python file to execute"),
        verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
        show_errors: bool = typer.Option(
            True, "--show-errors/--hide-errors", help="Show detailed errors"
        ),
    ):
        """Execute commands from a Python file."""
        path = Path(file_path)

        if not path.exists():
            self.console.print(f"❌ [bold red]File not found:[/bold red] {file_path}")
            raise typer.Exit(1)

        if verbose:
            self.console.print(f"📁 [bold blue]Executing file:[/bold blue] {file_path}")

        try:
            # Create shell and REPL
            shell = Shell()
            repl = Repl(shell.namespace)

            # Read file content
            content = path.read_text(encoding="utf-8")

            if verbose:
                syntax = Syntax(content, "python", theme="monokai", line_numbers=True)
                self.console.print(
                    Panel(syntax, title=f"File: {file_path}", border_style="cyan")
                )

            # Execute line by line for better error reporting
            lines = content.splitlines()
            for line_num, line in enumerate(lines, 1):
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith("#"):
                    continue

                if verbose:
                    self.console.print(f"[dim]Line {line_num}:[/dim] {line}")

                try:
                    result = repl.execute(line)

                    # Show result
                    if result is not None:
                        if verbose:
                            self.console.print("[dim]Result:[/dim]")
                        repl.format_result(result)

                except Exception as e:
                    if show_errors:
                        self.console.print(
                            f"❌ [bold red]Error on line {line_num}:[/bold red] {e}"
                        )
                        if not verbose:
                            self.console.print(f"[dim]Line:[/dim] {line}")
                    continue

            self.console.print("✅ [bold green]File execution completed[/bold green]")

        except Exception as e:
            self.console.print(f"❌ [bold red]File execution error:[/bold red] {e}")
            raise typer.Exit(1)

    def _show_startup_banner(self):
        """Show beautiful startup banner."""
        banner = Panel.fit(
            "[bold blue]🔧 Cara Tinker[/bold blue]\n"
            "[green]Laravel-style interactive shell for Cara framework[/green]\n\n"
            "[cyan]Available commands:[/cyan]\n"
            "• [bold]tinker[/bold] - Start interactive shell\n"
            '• [bold]tinker exec "code"[/bold] - Execute single command\n'
            "• [bold]tinker file script.py[/bold] - Execute Python file\n"
            "• [bold]tinker --help[/bold] - Show help",
            title="Welcome to Cara Tinker",
            border_style="blue",
            padding=(1, 2),
        )
        self.console.print(banner)

    def _include_modules(self, modules: List[str], shell: Shell, verbose: bool = False):
        """Include additional modules with Rich feedback."""
        for module_name in modules:
            try:
                # Import module
                module = __import__(module_name)

                # Add to namespace
                shell.namespace[module_name.split(".")[-1]] = module

                if verbose:
                    self.console.print(
                        f"✅ [green]Included module:[/green] {module_name}"
                    )

            except ImportError as e:
                self.console.print(
                    f"⚠️  [yellow]Could not import {module_name}:[/yellow] {e}"
                )

    def run(self, args: Optional[List[str]] = None):
        """Run the Typer application."""
        try:
            self.app(args)
        except Exception as e:
            self.console.print(f"❌ [bold red]Tinker error:[/bold red] {e}")
            raise typer.Exit(1)


def create_tinker_command() -> Command:
    """Create tinker command."""
    return Command()


def main():
    """Main function for CLI entry point."""
    command = create_tinker_command()
    command.run()


if __name__ == "__main__":
    main()
