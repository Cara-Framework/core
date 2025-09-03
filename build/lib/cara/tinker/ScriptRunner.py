"""
Tinker Script Runner - Script execution functionality for Cara Tinker

This file contains the script runner for executing Tinker scripts with Rich integration.
"""

from pathlib import Path
from typing import Any, Dict

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax
from rich.table import Table

from .Repl import Repl
from .Shell import Shell


class ScriptRunner:
    """Tinker script runner with Rich integration."""

    def __init__(self, shell: Shell = None):
        """Initialize script runner with Rich console."""
        self.shell = shell or Shell()
        self.repl = Repl(self.shell.namespace)
        self.console = Console()

    def run_script(
        self,
        script_content: str,
        context: Dict[str, Any] = None,
        show_progress: bool = True,
    ):
        """Run script content with Rich progress tracking."""
        if context:
            self.repl.namespace.update(context)

        lines = [
            line.strip()
            for line in script_content.splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        results = []

        if show_progress and len(lines) > 1:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
            ) as progress:
                task = progress.add_task("Executing script...", total=len(lines))

                for line_num, line in enumerate(lines, 1):
                    progress.update(
                        task, description=f"Executing line {line_num}/{len(lines)}"
                    )
                    result = self._execute_line(line, line_num)
                    results.append(result)
                    progress.advance(task)
        else:
            for line_num, line in enumerate(lines, 1):
                result = self._execute_line(line, line_num)
                results.append(result)

        self._show_results_summary(results)
        return results

    def run_file(
        self, file_path: str, context: Dict[str, Any] = None, show_progress: bool = True
    ):
        """Run script from file with Rich formatting."""
        path = Path(file_path)
        if not path.exists():
            self.console.print(
                f"❌ [bold red]Script file not found:[/bold red] {file_path}"
            )
            raise FileNotFoundError(f"Script file not found: {file_path}")

        self.console.print(f"📁 [bold blue]Running script:[/bold blue] {file_path}")

        content = path.read_text(encoding="utf-8")

        # Show file content with syntax highlighting
        syntax = Syntax(content, "python", theme="monokai", line_numbers=True)
        self.console.print(
            Panel(syntax, title=f"Script: {path.name}", border_style="cyan")
        )

        return self.run_script(content, context, show_progress)

    def _execute_line(self, line: str, line_num: int) -> Dict[str, Any]:
        """Execute a single line and return result info."""
        try:
            result = self.repl.execute(line)
            return {
                "line": line_num,
                "command": line,
                "result": result,
                "success": True,
                "error": None,
            }
        except Exception as e:
            return {
                "line": line_num,
                "command": line,
                "result": None,
                "success": False,
                "error": str(e),
            }

    def _show_results_summary(self, results: list):
        """Show execution results summary with Rich table."""
        if not results:
            return

        successful = sum(1 for r in results if r["success"])
        failed = len(results) - successful

        # Summary panel
        summary_text = f"[green]✅ Successful: {successful}[/green]"
        if failed > 0:
            summary_text += f"\n[red]❌ Failed: {failed}[/red]"

        self.console.print(
            Panel(summary_text, title="📊 Execution Summary", border_style="blue")
        )

        # Show failed commands if any
        if failed > 0:
            self._show_failed_commands(results)

    def _show_failed_commands(self, results: list):
        """Show failed commands in a table."""
        failed_results = [r for r in results if not r["success"]]

        if not failed_results:
            return

        table = Table(
            title="❌ Failed Commands", show_header=True, header_style="bold red"
        )
        table.add_column("Line", style="cyan", no_wrap=True)
        table.add_column("Command", style="yellow")
        table.add_column("Error", style="red")

        for result in failed_results:
            table.add_row(
                str(result["line"]),
                result["command"][:50] + "..."
                if len(result["command"]) > 50
                else result["command"],
                result["error"][:100] + "..."
                if len(result["error"]) > 100
                else result["error"],
            )

        self.console.print(table)

    def run_interactive_script(self, script_content: str):
        """Run script with interactive confirmation for each line."""
        lines = [
            line.strip()
            for line in script_content.splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        results = []

        self.console.print(
            Panel(
                "[bold blue]Interactive Script Execution[/bold blue]\n"
                "[green]You will be prompted before each command execution[/green]",
                border_style="blue",
            )
        )

        for line_num, line in enumerate(lines, 1):
            # Show the command with syntax highlighting
            syntax = Syntax(line, "python", theme="monokai", line_numbers=False)
            self.console.print(
                Panel(syntax, title=f"Line {line_num}/{len(lines)}", border_style="cyan")
            )

            # Ask for confirmation
            response = input("Execute this command? [Y/n/q]: ").strip().lower()

            if response == "q":
                self.console.print("[yellow]Script execution cancelled by user[/yellow]")
                break
            elif response in ["n", "no"]:
                self.console.print("[dim]Skipped[/dim]")
                results.append(
                    {
                        "line": line_num,
                        "command": line,
                        "result": None,
                        "success": True,
                        "error": None,
                        "skipped": True,
                    }
                )
                continue

            # Execute the command
            result = self._execute_line(line, line_num)
            results.append(result)

            # Show result
            if result["success"] and result["result"] is not None:
                self.repl.format_result(result["result"])
            elif not result["success"]:
                self.console.print(f"❌ [red]Error:[/red] {result['error']}")

        self._show_results_summary(results)
        return results
