"""
Command Base Class for the Cara framework.

This module provides the base class for all CLI commands in the application.
"""

from typing import Any

from rich.console import Console as RichConsole
from rich.progress import track
from rich.table import Table


class CommandBase:
    """
    Base for all commands: provides self.console and helpers like info, error, table, etc.
    Also stores parsed CLI options so self.option(name) works.
    """

    def __init__(self, application: Any = None):
        self.console = RichConsole()
        self.application = application
        # container for parsed options from Typer
        self._parsed_options: dict[str, Any] = {}
        super().__init__()

    def set_parsed_options(self, opts: dict[str, Any]):
        """
        Called by CommandRunner to pass CLI flags into this instance.
        """
        self._parsed_options = opts

    def option(self, name: str, default=None):
        """
        Return the parsed CLI option value by name, or default if not provided.
        """
        return self._parsed_options.get(name, default)

    def info(self, message: str):
        self.console.print(f"[#fff]{message}[/#fff]")

    def error(self, message: str, **kwargs):
        """
        Display an error message.
        Accepts **kwargs to be compatible with other loggers.
        """
        self.line(f"<error>{message}</error>")

    def warning(self, message: str):
        self.console.print(f"[#e5c07b]{message}[/#e5c07b]")

    def success(self, message: str):
        self.console.print(f"[#30e047][bold]✓ {message}[/bold][/#30e047]")

    def debug(self, message: str):
        self.console.print(f"[dim]{message}[/dim]")

    def line(self, message: str = ""):
        """Print a simple line without formatting."""
        self.console.print(message)

    def table(self, headers, rows):
        table = Table(show_header=True, header_style="bold #e5c07b")
        for h in headers:
            table.add_column(h)
        for row in rows:
            table.add_row(*[str(cell) for cell in row])
        self.console.print(table)

    def progress(self, items, description="Processing"):
        return track(items, description=description)
