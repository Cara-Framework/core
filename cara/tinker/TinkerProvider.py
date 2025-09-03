"""
Tinker Provider for the Cara framework.

This module provides the deferred service provider that configures and registers the tinker
subsystem, including shell, REPL, and command functionality.
"""

from cara.configuration import config
from cara.foundation import DeferredProvider

from .Command import Command
from .Repl import Repl
from .ScriptRunner import ScriptRunner
from .Shell import Shell


class TinkerProvider(DeferredProvider):
    """
    Deferred provider for the tinker subsystem.

    Registers the Tinker shell, REPL, and command services.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return [
            "tinker",
            "tinker.shell",
            "tinker.repl",
            "tinker.command",
            "tinker.script_runner",
        ]

    def register(self) -> None:
        """Register tinker services with the container."""
        # Register Shell as singleton
        shell = Shell()
        self.application.bind("tinker.shell", shell)
        self.application.bind("tinker", shell)  # Main tinker service
        self.application.bind(Shell, shell)

        # Register REPL
        repl = Repl(shell.namespace)
        self.application.bind("tinker.repl", repl)
        self.application.bind(Repl, repl)

        # Register Command
        command = Command()
        self.application.bind("tinker.command", command)
        self.application.bind(Command, command)

        # Register ScriptRunner
        script_runner = ScriptRunner(shell)
        self.application.bind("tinker.script_runner", script_runner)
        self.application.bind(ScriptRunner, script_runner)

    def boot(self) -> None:
        """Bootstrap tinker services."""
        # Get configuration for tinker
        tinker_config = config("tinker", {})

        # Configure shell if needed
        if "auto_imports" in tinker_config:
            shell = self.application.make("tinker.shell")
            auto_imports = tinker_config["auto_imports"]

            for module_name, classes in auto_imports.items():
                try:
                    module = __import__(module_name, fromlist=classes)
                    for class_name in classes:
                        try:
                            cls = getattr(module, class_name)
                            shell.add_to_namespace(class_name, cls)
                        except AttributeError:
                            pass
                except ImportError:
                    pass

        # Set up any additional tinker configuration
        if "include_helpers" in tinker_config and tinker_config["include_helpers"]:
            shell = self.application.make("tinker.shell")
            self._add_helper_functions(shell)

        # Add Rich and Typer utilities to shell namespace
        self._add_rich_utilities()

    def _add_helper_functions(self, shell: Shell):
        """Add additional helper functions to shell."""

        def app():
            """Get application instance."""
            return self.application

        def resolve(service_name: str):
            """Resolve service from container."""
            return self.application.make(service_name)

        def config_get(key: str, default=None):
            """Get configuration value."""
            return config(key, default)

        # Add helpers to namespace
        shell.add_to_namespace("app", app)
        shell.add_to_namespace("resolve", resolve)
        shell.add_to_namespace("config", config_get)

    def _add_rich_utilities(self):
        """Add Rich and Typer utilities to tinker shell."""
        try:
            shell = self.application.make("tinker.shell")

            # Import Rich components
            from rich.console import Console
            from rich.panel import Panel
            from rich.progress import Progress
            from rich.syntax import Syntax
            from rich.table import Table

            # Add Rich utilities to namespace
            shell.add_to_namespace("Console", Console)
            shell.add_to_namespace("Table", Table)
            shell.add_to_namespace("Panel", Panel)
            shell.add_to_namespace("Syntax", Syntax)
            shell.add_to_namespace("Progress", Progress)

            # Add a pre-configured console instance
            console = Console()
            shell.add_to_namespace("console", console)

            # Add helper functions for Rich
            def print_table(headers, rows, title=None):
                """Helper to quickly create and print a table."""
                table = Table(title=title, show_header=True, header_style="bold magenta")
                for header in headers:
                    table.add_column(header)
                for row in rows:
                    table.add_row(*[str(cell) for cell in row])
                console.print(table)

            def print_panel(content, title=None, style="blue"):
                """Helper to quickly create and print a panel."""
                panel = Panel(content, title=title, border_style=style)
                console.print(panel)

            def print_syntax(code, language="python", theme="monokai"):
                """Helper to print code with syntax highlighting."""
                syntax = Syntax(code, language, theme=theme, line_numbers=True)
                console.print(syntax)

            shell.add_to_namespace("print_table", print_table)
            shell.add_to_namespace("print_panel", print_panel)
            shell.add_to_namespace("print_syntax", print_syntax)

        except ImportError:
            # Rich not available, skip
            pass
        except Exception:
            # Shell not available yet, skip
            pass
