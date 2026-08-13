"""
Tinker Command for the Cara framework.

This module provides a CLI command to start the interactive Tinker shell with enhanced UX.
"""

from __future__ import annotations

import builtins
import importlib.util
import traceback

from cara.commands.CommandBase import CommandBase
from cara.decorators import command
from cara.tinker import Repl, ScriptRunner, Shell

from . import _TinkerFeatures


@command(
    name="tinker",
    help="Start the interactive Tinker shell for Laravel-style development.",
    options=[
        {
            "name": "--no-ipython",
            "help": "Use basic Python shell instead of IPython",
            "is_flag": True,
        },
        {
            "name": "--include",
            "help": "Comma-separated list of additional modules to include",
            "type": str,
        },
        {
            "name": "--execute",
            "help": "Execute a single command and exit",
            "type": str,
        },
        {
            "name": "--file",
            "help": "Execute commands from a Python file",
            "type": str,
        },
        {
            "name": "--verbose",
            "help": "Show verbose output during execution",
            "is_flag": True,
        },
        {
            "name": "--quiet",
            "help": "Minimal output mode",
            "is_flag": True,
        },
    ],
)
class TinkerCommand(CommandBase):
    """Interactive Tinker shell command with enhanced options."""

    def handle(
        self,
        include: str | None = None,
        execute: str | None = None,
        file: str | None = None,
    ):
        """Handle Tinker shell startup with enhanced options."""
        self.info("🔧 Starting Cara Tinker...")

        # ``cara.tinker`` is an optional install — probe for it
        # without importing the symbols (Repl / ScriptRunner / Shell
        # are instantiated inside the per-mode helpers below, so a
        # top-level ``from cara.tinker import ...`` here would import
        # the symbols but never use them). ``find_spec`` answers the
        # availability question without the unused-import noise.

        if importlib.util.find_spec("cara.tinker") is None:
            self.error("❌ Tinker not available: cara.tinker is not installed")
            self.error("💡 Make sure Tinker package is properly installed")
            return

        try:
            # Handle different execution modes
            if execute:
                self._execute_single_command(execute)
            elif file:
                self._execute_file(file)
            else:
                self._start_interactive_shell(include)
        except Exception as e:
            self.error(f"❌ Tinker error: {e}")
            if self.option("verbose"):
                self.error(f"Stack trace: {traceback.format_exc()}")

    def _start_interactive_shell(self, include: str | None = None):
        """Start interactive Tinker shell."""

        # Show startup message
        if not self.option("quiet"):
            self._show_startup_banner()

        # Create shell
        shell = Shell()

        # Add enhanced features (always enabled)
        self._add_enhanced_features(shell)

        # Include additional modules
        if include:
            self._include_modules(include.split(","), shell)

        # Configure shell options
        use_ipython = not self.option("no_ipython")

        self.info("🚀 Starting interactive shell...")
        if use_ipython:
            self.info("💡 Using IPython for enhanced experience")
        else:
            self.info("💡 Using basic Python shell")

        self.info("✨ Enhanced development features enabled")

        # Start shell
        try:
            shell.start(use_ipython=use_ipython)
        except KeyboardInterrupt:
            self.info("\n👋 Tinker session ended")
        except Exception as e:
            self.error(f"❌ Shell error: {e}")

    def _execute_single_command(self, command: str):
        """Execute a single command and exit."""

        if self.option("verbose"):
            self.info(f"🔧 Executing: {command}")

        # Create shell and REPL
        shell = Shell()

        # Add enhanced features (always enabled)
        self._add_enhanced_features(shell)

        repl = Repl(shell.namespace)

        try:
            # Execute command
            result = repl.execute(command)

            # Show result
            if result is not None:
                if self.option("verbose"):
                    self.info("📋 Result:")
                repl.format_result(result)

            self.success("✅ Command executed successfully")

        except Exception as e:
            self.error(f"❌ Command execution failed: {e}")

    def _execute_file(self, file_path: str):
        """Execute commands from a file."""

        if self.option("verbose"):
            self.info(f"📁 Executing file: {file_path}")

        # Create shell and script runner
        shell = Shell()

        # Add enhanced features (always enabled)
        self._add_enhanced_features(shell)

        runner = ScriptRunner(shell)

        try:
            # Execute file
            show_progress = not self.option("quiet")
            results = runner.run_file(file_path, show_progress=show_progress)

            # Show summary
            successful = sum(1 for r in results if r.get("success", False))
            total = len(results)

            if successful == total:
                self.success(
                    f"✅ File executed successfully ({successful}/{total} commands)"
                )
            else:
                failed = total - successful
                self.warning(
                    f"⚠️  File executed with errors ({successful}/{total} successful, {failed} failed)"
                )

        except Exception as e:
            self.error(f"❌ File execution failed: {e}")

    _add_enhanced_features = _TinkerFeatures._add_tinker_features

    def _show_startup_banner(self):
        """Show startup banner."""
        self.info("🔧 Cara Tinker - Interactive Shell")
        self.info("Laravel-style development environment for Cara framework")
        self.info("")

        self.info("✨ Enhanced Development Features:")
        self.info("  🏗️  Application: app_info(), db_info(), routes_count()")
        self.info("  🗄️  Database: query('users', 10), model_stats()")
        self.info("  💾 Cache: clear_cache(), test_cache()")
        self.info("  🔧 Config: show_config('app.name')")
        self.info("  📋 Development: logs(20), benchmark(func), craft('migrate:status')")
        self.info("  📧 Mail: test_mail(), send_test_mail(...)")
        self.info("  ⚡ Queue: test_queue(), queue_test_job(...)")
        self.info("  🔔 Notification: test_notification(), send_test_notification(...)")
        self.info("  ⚡ Jobs: show_queue_jobs(10)")
        self.info("")

        self.info("Built-in helper functions:")
        self.info("  • info() - Show available functions and classes")
        self.info("  • info(obj) - Show object information")
        self.info("  • dump(obj) - Pretty print object")
        self.info("  • dd(obj) - Dump and die")
        self.info("  • clear() - Clear screen")
        self.info("  • exit() or quit() - Exit shell")
        self.info("")
        self.info("Rich utilities available:")
        self.info("  • console - Rich Console instance")
        self.info("  • print_table(headers, rows) - Create tables")
        self.info("  • print_panel(content, title) - Create panels")
        self.info("  • print_syntax(code) - Syntax highlighting")

        self.info("-" * 60)

    def _include_modules(self, modules: list[str], shell):
        """Include additional modules."""
        for module_name in modules:
            module_name = module_name.strip()
            if not module_name:
                continue

            try:
                # Import module
                module = __import__(module_name)

                # Add to namespace
                shell.namespace[module_name.split(".")[-1]] = module

                if self.option("verbose"):
                    self.success(f"✅ Included module: {module_name}")

            except ImportError as e:
                self.warning(f"⚠️  Could not import {module_name}: {e}")

    def _show_usage_tips(self):
        """Show usage tips."""
        self.info("\n💡 Usage Tips:")
        self.info("   • Use tab completion for auto-complete")
        self.info("   • Use ? after any object for help")
        self.info("   • Use %magic commands in IPython mode")
        self.info("   • Access application: app()")
        self.info("   • Resolve services: resolve('service_name')")
        self.info("   • Get config: config('key.name')")

        self.info("   • Quick model queries: query('users', 10), model_stats()")
        self.info("   • Application info: app_info(), db_info()")
        self.info("   • Performance testing: benchmark(lambda: YourModel.all())")
        self.info("   • Run commands: craft('routes:list')")
        self.info("   • Mail testing: test_mail(), send_test_mail('user@example.com')")
        self.info("   • Queue testing: test_queue(), queue_test_job('MyJob')")
        self.info("   • Notifications: test_notification(), send_test_notification(1)")

    def _resolve_user_model(self):
        """
        Resolve User model from container (dependency injection).

        App must register User model in ApplicationProvider:
        self.application.bind("User", User)
        """

        if hasattr(builtins, "app"):
            app_instance = builtins.app()
            if app_instance and app_instance.has("User"):
                return app_instance.make("User")
        return None
