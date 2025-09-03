"""
Reloadable Mixin for the Cara framework.

This module provides a mixin class that adds reload functionality to commands.
"""

import signal
import time


class ReloadableMixin:
    """Mixin that adds reload functionality to commands."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shutdown_requested = False
        self._restart_params = []
        self._restart_kwargs = {}
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.shutdown_requested = True
        print(f"\n🛑 Received signal {signum}, initiating graceful shutdown...")

        # Force exit if called twice
        if hasattr(self, "_signal_count"):
            self._signal_count += 1
            if self._signal_count >= 2:
                print("🔥 Force exit!")
                import sys

                sys.exit(0)
        else:
            self._signal_count = 1

    def _store_restart_params(self, *args, **kwargs):
        """Store parameters needed for restart."""
        self._restart_params = args
        self._restart_kwargs = kwargs

    def _setup_file_watching(self):
        """Setup file watching for auto-reload using existing Command system."""
        self.info("🔄 Auto-reload enabled - watching for file changes...")

        # Import the existing Command class with file watching
        from cara.commands.Command import Command

        # Create a Command instance with watch=True
        self.command_watcher = Command(self.application, watch=True)

        # Override the reload method to restart the worker
        original_reload = self.command_watcher.reload

        def command_reload():
            # Allow commands to customize reload message
            if hasattr(self, "_get_reload_message"):
                self.info(self._get_reload_message())
            else:
                self.info("🔄 File change detected, restarting command...")

            self.shutdown_requested = True
            # Give command time to finish current operation gracefully
            time.sleep(0.5)
            # Restart the command loop instead of exiting
            self._restart_command()

        self.command_watcher.reload = command_reload

    def _restart_command(self):
        """Restart the command internally without exiting the process."""
        try:
            # Reset shutdown flag
            self.shutdown_requested = False

            self.info("🔄 Command restarted successfully")

            # Call the command's main loop again with stored parameters
            if hasattr(self, "_run_main_loop"):
                self._run_main_loop(*self._restart_params, **self._restart_kwargs)
            else:
                self.warning("⚠️  Command doesn't implement _run_main_loop method")

        except Exception as e:
            self.error(f"❌ Failed to restart command: {e}")
            self.shutdown_requested = True

    def _cleanup_watching(self):
        """Cleanup file watching resources."""
        if hasattr(self, "command_watcher") and self.command_watcher:
            try:
                self.command_watcher.shutdown()
            except Exception:
                pass

    def should_continue(self) -> bool:
        """Check if command should continue running."""
        return not self.shutdown_requested
