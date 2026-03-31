"""
Command Base for the Cara framework.

This module provides the base Command class for defining CLI commands in the application.
"""

import importlib
import os
import time
from typing import Any

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .CommandLoader import CommandLoader
from .CommandRegistry import CommandRegistry
from .CommandRunner import CommandRunner


class SimpleReloadHandler(FileSystemEventHandler):
    """Simple file watcher for command reloading."""

    def __init__(self, command):
        self.command = command
        self.last_reload = 0
        self.debounce_delay = 1.0
        super().__init__()

    def on_modified(self, event):
        if event.is_directory or not event.src_path.endswith(".py"):
            return

        # Debouncing
        current_time = time.time()
        if current_time - self.last_reload < self.debounce_delay:
            return

        # Skip temp/cache files
        ignore_patterns = ["__pycache__", ".pyc", ".pyo", ".tmp", ".swp", ".DS_Store"]
        if any(pattern in event.src_path for pattern in ignore_patterns):
            return

        self.last_reload = current_time
        self.command.reload()


class Command:
    """
    Main orchestrator.

    Combines loader, registry, and runner. Handles command loading, registration, running, and hot-
    reloading.
    """

    def __init__(self, application: Any, watch: bool = False):
        self.application = application
        self.loader = self._create_loader()
        self.registry = self._get_registry()
        self.runner = self._create_runner()
        self._initialize_commands()
        self.observer = None
        if watch:
            self._start_watcher()

    def _create_loader(self) -> CommandLoader:
        return CommandLoader("cara.commands.core")

    def _get_registry(self):
        return CommandRegistry

    def _create_runner(self) -> CommandRunner:
        return CommandRunner(self.application)

    def _initialize_commands(self):
        self._load_commands()
        self._register_custom_commands()
        self._register_core_commands()

    def _load_commands(self):
        self.loader.load()

    def _register_custom_commands(self):
        for cmd_cls in self.loader.get_custom_commands():
            self.runner.register(cmd_cls)

    def _register_core_commands(self):
        for cmd_cls in self.registry.get():
            self.runner.register(cmd_cls)

    def _start_watcher(self):
        """Start file watcher for hot reloading during development."""
        if self.observer is not None:
            return  # Already watching

        self.observer = Observer()
        handler = SimpleReloadHandler(self)

        # Watch paths to monitor for changes
        watch_paths = self._get_watch_paths()

        for path in watch_paths:
            if path and os.path.isdir(path):
                self.observer.schedule(handler, path=path, recursive=True)

        if watch_paths:
            self.observer.daemon = True
            self.observer.start()

    def _get_watch_paths(self):
        """Get all paths that should be watched for changes."""
        from cara.support import paths

        watch_paths = []

        # 1. Core commands path
        core_path = self._get_core_commands_path()
        if core_path:
            watch_paths.append(core_path)

        # 2. Application directories to watch
        app_dirs = ["app", "config", "routes", "database/migrations"]

        for app_dir in app_dirs:
            try:
                path = paths(app_dir)
                if path and os.path.isdir(path):
                    watch_paths.append(path)
            except Exception:
                continue

        return watch_paths

    def _get_core_commands_path(self):
        """Get the path to core commands."""
        try:
            pkg = importlib.import_module("cara.commands.core")
            path = getattr(pkg, "__path__", [None])[0]
            if path and os.path.isdir(path):
                return path
        except ImportError:
            pass
        return None

    def reload(self):
        """Reload the command system."""
        self._clear_registry()
        self._reset_runner()
        self._initialize_commands()
        # Note: We don't restart the entire process like Laravel does
        # because that would be too disruptive for development

    def _clear_registry(self):
        self.registry.clear()

    def _reset_runner(self):
        self.runner.console_app = CommandRunner(self.application).console_app

    def run(self):
        self.runner.run()

    def register_custom_command(self, cmd_cls):
        """Allow providers to register custom commands."""
        self.loader.add_command(cmd_cls)
        self.runner.register(cmd_cls)

    def shutdown(self):
        """Shutdown the command system and stop file watching."""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None
