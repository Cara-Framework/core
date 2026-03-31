"""
Command Loader for the Cara framework.

This module provides utilities for dynamically loading CLI command classes from the application.
"""

import importlib
import pkgutil


class CommandLoader:
    """Loads command modules from a given package and allows manual registration of custom
    commands."""

    def __init__(self, package: str):
        self.package = package
        self.custom_commands = []

    def add_command(self, cmd_cls):
        """Register a custom command class manually."""
        self.custom_commands.append(cmd_cls)

    def get_custom_commands(self):
        return self.custom_commands

    def load(self):
        try:
            module = importlib.import_module(self.package)
        except ImportError:
            return
        if not hasattr(module, "__path__"):
            return
        for _, name, _ in pkgutil.iter_modules(module.__path__):
            full = f"{self.package}.{name}"
            try:
                importlib.import_module(full)
            except ImportError:
                pass
