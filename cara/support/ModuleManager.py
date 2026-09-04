"""
Module Manager for Python module paths in the Cara framework.

This module provides utilities for handling Python module imports and paths.
"""

from __future__ import annotations

import importlib
import inspect


class ModuleManager:
    """
    Python Module Manager for the Cara framework.

    Handles all Python module path operations for dynamic imports and module loading.
    """

    # Base module configuration
    _app_module_base: str = "app"
    _routes_module_base: str = "routes"
    _config_module_base: str = "config"
    # Product code has one runtime namespace in development and production.
    _models_module_base: str = "app.models"

    @staticmethod
    def set_app_module_base(module_path: str) -> None:
        """Set the base module path for app components (default: 'app')."""
        ModuleManager._app_module_base = module_path

    # App module paths
    @staticmethod
    def app_module(submodule: str = "") -> str:
        """Return app module path (e.g., 'app' or 'app.controllers')."""
        base = ModuleManager._app_module_base
        return f"{base}.{submodule}" if submodule else base

    @staticmethod
    def models_module() -> str:
        """Return the models barrel (default: ``app.models``)."""
        return ModuleManager._models_module_base

    # Routes module paths
    @staticmethod
    def routes_module(submodule: str = "") -> str:
        """Return routes module path (e.g., 'routes' or 'routes.api')."""
        base = ModuleManager._routes_module_base
        return f"{base}.{submodule}" if submodule else base

    # Config module paths
    @staticmethod
    def config_module(submodule: str = "") -> str:
        """Return config module path (e.g., 'config' or 'config.database')."""
        base = ModuleManager._config_module_base
        return f"{base}.{submodule}" if submodule else base

    # Utility methods
    @staticmethod
    def import_module(module_path: str):
        """Dynamically import a module by its path."""

        return importlib.import_module(module_path)

    @staticmethod
    def get_module_classes(module_path: str, base_class=None):
        """Get all classes from a module, optionally filtered by base class."""

        module = ModuleManager.import_module(module_path)
        classes = []

        for _name, cls in inspect.getmembers(module, inspect.isclass):
            # Include classes from this module or its submodules
            if cls.__module__.startswith(module.__name__) and (
                base_class is None or (issubclass(cls, base_class) and cls != base_class)
            ):
                classes.append(cls)

        return classes
