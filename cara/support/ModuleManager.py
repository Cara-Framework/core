"""
Module Manager for Python module paths in the Cara framework.

This module provides utilities for handling Python module imports and paths.
"""

from __future__ import annotations


class ModuleManager:
    """
    Python Module Manager for the Cara framework.

    Handles all Python module path operations for dynamic imports and module loading.
    """

    # Base module configuration
    _app_module_base: str = "app"
    _routes_module_base: str = "routes"
    _config_module_base: str = "config"
    # Models live in the shared ``commons.models`` package so both deployables
    # (api/, services/) import IDENTICAL classes. This is therefore configured
    # independently of the (per-deployable) app base. Config-overridable.
    _models_module_base: str = "commons.models"

    @staticmethod
    def set_app_module_base(module_path: str) -> None:
        """Set the base module path for app components (default: 'app')."""
        ModuleManager._app_module_base = module_path

    @staticmethod
    def set_models_module_base(module_path: str) -> None:
        """Set the base module path for models (default: 'commons.models').

        The shared models package is not derived from the app base — product
        Kernels may point this elsewhere, but the framework default is the
        cross-deployable ``commons.models``.
        """
        ModuleManager._models_module_base = module_path

    @staticmethod
    def set_routes_module_base(module_path: str) -> None:
        """Set the base module path for routes (default: 'routes')."""
        ModuleManager._routes_module_base = module_path

    @staticmethod
    def set_config_module_base(module_path: str) -> None:
        """Set the base module path for config (default: 'config')."""
        ModuleManager._config_module_base = module_path

    # App module paths
    @staticmethod
    def app_module(submodule: str = "") -> str:
        """Return app module path (e.g., 'app' or 'app.controllers')."""
        base = ModuleManager._app_module_base
        return f"{base}.{submodule}" if submodule else base

    @staticmethod
    def controllers_module() -> str:
        """Return controllers module path (e.g., 'app.controllers')."""
        return ModuleManager.app_module("controllers")

    @staticmethod
    def middlewares_module() -> str:
        """Return middlewares module path (e.g., 'app.middlewares')."""
        return ModuleManager.app_module("middlewares")

    @staticmethod
    def models_module() -> str:
        """Return the models module path (default: 'commons.models').

        Deliberately NOT derived from the app base: models are shared across
        the api and services deployables via ``commons.models`` so both import
        identical classes. Config-overridable via ``set_models_module_base``.
        """
        return ModuleManager._models_module_base

    @staticmethod
    def commands_module() -> str:
        """Return commands module path (e.g., 'app.commands')."""
        return ModuleManager.app_module("commands")

    @staticmethod
    def providers_module() -> str:
        """Return providers module path (e.g., 'app.providers')."""
        return ModuleManager.app_module("providers")

    @staticmethod
    def mailables_module() -> str:
        """Return mail/mailables module path (e.g., 'app.mail')."""
        return ModuleManager.app_module("mail")

    @staticmethod
    def jobs_module() -> str:
        """Return jobs module path (e.g., 'app.jobs')."""
        return ModuleManager.app_module("jobs")

    @staticmethod
    def listeners_module() -> str:
        """Return listeners module path (e.g., 'app.listeners')."""
        return ModuleManager.app_module("listeners")

    @staticmethod
    def events_module() -> str:
        """Return events module path (e.g., 'app.events')."""
        return ModuleManager.app_module("events")

    @staticmethod
    def handlers_module() -> str:
        """Return handlers module path (e.g., 'app.handlers')."""
        return ModuleManager.app_module("handlers")

    @staticmethod
    def policies_module() -> str:
        """Return policies module path (e.g., 'app.policies')."""
        return ModuleManager.app_module("policies")

    # Routes module paths
    @staticmethod
    def routes_module(submodule: str = "") -> str:
        """Return routes module path (e.g., 'routes' or 'routes.api')."""
        base = ModuleManager._routes_module_base
        return f"{base}.{submodule}" if submodule else base

    @staticmethod
    def routes_api_module() -> str:
        """Return API routes module path (e.g., 'routes.api')."""
        return ModuleManager.routes_module("api")

    @staticmethod
    def routes_web_module() -> str:
        """Return web routes module path (e.g., 'routes.web')."""
        return ModuleManager.routes_module("web")

    # Config module paths
    @staticmethod
    def config_module(submodule: str = "") -> str:
        """Return config module path (e.g., 'config' or 'config.database')."""
        base = ModuleManager._config_module_base
        return f"{base}.{submodule}" if submodule else base

    @staticmethod
    def config_app_module() -> str:
        """Return app config module path (e.g., 'config.app')."""
        return ModuleManager.config_module("app")

    @staticmethod
    def config_database_module() -> str:
        """Return database config module path (e.g., 'config.database')."""
        return ModuleManager.config_module("database")

    @staticmethod
    def config_cache_module() -> str:
        """Return cache config module path (e.g., 'config.cache')."""
        return ModuleManager.config_module("cache")

    @staticmethod
    def config_mail_module() -> str:
        """Return mail config module path (e.g., 'config.mail')."""
        return ModuleManager.config_module("mail")

    # Utility methods
    @staticmethod
    def import_module(module_path: str):
        """Dynamically import a module by its path."""
        import importlib

        return importlib.import_module(module_path)

    @staticmethod
    def module_exists(module_path: str) -> bool:
        """Check if a module exists and can be imported."""
        try:
            ModuleManager.import_module(module_path)
            return True
        except ImportError:
            return False

    @staticmethod
    def get_module_classes(module_path: str, base_class=None):
        """Get all classes from a module, optionally filtered by base class."""
        import inspect

        try:
            module = ModuleManager.import_module(module_path)
            classes = []

            for _name, cls in inspect.getmembers(module, inspect.isclass):
                # Include classes from this module or its submodules
                if cls.__module__.startswith(module.__name__) and (
                    base_class is None
                    or (issubclass(cls, base_class) and cls != base_class)
                ):
                    classes.append(cls)

            return classes
        except ImportError:
            return []

    @staticmethod
    def get_module_functions(module_path: str):
        """Get all functions from a module."""
        import inspect

        try:
            module = ModuleManager.import_module(module_path)
            functions = []

            for _name, func in inspect.getmembers(module, inspect.isfunction):
                # Only include functions defined in this module
                if func.__module__ == module.__name__:
                    functions.append(func)

            return functions
        except ImportError:
            return []
