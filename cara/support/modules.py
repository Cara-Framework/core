"""
Dynamic Module Helper for the Cara framework.

Laravel config() style dynamic interface:
    modules('controllers')                    # app.controllers
    modules('routes.api')                    # routes.api
    modules('app.anything')                  # app.anything
    modules('mymodule.submodule')            # mymodule.submodule
    modules('controllers', base='myapp')     # myapp.controllers
"""

from __future__ import annotations

from .ModuleManager import ModuleManager


def modules(path: str = "", base: str | None = None) -> str:
    """
    Dynamic module path resolver with Laravel-style interface.

    Examples:
        modules('controllers')              # app.controllers
        modules('models')                   # app.models
        modules('routes.api')              # routes.api
        modules('config.database')         # config.database
        modules('anything.here')           # anything.here (passthrough)
        modules('new_component')           # app.new_component (auto app prefix)

        # Custom base
        modules('controllers', base='myapp')  # myapp.controllers
    """
    if not path:
        return ModuleManager.app_module()

    # Handle custom base temporarily
    if base:
        original_base = ModuleManager._app_module_base
        ModuleManager.set_app_module_base(base)
        try:
            result = _resolve_dynamic_module_path(path)
        finally:
            ModuleManager.set_app_module_base(original_base)
        return result

    return _resolve_dynamic_module_path(path)


def _resolve_dynamic_module_path(path: str) -> str:
    """Smart dynamic module path resolver."""

    # If already contains dots, assume it's a full module path
    if "." in path:
        return path

    # Single word components get smart prefixes
    return _get_smart_module_path(path)


def _get_smart_module_path(component: str) -> str:
    """Intelligently determine module prefix based on component name."""

    # App-level components (most common)
    app_patterns = [
        "controllers",
        "middlewares",
        "models",
        "commands",
        "providers",
        "mailables",
        "jobs",
        "listeners",
        "events",
        "handlers",
        "policies",
        "services",
        "repositories",
        "facades",
        "traits",
        "helpers",
        "resources",
        "transformers",
        "observers",
        "scouts",
        "rules",
    ]

    # Routes components
    if component.startswith("routes") or component in ["api", "web", "channels"]:
        if component == "api":
            return ModuleManager.routes_module("api")
        elif component == "web":
            return ModuleManager.routes_module("web")
        elif component == "channels":
            return ModuleManager.routes_module("channels")
        else:
            return ModuleManager.routes_module(component.replace("routes_", ""))

    # Config components
    if component.startswith("config") or component in [
        "app",
        "database",
        "cache",
        "mail",
        "queue",
        "session",
        "filesystems",
        "logging",
        "broadcasting",
        "auth",
        "services",
        "cors",
        "view",
    ]:
        if component.startswith("config_"):
            return ModuleManager.config_module(component.replace("config_", ""))
        elif component == "config":
            return ModuleManager.config_module()
        return ModuleManager.config_module(component)

    # App components (default for most things)
    if component in app_patterns:
        return ModuleManager.app_module(component)

    # Default: treat as app submodule for anything new
    return ModuleManager.app_module(component)


def get_classes(path: str, base_class=None):
    """Get classes from a module."""
    return ModuleManager.get_module_classes(modules(path), base_class)
