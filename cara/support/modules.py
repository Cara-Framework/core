"""
Dynamic Module Helper for the Cara framework.

Laravel config() style dynamic interface:
    modules('controllers')                    # app.controllers
    modules('routes.api')                    # routes.api
    modules('app.anything')                  # app.anything
    modules('mymodule.submodule')            # mymodule.submodule
    modules('controllers', base='myapp')     # myapp.controllers
"""

from .ModuleManager import ModuleManager


def modules(path: str = "", base: str = None) -> str:
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
        elif component in [
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
            return ModuleManager.config_module(component)
        else:
            return ModuleManager.config_module(component)

    # App components (default for most things)
    if component in app_patterns:
        return ModuleManager.app_module(component)

    # Default: treat as app submodule for anything new
    return ModuleManager.app_module(component)


# Convenience functions for common use cases
def module_exists(path: str) -> bool:
    """Check if a module exists."""
    return ModuleManager.module_exists(modules(path))


def import_module(path: str):
    """Import a module dynamically."""
    return ModuleManager.import_module(modules(path))


def get_classes(path: str, base_class=None):
    """Get classes from a module."""
    return ModuleManager.get_module_classes(modules(path), base_class)


def get_functions(path: str):
    """Get functions from a module."""
    return ModuleManager.get_module_functions(modules(path))


# Ultra-short aliases for power users
def m(path: str = "", base: str = None) -> str:
    """Ultra-short alias for modules()."""
    return modules(path, base)


# Dynamic attribute access for even cleaner syntax
class ModuleHelper:
    """Dynamic module access with dot notation."""

    def __init__(self, base_path: str = ""):
        self.base = base_path

    def __getattr__(self, name: str) -> str:
        path = f"{self.base}.{name}" if self.base else name
        return modules(path)

    def __call__(self, path: str = "") -> str:
        if path:
            full_path = f"{self.base}.{path}" if self.base else path
            return modules(full_path)
        return modules(self.base) if self.base else modules()


# Pre-configured module helpers
app = ModuleHelper("app")
routes = ModuleHelper("routes")
config = ModuleHelper("config")
