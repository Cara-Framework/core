"""
Dynamic Path Helper for the Cara framework.

Laravel-style dynamic interface for filesystem paths:
    paths('storage')                  # /project/storage
    paths('storage.logs')            # /project/storage/logs
    paths('app.controllers')         # /project/app/controllers
    paths('anything.here')           # /project/anything/here
"""

from .PathManager import PathManager


def paths(path: str = "", relative: str = "") -> str:
    """
    Dynamic filesystem path resolver with Laravel-style interface.

    Examples:
        paths('storage')                    # /project/storage
        paths('config')                     # /project/config
        paths('storage.logs')              # /project/storage/logs
        paths('app.controllers')           # /project/app/controllers
        paths('anything.new')              # /project/anything/new (auto-created)
        paths('custom', 'subfolder')       # /project/custom/subfolder
    """
    if not path:
        return PathManager.base_path(relative)

    return _resolve_dynamic_path(path, relative)


def _resolve_dynamic_path(path: str, relative: str = "") -> str:
    """Smart dynamic path resolver."""

    # Convert dot notation to filesystem path
    if "." in path:
        path_parts = path.split(".")
        base_component = path_parts[0]
        sub_path = "/".join(path_parts[1:])

        # Combine sub_path with relative if both exist
        final_relative = "/".join(filter(None, [sub_path, relative]))

        return _get_smart_filesystem_path(base_component, final_relative)

    # Single component
    return _get_smart_filesystem_path(path, relative)


def _get_smart_filesystem_path(component: str, relative: str = "") -> str:
    """Intelligently determine filesystem path based on component name."""

    # Special case for 'base' - return project root
    if component == "base":
        return PathManager.base_path(relative)

    # Known root-level directories
    root_paths = {
        "config": PathManager.config_path,
        "storage": PathManager.storage_path,
        "public": PathManager.public_path,
        "database": PathManager.database_path,
        "routes": PathManager.routes_path,
        "resources": PathManager.resources_path,
    }

    # Known app-level directories
    app_paths = {
        "app": PathManager.app_path,
        "controllers": PathManager.controllers_path,
        "middlewares": PathManager.middlewares_path,
        "models": PathManager.models_path,
        "commands": PathManager.commands_path,
        "providers": PathManager.providers_path,
        "mailables": PathManager.mailables_path,
        "jobs": PathManager.jobs_path,
        "events": PathManager.events_path,
        "listeners": PathManager.listeners_path,
        "notifications": PathManager.notifications_path,
        "policies": PathManager.policies_path,
    }

    # Database-related paths
    database_paths = {
        "migrations": PathManager.migrations_path,
        "seeds": PathManager.seeds_path,
        "db": PathManager.db_path,
    }

    # Storage shortcuts
    storage_shortcuts = {
        "logs": lambda r: PathManager.storage_path(f"logs/{r}" if r else "logs"),
        "cache": lambda r: PathManager.storage_path(f"cache/{r}" if r else "cache"),
        "uploads": lambda r: PathManager.storage_path(f"uploads/{r}" if r else "uploads"),
        "temp": lambda r: PathManager.storage_path(f"temp/{r}" if r else "temp"),
        "framework": lambda r: PathManager.storage_path(
            f"framework/{r}" if r else "framework"
        ),
    }

    # Check each category
    if component in root_paths:
        return root_paths[component](relative)
    elif component in app_paths:
        return app_paths[component](relative)
    elif component in database_paths:
        return database_paths[component](relative)
    elif component in storage_shortcuts:
        return storage_shortcuts[component](relative)

    # Special cases
    if component == "views":
        return PathManager.views_path(relative)

    # Default: treat as custom directory under base path
    return PathManager.base_path(f"{component}/{relative}" if relative else component)


# Convenience functions
def storage_path(relative: str = "") -> str:
    """Get storage path: /project/storage[/relative]"""
    return paths("storage", relative)


def config_path(relative: str = "") -> str:
    """Get config path: /project/config[/relative]"""
    return paths("config", relative)


def public_path(relative: str = "") -> str:
    """Get public path: /project/public[/relative]"""
    return paths("public", relative)


def app_path(relative: str = "") -> str:
    """Get app path: /project/app[/relative]"""
    return paths("app", relative)


def base_path(relative: str = "") -> str:
    """Get base project path: /project[/relative]"""
    return PathManager.base_path(relative)


# Ultra-short alias
def p(path: str = "", relative: str = "") -> str:
    """Ultra-short alias for paths()."""
    return paths(path, relative)


# Dynamic attribute access for even cleaner syntax
class PathHelper:
    """Dynamic path access with dot notation."""

    def __init__(self, base_path: str = ""):
        self.base = base_path

    def __getattr__(self, name: str) -> str:
        path = f"{self.base}.{name}" if self.base else name
        return paths(path)

    def __call__(self, relative: str = "") -> str:
        return paths(self.base, relative) if self.base else paths("", relative)


# Pre-configured path helpers
storage = PathHelper("storage")
config = PathHelper("config")
app = PathHelper("app")
